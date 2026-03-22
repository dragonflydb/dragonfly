// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "server/journal/types.h"
#include "server/rdb_save.h"
#include "server/serializer_base.h"
#include "server/synchronization.h"
#include "server/table.h"
#include "server/tiered_storage.h"

namespace dfly {

class ExecutionState;

namespace journal {
struct Entry;
}  // namespace journal

namespace search {
using DocId = uint32_t;
}  // namespace search

// ┌────────────────┐   ┌─────────────┐
// │IterateBucketsFb│   │  OnChange   │
// └──────┬─────────┘   └─┬───────────┘
//        │               │            OnChange forces whole bucket to be
//        ▼               ▼            serialized if iterate didn't reach it yet
// ┌──────────────────────────┐
// │     DoSerializeBucket    │        Both might fall back to a temporary serializer
// └────────────┬─────────────┘        if default is used on another db index
//              │
//              |                      Socket is left open in journal streaming mode
//              ▼
// ┌──────────────────────────┐          ┌─────────────────────────┐
// │     SerializeEntry       │          │  ConsumeJournalChange   │
// └─────────────┬────────────┘          └────────────┬────────────┘
//               │                                    │
//         PushBytes                                  │   into serializer buffer)
//               │                                    ▼
//               ▼                        ┌──────────────────────────┐
//               ▼                        │     WriteJournalEntry    │
// ┌──────────────────────────────┐       │  (appends journal entry  │
// │     push_cb(buffer)          │       │   into serializer buffer)│
// └──────────────────────────────┘       └──────────────────────────┘

// SliceSnapshot is used for iterating over a shard at a specified point-in-time
// and submitting all values to an output sink.
// In journal streaming mode, the snapshot continues submitting changes
// over the sink until explicitly stopped.
class SliceSnapshot : public SerializerBase, public journal::JournalConsumerInterface {
 public:
  // Represents a target sink for receiving snapshot data. Specifically designed
  // to send data to RdbSaver wrapping up a file shard or a socket.
  struct SnapshotDataConsumerInterface {
    virtual ~SnapshotDataConsumerInterface() = default;

    // Receives a chunk of snapshot data for processing
    virtual void ConsumeData(std::string data, ExecutionState* cntx) = 0;

    // Finalizes the snapshot writing
    virtual void Finalize() = 0;
  };

  SliceSnapshot(CompressionMode compression_mode, DbSlice* slice,
                SnapshotDataConsumerInterface* consumer, ExecutionState* cntx,
                DflyVersion replica_dfly_version);
  ~SliceSnapshot();

  static size_t GetThreadLocalMemoryUsage();
  static bool IsSnaphotInProgress();

  // Initialize snapshot, start bucket iteration fiber, register listeners.
  // In journal streaming mode it needs to be stopped by either Stop or Cancel.
  enum class SnapshotFlush : uint8_t { kAllow, kDisallow };

  void Start(bool stream_journal, SnapshotFlush allow_flush = SnapshotFlush::kDisallow);

  // Finalizes journal streaming writes. Only called for replication.
  // Blocking. Must be called from the Snapshot thread.
  void FinalizeJournalStream(bool cancel);

  // Waits for a regular, non journal snapshot to finish.
  // Called only for non-replication, backups usecases.
  void WaitSnapshotting() {
    snapshot_fb_.JoinIfNeeded();
  }

  const RdbTypeFreqMap& freq_map() const {
    return type_freq_map_;
  }

  // Get different sizes, in bytes. All disjoint.
  size_t GetBufferCapacity() const;
  size_t GetTempBuffersSize() const;

  RdbSaver::SnapshotStats GetCurrentSnapshotProgress() const;

  // Journal listener
  void ConsumeJournalChange(const journal::JournalChangeItem& item) override;
  void ThrottleIfNeeded() override;

 private:
  [[maybe_unused]] void SerializeIndexMapping(
      uint32_t shard_id, std::string_view index_name,
      const std::vector<std::pair<std::string, search::DocId>>& mappings);

  // Serialize ShardDocIndex key-to-DocId mappings for all search indices on this shard
  void SerializeIndexMappings();

  // Serialize HNSW global indices for shard 0 only
  void SerializeGlobalHnswIndices();

  // Main snapshotting fiber that iterates over all buckets in the db slice.
  void IterateBucketsFb(bool send_full_sync_cut);

  // Serialize single bucket.
  // Returns number of serialized entries.
  unsigned SerializeBucket(DbIndex db_index, PrimeTable::bucket_iterator bucket_it,
                           bool on_update) override;

  // Serialize entry into passed serializer.
  void SerializeEntry(DbIndex db_index, const PrimeKey& pk, const PrimeValue& pv);

  // Push serializer's internal buffer.
  // Push regardless of buffer size if force is true.
  // Return true if pushed. Can block. Is called from the snapshot thread.
  bool PushSerialized(bool force);
  void SerializeExternal(DbIndex db_index, PrimeKey pk, const PrimeValue& pv, time_t expire_time,
                         uint32_t mc_flags);

  // Handles data provided by RdbSerializer when its internal buffer exceeds the threshold
  // during big value serialization (e.g. huge sets/lists or large strings).
  // The data has already been extracted from the serializer and is owned here, ensuring correct
  // plumbing and making it safe to move.
  void HandleFlushData(std::string data);

  // Used for explicit flushes at safe points (e.g. between entries). Can block.
  size_t FlushSerialized();

  // Tuple <db_index, key> is used as a key to uniquely identify tiered entry on shard.
  using TieredDelayEntryKey = std::pair<DbIndex, std::string>;

  // Serialize delayed entries.
  // If bucket_tiered_keys is provided we should serialize these keys forcefully.
  // Other entries can be serialized if they are resolved, but we don't wait for them unless force
  // is true.
  void PushDelayedEntries(bool force, std::vector<TieredDelayEntryKey>* bucket_tiered_keys);

  PrimeTable::Cursor snapshot_cursor_;

  std::unique_ptr<RdbSerializer> serializer_;
  // Delayed entries that are waiting for tiered storage reads to complete before they can be
  // serialized.
  absl::flat_hash_map<TieredDelayEntryKey, std::unique_ptr<TieredDelayedEntry>> delayed_entries_;

  // Used for sanity checks.
  bool serialize_bucket_running_ = false;
  uint32_t journal_cb_id_ = 0;

  util::fb2::Fiber snapshot_fb_;
  util::fb2::CondVarAny seq_cond_;

  const CompressionMode compression_mode_;
  RdbTypeFreqMap type_freq_map_;

  bool use_background_mode_ = false;
  DflyVersion replica_dfly_version_ = DflyVersion::CURRENT_VER;

  uint64_t rec_id_ = 1, last_pushed_id_ = 0;

  struct Stats {
    size_t skipped = 0;
    size_t keys_total = 0;
    size_t jounal_changes = 0;
    size_t flushed_under_lock = 0;
  } stats_;

  SnapshotDataConsumerInterface* consumer_;
  ExecutionState* cntx_;
};

}  // namespace dfly
