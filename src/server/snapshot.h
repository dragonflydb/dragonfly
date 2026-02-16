// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <deque>

#include "base/pod_array.h"
#include "core/search/base.h"
#include "io/file.h"
#include "server/db_slice.h"
#include "server/execution_state.h"
#include "server/rdb_save.h"
#include "server/synchronization.h"
#include "server/table.h"
#include "util/fibers/future.h"

namespace dfly {

namespace journal {
struct Entry;
}  // namespace journal

// ┌────────────────┐   ┌─────────────┐
// │IterateBucketsFb│   │  OnDbChange │
// └──────┬─────────┘   └─┬───────────┘
//        │               │            OnDbChange forces whole bucket to be
//        ▼               ▼            serialized if iterate didn't reach it yet
// ┌──────────────────────────┐
// │     SerializeBucket      │        Both might fall back to a temporary serializer
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
class SliceSnapshot : public journal::JournalConsumerInterface {
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
                SnapshotDataConsumerInterface* consumer, ExecutionState* cntx);
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
  void ConsumeJournalChange(const journal::JournalChangeItem& item);
  void ThrottleIfNeeded();

 private:
  [[maybe_unused]] void SerializeIndexMapping(
      uint32_t shard_id, std::string_view index_name,
      const std::vector<std::pair<std::string, search::DocId>>& mappings);

  // Serialize ShardDocIndex key-to-DocId mappings for all search indices on this shard
  void SerializeIndexMappings();

  // Serialize HNSW global indices for shard 0 only
  void SerializeGlobalHnswIndices();

  // Main snapshotting fiber that iterates over all buckets in the db slice
  // and submits them to SerializeBucket.
  void IterateBucketsFb(bool send_full_sync_cut);

  // Called on traversing cursor by IterateBucketsFb.
  bool BucketSaveCb(DbIndex db_index, PrimeTable::bucket_iterator it);

  // Serialize single bucket.
  // Returns number of serialized entries, updates bucket version to snapshot version.
  unsigned SerializeBucket(DbIndex db_index, PrimeTable::bucket_iterator bucket_it);

  // Serialize entry into passed serializer.
  void SerializeEntry(DbIndex db_index, const PrimeKey& pk, const PrimeValue& pv);

  // DbChange listener
  void OnDbChange(DbIndex db_index, const DbSlice::ChangeReq& req);

  // DbSlice moved listener
  void OnMoved(DbIndex db_index, const DbSlice::MovedItemsVec& items);
  bool IsPositionSerialized(DbIndex db_index, PrimeTable::Cursor cursor);

  // Push serializer's internal buffer.
  // Push regardless of buffer size if force is true.
  // Return true if pushed. Can block. Is called from the snapshot thread.
  bool PushSerialized(bool force);
  void SerializeExternal(DbIndex db_index, PrimeKey key, const PrimeValue& pv, time_t expire_time,
                         uint32_t mc_flags);

  // Handles data provided by RdbSerializer when its internal buffer exceeds the threshold
  // during big value serialization (e.g. huge sets/lists or large strings).
  // The data has already been extracted from the serializer and is owned here, ensuring correct
  // plumbing and making it safe to move.
  void HandleFlushData(std::string data);

  // Flush data from built in (or custom) serializer and pass it to HandleFlushData.
  // Used for explicit flushes at safe points (e.g. between entries). Can block.
  size_t FlushSerialized(RdbSerializer* serializer = nullptr /* use serializer_ */);

  // An entry whose value must be awaited
  struct DelayedEntry {
    DbIndex dbid;
    PrimeKey key;
    util::fb2::Future<io::Result<string>> value;
    time_t expire;
    uint32_t mc_flags;
  };

  DbSlice* db_slice_;
  const DbTableArray db_array_;
  PrimeTable::Cursor snapshot_cursor_;
  DbIndex snapshot_db_index_ = 0;

  std::unique_ptr<RdbSerializer> serializer_;
  std::deque<DelayedEntry> delayed_entries_;  // collected during atomic bucket traversal

  // Used for sanity checks.
  bool serialize_bucket_running_ = false;

  util::fb2::Fiber snapshot_fb_;  // IterateEntriesFb
  util::fb2::CondVarAny seq_cond_;

  const CompressionMode compression_mode_;
  RdbTypeFreqMap type_freq_map_;

  // version upper bound for entries that should be saved (not included).
  uint64_t snapshot_version_;
  uint64_t moved_cb_id_ = 0;
  uint32_t journal_cb_id_ = 0;
  uint32_t moved_cb_id = 0;

  bool use_background_mode_ = false;
  bool use_snapshot_version_ = true;

  uint64_t rec_id_ = 1, last_pushed_id_ = 0;

  struct Stats {
    size_t loop_serialized = 0;
    size_t skipped = 0;
    size_t side_saved = 0;
    size_t savecb_calls = 0;
    size_t keys_total = 0;
    size_t jounal_changes = 0;
    size_t moved_saved = 0;
  } stats_;

  ThreadLocalMutex big_value_mu_;

  SnapshotDataConsumerInterface* consumer_;
  ExecutionState* cntx_;
};

}  // namespace dfly
