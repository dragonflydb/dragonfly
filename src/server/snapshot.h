// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <atomic>
#include <bitset>

#include "base/pod_array.h"
#include "io/file.h"
#include "server/common.h"
#include "server/db_slice.h"
#include "server/rdb_save.h"
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
// │     SerializeEntry       │ ◄────────┤     OnJournalEntry      │
// └─────────────┬────────────┘          └─────────────────────────┘
//               │
//         PushBytes                  Default buffer gets flushed on iteration,
//               │                    temporary on destruction
//               ▼
// ┌──────────────────────────────┐
// │     push_cb(buffer)       │
// └──────────────────────────────┘

// SliceSnapshot is used for iterating over a shard at a specified point-in-time
// and submitting all values to an output sink.
// In journal streaming mode, the snapshot continues submitting changes
// over the sink until explicitly stopped.
class SliceSnapshot {
 public:
  // Represents a target for receiving snapshot data.
  struct SnapshotDataConsumerInterface {
    virtual ~SnapshotDataConsumerInterface() = default;

    // Receives a chunk of snapshot data for processing
    virtual void ConsumeData(std::string data, Context* cntx) = 0;
    // Finalizes the snapshot writing
    virtual void Finalize() = 0;
  };

  SliceSnapshot(CompressionMode compression_mode, DbSlice* slice,
                SnapshotDataConsumerInterface* consumer, Context* cntx);
  ~SliceSnapshot();

  static size_t GetThreadLocalMemoryUsage();
  static bool IsSnaphotInProgress();

  // Initialize snapshot, start bucket iteration fiber, register listeners.
  // In journal streaming mode it needs to be stopped by either Stop or Cancel.
  enum class SnapshotFlush { kAllow, kDisallow };

  void Start(bool stream_journal, SnapshotFlush allow_flush = SnapshotFlush::kDisallow);

  // Initialize a snapshot that sends only the missing journal updates
  // since start_lsn and then registers a callback switches into the
  // journal streaming mode until stopped.
  // If we're slower than the buffer and can't continue, `Cancel()` is
  // called.
  void StartIncremental(LSN start_lsn);

  // Finalizes journal streaming writes. Only called for replication.
  // Blocking. Must be called from the Snapshot thread.
  void FinalizeJournalStream(bool cancel);

  // Waits for a regular, non journal snapshot to finish.
  // Called only for non-replication, backups usecases.
  void WaitSnapshotting() {
    snapshot_fb_.JoinIfNeeded();
  }

 private:
  // Main snapshotting fiber that iterates over all buckets in the db slice
  // and submits them to SerializeBucket.
  void IterateBucketsFb(bool send_full_sync_cut);

  // A fiber function that switches to the incremental mode
  void SwitchIncrementalFb(LSN lsn);

  // Called on traversing cursor by IterateBucketsFb.
  bool BucketSaveCb(DbIndex db_index, PrimeTable::bucket_iterator it);

  // Serialize single bucket.
  // Returns number of serialized entries, updates bucket version to snapshot version.
  unsigned SerializeBucket(DbIndex db_index, PrimeTable::bucket_iterator bucket_it);

  // Serialize entry into passed serializer.
  void SerializeEntry(DbIndex db_index, const PrimeKey& pk, const PrimeValue& pv);

  // DbChange listener
  void OnDbChange(DbIndex db_index, const DbSlice::ChangeReq& req);

  // Journal listener
  void OnJournalEntry(const journal::JournalItem& item, bool unused_await_arg);

  // Push serializer's internal buffer.
  // Push regardless of buffer size if force is true.
  // Return true if pushed. Can block. Is called from the snapshot thread.
  bool PushSerialized(bool force);

  // Helper function that flushes the serialized items into the RecordStream.
  // Can block.
  using FlushState = SerializerBase::FlushState;
  size_t FlushSerialized(FlushState flush_state);

 public:
  uint64_t snapshot_version() const {
    return snapshot_version_;
  }

  const RdbTypeFreqMap& freq_map() const {
    return type_freq_map_;
  }

  // Get different sizes, in bytes. All disjoint.
  size_t GetBufferCapacity() const;
  size_t GetTempBuffersSize() const;

  RdbSaver::SnapshotStats GetCurrentSnapshotProgress() const;

 private:
  // An entry whose value must be awaited
  struct DelayedEntry {
    DbIndex dbid;
    CompactObj key;
    util::fb2::Future<PrimeValue> value;
    time_t expire;
    uint32_t mc_flags;
  };

  DbSlice* db_slice_;
  const DbTableArray db_array_;

  std::unique_ptr<RdbSerializer> serializer_;
  std::vector<DelayedEntry> delayed_entries_;  // collected during atomic bucket traversal

  // Used for sanity checks.
  bool serialize_bucket_running_ = false;
  util::fb2::Fiber snapshot_fb_;  // IterateEntriesFb
  util::fb2::CondVarAny seq_cond_;
  const CompressionMode compression_mode_;
  RdbTypeFreqMap type_freq_map_;

  // version upper bound for entries that should be saved (not included).
  uint64_t snapshot_version_ = 0;
  uint32_t journal_cb_id_ = 0;

  uint64_t rec_id_ = 1, last_pushed_id_ = 0;

  struct Stats {
    size_t loop_serialized = 0;
    size_t skipped = 0;
    size_t side_saved = 0;
    size_t savecb_calls = 0;
    size_t keys_total = 0;
  } stats_;

  ThreadLocalMutex big_value_mu_;

  SnapshotDataConsumerInterface* consumer_;
  Context* cntx_;
};

}  // namespace dfly
