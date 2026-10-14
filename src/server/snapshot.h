// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <deque>

#include "server/detail/egress_throttle.h"
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

// SliceSnapshot is used for iterating over a shard at a specified point-in-time
// and submitting all values to an output sink via RdbSerializer.
// In journal streaming mode, the snapshot continues submitting changes
// over the sink until explicitly stopped.
//
// See serializer_base.h for the overall serialization pipeline diagram.
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
  void ConsumeJournalChange(const journal::JournalChangeItem& item) final;
  void ThrottleIfNeeded() final;

 private:
  // Main snapshotting fiber that iterates over all buckets in the db slice.
  void IterateBucketsFb(bool send_full_sync_cut);

  // Flushes the serializer and yields between traversal steps.
  void PaceTraversal(bool done) override;

  // Called under stream_mu_ to perform RDB serialization of a single entry.
  void SerializeEntryLocked(DbIndex db_index, const PrimeKey& pk, const PrimeValue& pv,
                            time_t expire, uint32_t mc_flags) override;

  // Push serializer's internal buffer.
  // Push regardless of buffer size if force is true.
  // Return true if pushed. Can block. Is called from the snapshot thread.
  bool PushSerialized(bool force);

  // Handles data provided by RdbSerializer when its internal buffer exceeds the threshold
  // during big value serialization (e.g. huge sets/lists or large strings).
  // The data has already been extracted from the serializer and is owned here, ensuring correct
  // plumbing and making it safe to move. Queues the chunk for flush_fb_. At a safe point the
  // traversal fiber then waits for the chunk to be written; any other call returns while the
  // queue holds at most pending_limit_ bytes, so a bucket dependency held mid-value is never
  // kept across a sink write.
  void HandleFlushData(std::string data, bool safe_point);

  // Body of flush_fb_: writes pending_ to the consumer in FIFO order until StopFlushFb.
  void FlushPendingFb();

  // Drains pending_ and joins flush_fb_. Every producer must be unregistered by then: no
  // HandleFlushData call may follow. Idempotent.
  void StopFlushFb();

  // Callback of RdbSerializer to push big value chunks
  std::error_code ConsumeBigValueChunk(std::string data);

  // Used for explicit flushes at safe points (e.g. between entries). Can block.
  size_t FlushSerialized();

  std::unique_ptr<RdbSerializer> serializer_;

  uint32_t journal_cb_id_ = 0;

  util::fb2::Fiber snapshot_fb_;

  // Chunks produced by HandleFlushData, written to the consumer by flush_fb_ in FIFO order.
  struct PendingChunk {
    std::string data;
    bool high_prio;  // not produced by the traversal fiber, for the egress throttler
  };
  std::deque<PendingChunk> pending_;
  // Sum of the capacities of the strings in pending_.
  size_t pending_bytes_ = 0;
  uint64_t produced_seq_ = 0, written_seq_ = 0;  // chunk sequence numbers
  // Bytes of pending_ a mutation fiber may leave queued without waiting.
  size_t pending_limit_ = 0;
  bool stop_flush_ = false;
  util::fb2::Fiber flush_fb_;
  util::fb2::CondVarAny flush_cv_;  // wakes flush_fb_
  util::fb2::CondVarAny drain_cv_;  // wakes producers waiting on pending_bytes_

  const CompressionMode compression_mode_;
  RdbTypeFreqMap type_freq_map_;

  bool use_background_mode_ = false;
  DflyVersion replica_dfly_version_ = DflyVersion::CURRENT_VER;

  // Limits this snapshot's socket egress to a configured bandwidth budget.
  detail::EgressThrottler throttler_{0};

  struct Stats {
    size_t keys_total = 0;
    size_t jounal_changes = 0;
    size_t flushed_under_lock = 0;
  } stats_;

  SnapshotDataConsumerInterface* consumer_;
};

}  // namespace dfly
