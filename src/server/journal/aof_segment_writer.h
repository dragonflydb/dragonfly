// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <deque>
#include <memory>
#include <optional>
#include <string>
#include <system_error>

#include "server/journal/aof.h"
#include "util/fibers/synchronization.h"
#include "util/fibers/uring_file.h"

namespace dfly {

class AofSegmentWriter {
 public:
  AofSegmentWriter(std::string dir, uint32_t shard_id, uint32_t shard_count);
  ~AofSegmentWriter();

  // Creates segment `seq` with a durable header: tmp -> header -> fdatasync -> link -> fsync dir.
  // Synchronous
  std::error_code Open(uint64_t seq);

  // non blocking
  void AddRecord(std::string_view record, uint64_t lsn);
  void Seal();

  // Retries failed writes once, waits for in-flight I/O and fdatasyncs the segment.
  std::error_code Shutdown();

  // Blocks the calling fiber until UnwrittenBytes() <= limit.
  void WaitUnwritten(size_t limit);

  // Open block payload + sealed blocks not yet written.
  size_t UnwrittenBytes() const;

  // Last record fully written (not necessarily durable), 0 if none.
  uint64_t WrittenLsn() const {
    return written_lsn_;
  }

  // Last record covered by a completed fdatasync, 0 if none.
  uint64_t DurableLsn() const {
    return durable_lsn_;
  }

  // A write is being retried, or an fdatasync failed (cleared only by a checkpoint).
  bool ReducedDurability() const {
    return failed_blocks_ > 0 || sync_failed_;
  }

  static std::string SegmentName(uint32_t shard_id, uint64_t seq);

 private:
  struct PendingBlock {
    std::string bytes;
    size_t offset;
    // Last record ending in this block, 0 if none.
    uint64_t last_lsn;
    size_t written = 0;
    bool done = false;
    // Write failed; resubmitted on the next tick.
    bool failed = false;
  };

  void OnSealed(AofSealedBlock block);
  void Submit(PendingBlock* pb);
  void OnWriteDone(PendingBlock* pb, int res);

  // Runs every kAofSyncMs: retries failed writes and syncs completed ones.
  void OnTick();
  void RetryFailed();
  void OnSyncDone(uint64_t sync_target, int res);

  std::string dir_;
  uint32_t shard_id_;
  uint32_t shard_count_;
  std::unique_ptr<util::fb2::LinuxFile> file_;
  std::optional<AofBlockBuilder> builder_;
  size_t next_offset_ = kAofSegmentHeaderSize;

  // Offset order; deque keeps references stable for completions.
  std::deque<PendingBlock> pending_;
  size_t pending_bytes_ = 0;
  uint64_t in_flight_ = 0;
  uint64_t failed_blocks_ = 0;
  uint64_t written_lsn_ = 0;
  uint64_t durable_lsn_ = 0;
  bool sync_in_flight_ = false;
  bool sync_failed_ = false;
  // Set by Shutdown: failed writes are final instead of retried.
  bool stopping_ = false;
  uint32_t tick_id_ = 0;
  std::error_code write_ec_;
  util::fb2::EventCount ev_;
};

}  // namespace dfly
