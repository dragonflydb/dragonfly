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
  enum class Durability : uint8_t {
    kNormal,
    // A write failed; ends once it is written and a later fdatasync covers it.
    kWriteFailed,
    // An fdatasync failed; ends only with a checkpoint. Writes go on as best effort, syncs stop.
    kSyncFailed,
  };

  AofSegmentWriter(std::string dir, uint32_t shard_id, uint32_t shard_count);
  ~AofSegmentWriter();

  // Creates segment `seq` with a durable header: tmp -> header -> fdatasync -> link -> fsync dir.
  // Synchronous
  std::error_code Open(uint64_t seq);

  // non blocking
  void AddRecord(std::string_view record, uint64_t lsn);
  void Seal();

  // Retries failed writes once, waits for in-flight I/O and fdatasyncs the segment.
  // Fails if any write or fdatasync failed, even an earlier periodic one.
  std::error_code Shutdown();

  // Blocks the calling fiber until UnwrittenBytes() <= limit.
  void WaitUnwritten(size_t limit);

  // Open block payload + sealed blocks not yet written.
  size_t UnwrittenBytes() const;

  // Last record fully written (not necessarily durable), 0 if none.
  uint64_t WrittenLsn() const {
    return written_lsn_;
  }

  // Last record covered by a successful fdatasync before any failed one, 0 if none.
  uint64_t DurableLsn() const {
    return durable_lsn_;
  }

  Durability GetDurability() const;

  static std::string SegmentName(uint32_t shard_id, uint64_t seq);

 private:
  enum class BlockState : uint8_t { kInFlight, kFailed, kDone };

  struct PendingBlock {
    std::string bytes;
    size_t offset;
    // Last record ending in this block, 0 if none.
    uint64_t last_lsn;
    size_t written = 0;
    BlockState state = BlockState::kInFlight;
  };

  void OnSealed(AofSealedBlock block);
  void Submit(PendingBlock* pb);
  void OnWriteDone(PendingBlock* pb, int res);

  // Runs every kAofSyncMs: retries failed writes and syncs completed ones.
  void OnTick();
  void RetryFailed();
  void OnSyncDone(size_t sync_offset, uint64_t sync_lsn, int res);

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

  // End of the contiguous written prefix of the file, and of the part covered by a successful
  // fdatasync. The header is durable after Open.
  // When written_offset_ == next_offset_ all blocks are written
  size_t written_offset_ = kAofSegmentHeaderSize;
  size_t durable_offset_ = kAofSegmentHeaderSize;
  // End of the furthest block whose write ever failed.
  size_t repair_offset_ = 0;
  uint64_t written_lsn_ = 0;
  uint64_t durable_lsn_ = 0;

  bool sync_in_flight_ = false;
  uint32_t tick_id_ = 0;
  // Last write error.
  std::error_code write_ec_;
  // First failed fdatasync; sticky until a checkpoint.
  std::error_code sync_ec_;
  util::fb2::EventCount ev_;
};

}  // namespace dfly
