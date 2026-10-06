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

  // Waits for in-flight I/O and fdatasyncs the segment. Returns error() if one is set.
  std::error_code Shutdown();

  // Blocks the calling fiber until PendingBytes() <= limit or an I/O error stopped the writer.
  void WaitPending(size_t limit);

  // Open block payload + sealed blocks still held: unwritten, or written behind an unwritten one.
  size_t PendingBytes() const;

  // Last record fully written (not necessarily durable), 0 if none.
  uint64_t WrittenLsn() const {
    return written_lsn_;
  }

  // Last record covered by a successful fdatasync, 0 if none.
  uint64_t DurableLsn() const {
    return durable_lsn_;
  }

  // First failed write or fdatasync. It stops the writer for good: new records are dropped and
  // syncs stop. Recovery is Shutdown() and a new writer on a new segment, at a checkpoint.
  std::error_code error() const {
    return ec_;
  }

  static std::string SegmentName(uint32_t shard_id, uint64_t seq);

 private:
  struct PendingBlock {
    // Header + payload, kept until the block is written.
    std::string bytes;
    // File offset of the block.
    size_t offset;
    // Last record ending in this block, 0 if none.
    uint64_t last_lsn;
    // Bytes written so far; a short write resumes from here.
    size_t written = 0;
    // Fully written; popped once every block before it is done too.
    bool done = false;
  };

  void OnSealed(AofSealedBlock block);
  void Submit(PendingBlock* pb);
  void OnWriteDone(PendingBlock* pb, int res);

  // Runs every kAofSyncMs: starts an fdatasync of completed writes, unless one is running or
  // nothing new was written.
  void MaybeStartFSync();
  void OnSyncDone(uint64_t sync_lsn, int res);

  std::string dir_;
  uint32_t shard_id_;
  uint32_t shard_count_;
  std::unique_ptr<util::fb2::LinuxFile> file_;
  // Created by Open, once the segment_uid that seeds block CRCs is known.
  std::optional<AofBlockBuilder> builder_;
  // File offset where the next sealed block goes.
  size_t next_offset_ = kAofSegmentHeaderSize;

  // Offset order; deque keeps references stable for completions.
  std::deque<PendingBlock> pending_;
  // Writes submitted and not yet completed; must be 0 before destruction.
  uint64_t in_flight_ = 0;

  // End of the contiguous written prefix of the file. The header is durable after Open.
  // When written_offset_ == next_offset_ all blocks are written
  size_t written_offset_ = kAofSegmentHeaderSize;
  uint64_t written_lsn_ = 0;
  uint64_t durable_lsn_ = 0;

  bool sync_in_flight_ = false;
  uint32_t tick_id_ = 0;
  // See error(); never cleared.
  std::error_code ec_;
  // Notified on write and sync completions; WaitPending and Shutdown wait on it.
  util::fb2::CondVarAny cv_;
};

}  // namespace dfly
