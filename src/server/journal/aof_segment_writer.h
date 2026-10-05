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

  // Waits for in-flight writes and fdatasyncs the segment.
  std::error_code Shutdown();

  // Blocks the calling fiber until UnwrittenBytes() <= limit or a write fails.
  void WaitUnwritten(size_t limit);

  // Open block payload + sealed blocks not yet written.
  size_t UnwrittenBytes() const;

  // Last record fully written (not necessarily durable), 0 if none.
  uint64_t WrittenLsn() const {
    return written_lsn_;
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
  };

  void OnSealed(AofSealedBlock block);
  void Submit(PendingBlock* pb);
  void OnWriteDone(PendingBlock* pb, int res);

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
  uint64_t written_lsn_ = 0;
  std::error_code write_ec_;
  util::fb2::EventCount ev_;
};

}  // namespace dfly
