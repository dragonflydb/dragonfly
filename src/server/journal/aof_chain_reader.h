// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

#include "io/io.h"
#include "server/journal/aof.h"

namespace dfly {

enum class AofReadError {
  // The log ends with a damaged tail: an invalid block, an LSN break or a cut record.
  kTornTail = 1,
  kNoSegments,
  kBadSegmentName,
  kSegmentGap,
  kShortSegment,
  kBadSegmentHeader,
  kShardCountChanged,
};

std::error_code make_error_code(AofReadError e);

// Reads one shard's AOF chain, its segments in seq order, as a single stream of journal records.
// io::Source cause JournalReader can read from it directly
class AofChainReader : public io::Source {
 public:
  AofChainReader(std::string dir, uint32_t shard_id, uint32_t shard_count);
  ~AofChainReader();

  // Finds and validates the segments; reads no blocks.
  std::error_code Open();

  // Streams the payloads of complete records only: a block ending with a partial record is held
  // back until that record completes. At the end of the log returns 0 if the log ends cleanly, or
  // AofReadError::kTornTail if a damaged tail follows.
  io::Result<size_t> ReadSome(const iovec* v, uint32_t len) override;

  // The accessors and Repair() are valid once ReadSome() reached the end of the log.

  // Makes the log durable as read; call it before resuming, torn tail or not. Truncates the log at
  // its end, fdatasyncs the kept segments and renames later ones to *.discarded.
  std::error_code Repair();

  // Last complete record of the log, 0 if none.
  uint64_t LastLsn() const {
    return last_lsn_;
  }

  // Bytes past the end of the log, dropped by Repair().
  uint64_t DiscardedBytes() const {
    return discarded_bytes_;
  }

 private:
  struct Segment {
    uint64_t seq;
    std::string path;
    uint64_t size;
    uint64_t uid;
  };

  class BlockScanner;

  // Builds segments_, the chain's files sorted by seq with their header fields; reads no blocks.
  std::error_code DiscoverAndIndexSegments();

  // Marks the end of the log and computes the discarded bytes; returns ReadSome's result.
  io::Result<size_t> Finish(bool torn);

  std::string dir_;
  uint32_t shard_id_;
  uint32_t shard_count_;
  std::vector<Segment> segments_;

  // End of the log: the segment and the file offset right after its last complete record.
  size_t end_seg_ = 0;
  uint64_t end_offset_ = kAofSegmentHeaderSize;
  uint64_t last_lsn_ = 0;
  uint64_t discarded_bytes_ = 0;

  // Streaming state: the segment being read and the unread part of the current payload.
  size_t read_seg_ = 0;
  std::unique_ptr<BlockScanner> scanner_;
  std::string_view pending_;
  // Payloads of blocks ending with a partial record, held back until that record completes.
  std::string held_;
  // pending_ points into held_.
  bool pending_held_ = false;
  std::optional<uint64_t> expected_lsn_;
  // The last block ended with a record that continues in the next block.
  bool partial_ = false;
  bool finished_ = false;
  bool torn_ = false;
};

}  // namespace dfly

namespace std {
template <> struct is_error_code_enum<dfly::AofReadError> : true_type {};
}  // namespace std
