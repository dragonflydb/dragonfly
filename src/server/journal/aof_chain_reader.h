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
  kTornTail = 1,
  kNoSegments,
  // A file matches the shard's segment pattern, but its name does not parse.
  kBadSegmentName,
  kSegmentNonContiguous,
  // A segment is shorter than its header.
  kShortSegment,
  // A header's CRC, shard id or seq does not match its file.
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

  // Finds the chain's segments, sorted by seq, and validates their headers; reads no blocks.
  // A bad name, a seq gap or a bad header fails it without touching any file: a segment gets its
  // final name only after its header is durable, so these mean outside damage, not a crash.
  std::error_code Open();

  // Streams the payloads of valid blocks in order; a record cut at the end is left for
  // JournalReader to drop. The log is a prefix by LSN: the first invalid block, LSN break or record
  // cut at a segment's end ends it, even if later segments look valid, since their records may
  // depend on the lost ones. At the end of the log returns 0 if the log ends cleanly, or
  // AofReadError::kTornTail if a damaged tail follows.
  io::Result<size_t> ReadSome(const iovec* v, uint32_t len) override;

  // Call once ReadSome() reached the end of the log, before resuming, torn tail or not. Makes the
  // log durable as read: truncates it at its end, fdatasyncs the kept segments and renames later
  // ones to *.discarded.
  std::error_code Repair();

  // Last complete record read so far, 0 if none; the log's last once the end was reached.
  uint64_t LastLsn() const {
    return last_lsn_;
  }

  // Bytes past the end of the log, dropped by Repair(). Valid once the end was reached.
  uint64_t DiscardedBytes() const {
    return discarded_bytes_;
  }

 private:
  struct Segment {
    uint64_t seq;
    uint64_t size;
    uint64_t uid;
  };

  class BlockScanner;

  std::string SegmentPath(uint64_t seq) const;

  // Marks the end of the log and computes the discarded bytes.
  void Finish(bool torn);

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
  std::optional<uint64_t> expected_lsn_;
  // The last block ended with a record that continues in the next block.
  bool partial_ = false;
  // The end of the log: not reached yet, or reached cleanly or at a damaged tail.
  enum class End : uint8_t { kNotYet, kTailReached, kTailTorn };
  End end_ = End::kNotYet;
};

}  // namespace dfly

namespace std {
template <> struct is_error_code_enum<dfly::AofReadError> : true_type {};
}  // namespace std
