// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <memory>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

#include "io/io.h"
#include "server/journal/aof.h"

namespace util::fb2 {
class LinuxFile;
}  // namespace util::fb2

namespace dfly {

// Reads one shard's AOF chain, its segments in seq order, as a single stream of journal records.
// An io::Source, so JournalReader can read from it directly. Runs on a proactor thread, and is
// destroyed there: it closes the segments' files.
class AofChainReader : public io::Source {
 public:
  AofChainReader(std::string dir, uint32_t shard_id, uint32_t shard_count);
  ~AofChainReader();

  // Finds the chain's segments, sorted by seq, and validates their headers.
  std::error_code Open();

  // Streams the payloads of valid blocks in order; a record cut at the end is left for
  // JournalReader to drop. The log is a prefix by LSN: the first invalid block, LSN break or record
  // cut at a segment's end ends it, even if later segments look valid, since their records may
  // depend on the lost ones. At the end of the log returns 0 if the log ends cleanly, or
  // illegal_byte_sequence if a damaged tail follows.
  io::Result<size_t> ReadSome(const iovec* v, uint32_t len) override;

  // Last complete record read so far, 0 if none; the log's last once the end was reached.
  uint64_t LastLsn() const {
    return expected_first_lsn_ ? expected_first_lsn_ - 1 : 0;
  }

  // Bytes past the end of the log. Valid once the end was reached.
  uint64_t DiscardedBytes() const {
    return discarded_bytes_;
  }

 private:
  struct Segment {
    uint64_t seq;
    uint64_t size;
    uint64_t uid;

    // Opened by Open() and kept until the reader is destroyed.
    std::unique_ptr<util::fb2::LinuxFile> file;
  };

  // The end of the log: not reached yet, or reached cleanly or at a damaged tail.
  enum class End : uint8_t { kNotYet, kTailReached, kTailTorn };

  std::string SegmentPath(uint64_t seq) const;

  // Reads the next block of the current segment and points pending_ at its payload, or ends the
  // log if the block is not valid.
  std::error_code ReadBlock();

  // Marks the end of the log and computes the discarded bytes.
  void MarkEndOfLog(End end);

  // Every block of the current segment was read.
  bool SegmentFullyRead() const {
    return offset_ == segment_chain_[read_seg_id_].size;
  }

  // Read past the last segment.
  bool AllSegmentsRead() const {
    return read_seg_id_ == segment_chain_.size();
  }

  std::string dir_;
  uint32_t shard_id_;
  uint32_t shard_count_;
  std::vector<Segment> segment_chain_;

  uint64_t discarded_bytes_ = 0;

  // The segment being read and the offset of its next block.
  size_t read_seg_id_ = 0;
  uint64_t offset_ = kAofSegmentHeaderSize;
  std::string buf_;

  // The unread part of the current block's payload, in buf_.
  std::string_view pending_;

  // The LSN the next block must start at, 0 before the first block.
  uint64_t expected_first_lsn_ = 0;

  // The last valid block ends with a record that continues in the next block.
  bool ends_mid_record_ = false;

  End end_ = End::kNotYet;
};

}  // namespace dfly
