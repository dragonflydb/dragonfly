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

  // Streams the payloads of the blocks in order; returns 0 after the last segment.
  io::Result<size_t> ReadSome(const iovec* v, uint32_t len) override;

 private:
  struct Segment {
    uint64_t seq;
    uint64_t size;
    uint64_t uid;
    // Opened by Open() and kept until the reader is destroyed.
    std::unique_ptr<util::fb2::LinuxFile> file;
  };

  std::string SegmentPath(uint64_t seq) const;

  // Reads the next block of the current segment and points pending_ at its payload.
  std::error_code ReadBlock();

  std::string dir_;
  uint32_t shard_id_;
  uint32_t shard_count_;
  std::vector<Segment> segment_chain_;

  // The segment being read and the offset of its next block.
  size_t read_seg_id_ = 0;
  uint64_t offset_ = kAofSegmentHeaderSize;
  std::string buf_;
  // The unread part of the current block's payload, in buf_.
  std::string_view pending_;
};

}  // namespace dfly
