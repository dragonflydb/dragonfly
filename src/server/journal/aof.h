// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/crc/crc32c.h>

#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <string_view>

namespace dfly {

// On-disk AOF format: a segment is a fixed header followed by blocks of journal records.
constexpr std::string_view kAofMagic = "DFAOF1";
// 44 base + 18 to keep allignment for the future
constexpr size_t kAofSegmentHeaderSize = 64;
constexpr size_t kAofBlockHeaderSize = 25;
// Payload cap
constexpr size_t kAofBlockBytes = 8192;

struct AofSegmentHeader {
  uint32_t shard_id = 0;
  uint32_t shard_count = 0;
  uint64_t seq = 0;
  uint64_t segment_uid = 0;
};

std::string EncodeAofSegmentHeader(const AofSegmentHeader& hdr);

// Returns nullopt on short input, bad magic or bad CRC.
std::optional<AofSegmentHeader> DecodeAofSegmentHeader(std::string_view src);

enum AofBlockFlags : uint8_t {
  kStartsWithContinuation = 1,
  kEndsWithPartial = 2,
};

struct AofBlockHeader {
  // kAofBlockHeaderSize + payload bytes
  uint64_t total_block_bytes = 0;
  uint32_t crc = 0;
  uint64_t first_lsn = 0;
  uint32_t n_records = 0;
  uint8_t flags = 0;
};

// Decodes the block at the start of src and verifies its CRC. nullopt if corrupt.
std::optional<AofBlockHeader> DecodeAofBlock(std::string_view src, uint64_t segment_uid);

struct AofSealedBlock {
  std::string bytes;  // header + payload, ready to write as-is
  uint64_t first_lsn;
  uint32_t n_records;
  uint8_t flags;
};

// Packs journal records into blocks of at most kAofBlockBytes payload. Never yields.
class AofBlockBuilder {
 public:
  // Gets sealed blocks in order, the writer queues them.
  using SealCb = std::function<void(AofSealedBlock)>;

  AofBlockBuilder(uint64_t segment_uid, SealCb seal_cb);

  // Copies the record, splitting it across blocks if needed.
  void Append(std::string_view record, uint64_t lsn);

  // Seals the open block if non-empty.
  void Seal();

 private:
  size_t PayloadSize() const {
    return buf_.size() - kAofBlockHeaderSize;
  }

  void SealOpen();
  void ResetOpen();

  uint64_t segment_uid_;
  absl::crc32c_t crc_;
  // header placeholder + payload of the open block
  std::string buf_;
  uint64_t first_lsn_ = 0;
  uint32_t n_records_ = 0;
  uint8_t flags_ = 0;
  std::optional<uint64_t> next_lsn_;
  SealCb seal_cb_;
};

}  // namespace dfly
