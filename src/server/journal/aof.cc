// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/journal/aof.h"

#include <absl/base/internal/endian.h>

#include <algorithm>
#include <utility>

#include "base/logging.h"
#include "util/fibers/fibers.h"

namespace dfly {

using absl::little_endian::Load32;
using absl::little_endian::Load64;
using absl::little_endian::Store32;
using absl::little_endian::Store64;

namespace {

// Segment header offsets.
constexpr size_t kSegmentShardIdOffset = 6;
constexpr size_t kSegmentShardCountOffset = 10;
constexpr size_t kSegmentSeqOffset = 14;
constexpr size_t kSegmentUidOffset = 22;
constexpr size_t kSegmentCrcOffset = 60;
static_assert(kSegmentShardIdOffset == kAofMagic.size());
static_assert(kSegmentCrcOffset + 4 == kAofSegmentHeaderSize);

// Block header offsets.
constexpr size_t kBlockTotalBytesOffset = 0;
constexpr size_t kBlockCrcOffset = 8;
constexpr size_t kBlockFirstLsnOffset = 12;
constexpr size_t kBlockNRecordsOffset = 20;
constexpr size_t kBlockFlagsOffset = 24;
static_assert(kBlockFlagsOffset + 1 == kAofBlockHeaderSize);

void EncodeBlockHeader(const AofBlockHeader& hdr, char* dst) {
  Store64(dst + kBlockTotalBytesOffset, hdr.total_block_bytes);
  Store32(dst + kBlockCrcOffset, hdr.crc);
  Store64(dst + kBlockFirstLsnOffset, hdr.first_lsn);
  Store32(dst + kBlockNRecordsOffset, hdr.n_records);
  dst[kBlockFlagsOffset] = hdr.flags;
}

AofBlockHeader DecodeBlockHeader(const char* src) {
  return {Load64(src + kBlockTotalBytesOffset), Load32(src + kBlockCrcOffset),
          Load64(src + kBlockFirstLsnOffset), Load32(src + kBlockNRecordsOffset),
          static_cast<uint8_t>(src[kBlockFlagsOffset])};
}

absl::crc32c_t SeedCrc(uint64_t segment_uid) {
  char buf[8];
  Store64(buf, segment_uid);
  return absl::ComputeCrc32c({buf, sizeof(buf)});
}

// Extends crc with the header fields except the CRC itself, in on-disk order.
uint32_t FinishCrc(absl::crc32c_t crc, const AofBlockHeader& hdr) {
  char buf[kAofBlockHeaderSize];
  EncodeBlockHeader(hdr, buf);
  crc = absl::ExtendCrc32c(crc, {buf, kBlockCrcOffset});
  crc = absl::ExtendCrc32c(
      crc, {buf + kBlockFirstLsnOffset, kAofBlockHeaderSize - kBlockFirstLsnOffset});
  return static_cast<uint32_t>(crc);
}

}  // namespace

std::string EncodeAofSegmentHeader(const AofSegmentHeader& hdr) {
  std::string res(kAofSegmentHeaderSize, '\0');
  char* p = res.data();
  kAofMagic.copy(p, kAofMagic.size());
  Store32(p + kSegmentShardIdOffset, hdr.shard_id);
  Store32(p + kSegmentShardCountOffset, hdr.shard_count);
  Store64(p + kSegmentSeqOffset, hdr.seq);
  Store64(p + kSegmentUidOffset, hdr.segment_uid);
  Store32(p + kSegmentCrcOffset,
          static_cast<uint32_t>(absl::ComputeCrc32c({p, kSegmentCrcOffset})));
  return res;
}

std::optional<AofSegmentHeader> DecodeAofSegmentHeader(std::string_view src) {
  if (src.size() < kAofSegmentHeaderSize || !src.starts_with(kAofMagic))
    return std::nullopt;
  const char* p = src.data();
  uint32_t crc = static_cast<uint32_t>(absl::ComputeCrc32c({p, kSegmentCrcOffset}));
  if (crc != Load32(p + kSegmentCrcOffset))
    return std::nullopt;
  return AofSegmentHeader{Load32(p + kSegmentShardIdOffset), Load32(p + kSegmentShardCountOffset),
                          Load64(p + kSegmentSeqOffset), Load64(p + kSegmentUidOffset)};
}

std::optional<AofBlockHeader> DecodeAofBlock(std::string_view src, uint64_t segment_uid) {
  if (src.size() < kAofBlockHeaderSize)
    return std::nullopt;
  AofBlockHeader hdr = DecodeBlockHeader(src.data());
  if (hdr.total_block_bytes < kAofBlockHeaderSize || hdr.total_block_bytes > src.size())
    return std::nullopt;
  std::string_view payload =
      src.substr(kAofBlockHeaderSize, hdr.total_block_bytes - kAofBlockHeaderSize);
  if (FinishCrc(absl::ExtendCrc32c(SeedCrc(segment_uid), payload), hdr) != hdr.crc)
    return std::nullopt;
  return hdr;
}

AofBlockBuilder::AofBlockBuilder(uint64_t segment_uid, SealCb seal_cb)
    : segment_uid_(segment_uid), seal_cb_(std::move(seal_cb)) {
  ResetOpen();
}

void AofBlockBuilder::Append(std::string_view record, uint64_t lsn) {
  DCHECK(!record.empty());

  uint8_t start_flags = 0;
  while (true) {
    if (PayloadSize() == 0) {
      first_lsn_ = lsn;
      flags_ = start_flags;
    }
    size_t n = std::min(record.size(), kAofBlockBytes - PayloadSize());
    std::string_view chunk = record.substr(0, n);
    buf_.append(chunk);
    crc_ = absl::ExtendCrc32c(crc_, chunk);
    record.remove_prefix(n);
    if (record.empty())
      break;
    flags_ |= kEndsWithPartial;
    SealOpen();
    start_flags = kStartsWithContinuation;
  }

  ++n_records_;
  if (PayloadSize() == kAofBlockBytes)
    SealOpen();
}

void AofBlockBuilder::Seal() {
  if (PayloadSize() > 0)
    SealOpen();
}

void AofBlockBuilder::SealOpen() {
  util::FiberAtomicGuard guard;

  AofBlockHeader hdr{buf_.size(), 0, first_lsn_, n_records_, flags_};
  hdr.crc = FinishCrc(crc_, hdr);
  EncodeBlockHeader(hdr, buf_.data());

  seal_cb_({std::move(buf_), first_lsn_, n_records_, flags_});
  ResetOpen();
}

void AofBlockBuilder::ResetOpen() {
  buf_.clear();
  buf_.reserve(kAofBlockHeaderSize + kAofBlockBytes);
  buf_.resize(kAofBlockHeaderSize);
  crc_ = SeedCrc(segment_uid_);
  n_records_ = 0;
  flags_ = 0;
}

}  // namespace dfly
