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
constexpr size_t kSegShardIdOffset = 6;
constexpr size_t kSegShardCountOffset = 10;
constexpr size_t kSegSeqOffset = 14;
constexpr size_t kSegUidOffset = 22;
constexpr size_t kSegCrcOffset = 60;
static_assert(kSegShardIdOffset == kAofMagic.size());
static_assert(kSegCrcOffset + 4 == kAofSegmentHeaderSize);

// Block header offsets.
constexpr size_t kBlkTotalBytesOffset = 0;
constexpr size_t kBlkCrcOffset = 8;
constexpr size_t kBlkFirstLsnOffset = 12;
constexpr size_t kBlkNRecordsOffset = 20;
constexpr size_t kBlkFlagsOffset = 24;
static_assert(kBlkFlagsOffset + 1 == kAofBlockHeaderSize);

void EncodeBlockHeader(const AofBlockHeader& hdr, char* dst) {
  Store64(dst + kBlkTotalBytesOffset, hdr.total_block_bytes);
  Store32(dst + kBlkCrcOffset, hdr.crc);
  Store64(dst + kBlkFirstLsnOffset, hdr.first_lsn);
  Store32(dst + kBlkNRecordsOffset, hdr.n_records);
  dst[kBlkFlagsOffset] = hdr.flags;
}

AofBlockHeader DecodeBlockHeader(const char* src) {
  return {Load64(src + kBlkTotalBytesOffset), Load32(src + kBlkCrcOffset),
          Load64(src + kBlkFirstLsnOffset), Load32(src + kBlkNRecordsOffset),
          static_cast<uint8_t>(src[kBlkFlagsOffset])};
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
  crc = absl::ExtendCrc32c(crc, {buf, kBlkCrcOffset});
  crc =
      absl::ExtendCrc32c(crc, {buf + kBlkFirstLsnOffset, kAofBlockHeaderSize - kBlkFirstLsnOffset});
  return static_cast<uint32_t>(crc);
}

}  // namespace

std::string EncodeAofSegmentHeader(const AofSegmentHeader& hdr) {
  std::string res(kAofSegmentHeaderSize, '\0');
  char* p = res.data();
  kAofMagic.copy(p, kAofMagic.size());
  Store32(p + kSegShardIdOffset, hdr.shard_id);
  Store32(p + kSegShardCountOffset, hdr.shard_count);
  Store64(p + kSegSeqOffset, hdr.seq);
  Store64(p + kSegUidOffset, hdr.segment_uid);
  Store32(p + kSegCrcOffset, static_cast<uint32_t>(absl::ComputeCrc32c({p, kSegCrcOffset})));
  return res;
}

std::optional<AofSegmentHeader> DecodeAofSegmentHeader(std::string_view src) {
  if (src.size() < kAofSegmentHeaderSize || !src.starts_with(kAofMagic))
    return std::nullopt;
  const char* p = src.data();
  uint32_t crc = static_cast<uint32_t>(absl::ComputeCrc32c({p, kSegCrcOffset}));
  if (crc != Load32(p + kSegCrcOffset))
    return std::nullopt;
  return AofSegmentHeader{Load32(p + kSegShardIdOffset), Load32(p + kSegShardCountOffset),
                          Load64(p + kSegSeqOffset), Load64(p + kSegUidOffset)};
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
