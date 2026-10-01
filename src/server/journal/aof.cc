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

constexpr size_t kSegmentCrcOffset = 60;

absl::crc32c_t SeedCrc(uint64_t segment_uid) {
  char buf[8];
  Store64(buf, segment_uid);
  return absl::ComputeCrc32c({buf, sizeof(buf)});
}

// Seal-time fields go after the payload: total_block_bytes | first_lsn | n_records | flags.
uint32_t FinishCrc(absl::crc32c_t crc, const AofBlockHeader& hdr) {
  char buf[21];
  Store64(buf, hdr.total_block_bytes);
  Store64(buf + 8, hdr.first_lsn);
  Store32(buf + 16, hdr.n_records);
  buf[20] = hdr.flags;
  return static_cast<uint32_t>(absl::ExtendCrc32c(crc, {buf, sizeof(buf)}));
}

}  // namespace

std::string EncodeAofSegmentHeader(const AofSegmentHeader& hdr) {
  std::string res(kAofSegmentHeaderSize, '\0');
  char* p = res.data();
  kAofMagic.copy(p, kAofMagic.size());
  Store32(p + 6, hdr.shard_id);
  Store32(p + 10, hdr.shard_count);
  Store64(p + 14, hdr.seq);
  Store64(p + 22, hdr.segment_uid);
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
  return AofSegmentHeader{Load32(p + 6), Load32(p + 10), Load64(p + 14), Load64(p + 22)};
}

std::optional<AofBlockHeader> DecodeAofBlock(std::string_view src, uint64_t segment_uid) {
  if (src.size() < kAofBlockHeaderSize)
    return std::nullopt;
  const char* p = src.data();
  AofBlockHeader hdr{Load64(p), Load32(p + 8), Load64(p + 12), Load32(p + 20),
                     static_cast<uint8_t>(p[24])};
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
  DCHECK(!next_lsn_ || *next_lsn_ == lsn) << *next_lsn_ << " " << lsn;
  next_lsn_ = lsn + 1;

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

  char* p = buf_.data();
  Store64(p, hdr.total_block_bytes);
  Store32(p + 8, hdr.crc);
  Store64(p + 12, hdr.first_lsn);
  Store32(p + 20, hdr.n_records);
  p[24] = hdr.flags;

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
