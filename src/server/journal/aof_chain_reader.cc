// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/journal/aof_chain_reader.h"

#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/strip.h>
#include <fcntl.h>

#include <algorithm>
#include <cstring>

#include "base/logging.h"
#include "facade/facade_types.h"
#include "io/file_util.h"
#include "server/error.h"
#include "server/journal/aof_segment_writer.h"
#include "util/fibers/uring_file.h"

namespace dfly {

using namespace std;
using facade::operator""_KB;
using util::fb2::OpenLinux;

namespace {

constexpr size_t kMaxBlockBytes = kAofBlockHeaderSize + kAofBlockBytes;

// Matches the kernel's default readahead window (read_ahead_kb). Reads are buffered and
// sequential, so the kernel prefetches the next window while we decode this chunk.
constexpr size_t kReadChunk = 128_KB;
static_assert(kReadChunk >= kMaxBlockBytes);

}  // namespace

AofChainReader::AofChainReader(string dir, uint32_t shard_id, uint32_t shard_count)
    : dir_(std::move(dir)), shard_id_(shard_id), shard_count_(shard_count) {
}

AofChainReader::~AofChainReader() = default;

string AofChainReader::SegmentPath(uint64_t seq) const {
  return absl::StrCat(dir_, "/", AofSegmentWriter::SegmentName(shard_id_, seq));
}

error_code AofChainReader::Open() {
  DCHECK(segment_chain_.empty());
  auto files = io::StatFiles(absl::StrCat(dir_, "/appendonly-", shard_id_, "-*.aof"));
  if (!files)
    return files.error();

  // Collect the shard's segments; the seq is in the file name.
  string prefix = absl::StrCat("appendonly-", shard_id_, "-");
  for (const auto& file : *files) {
    string_view seq_str = string_view(file.name).substr(file.name.rfind('/') + 1);
    uint64_t seq = 0;
    if (!absl::ConsumePrefix(&seq_str, prefix) || !absl::ConsumeSuffix(&seq_str, ".aof") ||
        !absl::SimpleAtoi(seq_str, &seq)) {
      return make_error_code(errc::illegal_byte_sequence);
    }
    segment_chain_.push_back({seq, file.size, 0, nullptr});
  }
  if (segment_chain_.empty())
    return make_error_code(errc::no_such_file_or_directory);

  // The log runs through the segments in seq order.
  sort(segment_chain_.begin(), segment_chain_.end(),
       [](const Segment& a, const Segment& b) { return a.seq < b.seq; });

  for (size_t i = 0; i < segment_chain_.size(); ++i) {
    Segment& seg = segment_chain_[i];

    // A missing seq or a file without a full header.
    if (seg.seq != segment_chain_.front().seq + i || seg.size < kAofSegmentHeaderSize)
      return make_error_code(errc::illegal_byte_sequence);

    auto file = OpenLinux(SegmentPath(seg.seq), O_RDONLY, 0);
    if (!file)
      return file.error();
    seg.file = std::move(*file);
    string hdr_bytes(kAofSegmentHeaderSize, '\0');
    iovec v{hdr_bytes.data(), hdr_bytes.size()};
    RETURN_ON_ERR(seg.file->Read(&v, 1, 0, 0));

    // The header must be valid and match the file's name and the chain.
    auto hdr = DecodeAofSegmentHeader(hdr_bytes);
    if (!hdr || hdr->shard_id != shard_id_ || hdr->shard_count != shard_count_ ||
        hdr->seq != seg.seq) {
      return make_error_code(errc::illegal_byte_sequence);
    }

    // Seeds the CRCs of the segment's blocks.
    seg.uid = hdr->segment_uid;
  }
  return {};
}

error_code AofChainReader::RefillFromDisk() {
  const Segment& seg = segment_chain_[read_seg_id_];
  DCHECK(pending_.empty());
  DCHECK_LT(buf_.size() - pos_, kMaxBlockBytes);

  // The decoded bytes were all handed out: drop them and move the unread ones, less than one
  // block, to the front, so no byte is read twice.
  buf_.erase(0, pos_);
  pos_ = 0;
  size_t unread = buf_.size();
  size_t want = min<uint64_t>(kReadChunk, seg.size - offset_ - unread);

  // unread < kMaxBlockBytes and want <= kReadChunk, so buf_ stays below kReadChunk +
  // kMaxBlockBytes.
  buf_.resize(unread + want);
  iovec v{buf_.data() + unread, want};
  return seg.file->Read(&v, 1, offset_ + unread, 0);
}

error_code AofChainReader::ReadBlock() {
  const Segment& seg = segment_chain_[read_seg_id_];

  // The next block takes at most kMaxBlockBytes, or the rest of the segment if less is left. Read
  // more when buf_ holds fewer unread bytes than that.
  if (buf_.size() - pos_ < min<uint64_t>(kMaxBlockBytes, seg.size - offset_))
    RETURN_ON_ERR(RefillFromDisk());
  DCHECK_GE(buf_.size() - pos_, min<uint64_t>(kMaxBlockBytes, seg.size - offset_));

  // Bad CRC or size: a torn write or a hole.
  string_view avail = string_view(buf_).substr(pos_);
  auto hdr = DecodeAofBlock(avail, seg.uid);
  if (!hdr || hdr->total_block_bytes > kMaxBlockBytes) {
    MarkEndOfLog(End::kTailTorn);
    return {};
  }

  // A block that does not follow the previous one: a stale block past the end of the log.
  bool lsn_break = expected_first_lsn_ && hdr->first_lsn != expected_first_lsn_;
  bool starts_mid_record = hdr->flags & kStartsMidRecord;
  if (lsn_break || starts_mid_record != ends_mid_record_) {
    MarkEndOfLog(End::kTailTorn);
    return {};
  }

  expected_first_lsn_ = hdr->first_lsn + hdr->n_records;
  ends_mid_record_ = hdr->flags & kEndsMidRecord;
  pos_ += hdr->total_block_bytes;
  offset_ += hdr->total_block_bytes;

  // Every valid block is handed out right away, so memory stays bounded by the read chunk. A record
  // that continues in the next block is buffered by JournalReader until it completes.
  // TODO: a log that ends mid-record keeps the complete records before the cut, but resuming after
  // it starts a new record, which the continuation check above rejects as torn. The resumed
  // segment's header must mark the cut record abandoned, and replay must reset its JournalReader
  // at that segment boundary.
  // Hand out the payload, without the block header.
  pending_ = avail.substr(kAofBlockHeaderSize, hdr->total_block_bytes - kAofBlockHeaderSize);
  return {};
}

io::Result<size_t> AofChainReader::ReadSome(const iovec* v, uint32_t len) {
  while (pending_.empty()) {
    if (end_ == End::kTailTorn)
      return nonstd::make_unexpected(make_error_code(errc::illegal_byte_sequence));
    if (end_ == End::kTailReached)
      return 0;
    if (SegmentFullyRead()) {
      // A record cut at the end of the segment.
      if (ends_mid_record_) {
        MarkEndOfLog(End::kTailTorn);
        continue;
      }
      ++read_seg_id_;

      // Read past the last segment: the log ends cleanly.
      if (AllSegmentsRead()) {
        MarkEndOfLog(End::kTailReached);
        continue;
      }

      // Continue with the next segment, after its header. buf_ holds nothing unread: reads stop at
      // the segment's end.
      DCHECK_EQ(pos_, buf_.size());
      offset_ = kAofSegmentHeaderSize;
      continue;
    }
    if (error_code ec = ReadBlock(); ec)
      return nonstd::make_unexpected(ec);
  }

  // Copy as much of the current payload as fits in v.
  size_t copied = 0;
  for (uint32_t i = 0; i < len && !pending_.empty(); ++i) {
    size_t n = min(v[i].iov_len, pending_.size());
    memcpy(v[i].iov_base, pending_.data(), n);
    pending_.remove_prefix(n);
    copied += n;
  }
  return copied;
}

void AofChainReader::MarkEndOfLog(End end) {
  DCHECK(end_ == End::kNotYet);
  end_ = end;

  // A clean end read every segment to its end, so nothing is discarded.
  if (end == End::kTailReached) {
    DCHECK(AllSegmentsRead());
    return;
  }

  // The log ends at the read position, right after the last valid block.
  discarded_bytes_ = segment_chain_[read_seg_id_].size - offset_;
  for (size_t i = read_seg_id_ + 1; i < segment_chain_.size(); ++i)
    discarded_bytes_ += segment_chain_[i].size;
  LOG(WARNING) << "AOF shard " << shard_id_ << ": damaged tail of " << discarded_bytes_
               << " bytes after lsn " << LastLsn();
}

}  // namespace dfly
