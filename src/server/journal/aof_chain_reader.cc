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
#include "io/file_util.h"
#include "server/error.h"
#include "server/journal/aof_segment_writer.h"
#include "util/fibers/uring_file.h"

namespace dfly {

using namespace std;
using util::fb2::OpenLinux;

namespace {

constexpr size_t kMaxBlockBytes = kAofBlockHeaderSize + kAofBlockBytes;

}  // namespace

AofChainReader::AofChainReader(string dir, uint32_t shard_id, uint32_t shard_count)
    : dir_(std::move(dir)), shard_id_(shard_id), shard_count_(shard_count) {
}

AofChainReader::~AofChainReader() = default;

string AofChainReader::SegmentPath(uint64_t seq) const {
  return absl::StrCat(dir_, "/", AofSegmentWriter::SegmentName(shard_id_, seq));
}

error_code AofChainReader::Open() {
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

error_code AofChainReader::ReadBlock() {
  const Segment& seg = segment_chain_[read_seg_id_];

  // Up to one maximum-size block; a smaller block leaves the next block's bytes for the next read.
  buf_.resize(min<uint64_t>(kMaxBlockBytes, seg.size - offset_));
  iovec v{buf_.data(), buf_.size()};
  RETURN_ON_ERR(seg.file->Read(&v, 1, offset_, 0));

  // Bad CRC or size: a torn write or a hole.
  auto hdr = DecodeAofBlock(buf_, seg.uid);
  if (!hdr) {
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
  offset_ += hdr->total_block_bytes;

  // Every valid block is handed out right away, so memory stays bounded by one block. A record
  // that continues in the next block is buffered by JournalReader until it completes.
  // TODO: a log that ends mid-record keeps the complete records before the cut, but resuming after
  // it starts a new record, which the continuation check above rejects as torn. The resumed
  // segment's header must mark the cut record abandoned, and replay must reset its JournalReader
  // at that segment boundary.
  // Hand out the payload, without the block header.
  pending_ =
      string_view(buf_).substr(kAofBlockHeaderSize, hdr->total_block_bytes - kAofBlockHeaderSize);
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

      // Continue with the next segment, after its header.
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
  end_ = end;

  // A clean end read every segment to its end, so nothing is discarded.
  if (end == End::kTailReached)
    return;

  // The log ends at the read position, right after the last valid block.
  discarded_bytes_ = segment_chain_[read_seg_id_].size - offset_;
  for (size_t i = read_seg_id_ + 1; i < segment_chain_.size(); ++i)
    discarded_bytes_ += segment_chain_[i].size;
  LOG(WARNING) << "AOF shard " << shard_id_ << ": damaged tail of " << discarded_bytes_
               << " bytes after lsn " << LastLsn();
}

}  // namespace dfly
