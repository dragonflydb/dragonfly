// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/journal/aof_chain_reader.h"

#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/strip.h>
#include <fcntl.h>
#include <linux/io_uring.h>
#include <unistd.h>

#include <algorithm>
#include <cstdio>
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

error_code LastErrno() {
  return {errno, system_category()};
}

}  // namespace

AofChainReader::AofChainReader(string dir, uint32_t shard_id, uint32_t shard_count)
    : dir_(std::move(dir)), shard_id_(shard_id), shard_count_(shard_count) {
}

AofChainReader::~AofChainReader() = default;

string AofChainReader::SegmentPath(uint64_t seq) const {
  return absl::StrCat(dir_, "/", AofSegmentWriter::SegmentName(shard_id_, seq));
}

error_code AofChainReader::Open(uint64_t cut_seq) {
  DCHECK(segment_chain_.empty());

  // TODO: StatFiles treats dir_ as part of a glob pattern, so a --dir with *, ? or [ breaks it.
  // List the directory with std::filesystem::directory_iterator instead.
  auto files = io::StatFiles(absl::StrCat(dir_, "/appendonly-", shard_id_, "-*.aof"));
  if (!files)
    return files.error();

  // Collect the shard's segments; the seq is in the file name.
  for (const auto& file : *files) {
    string_view name = string_view(file.name).substr(file.name.rfind('/') + 1);
    uint32_t sid = 0;
    uint64_t seq = 0;
    if (!AofSegmentWriter::ParseSegmentName(name, &sid, &seq) || sid != shard_id_) {
      LOG(ERROR) << "Unexpected AOF segment name: " << file.name;
      return AofError::kBadSegmentName;
    }

    // Before the cut: in the base, and garbage.
    if (seq < cut_seq)
      continue;
    segment_chain_.push_back({seq, file.size, 0, nullptr});
  }
  if (segment_chain_.empty())
    return AofError::kNoSegments;

  // The log runs through the segments in seq order.
  sort(segment_chain_.begin(), segment_chain_.end(),
       [](const Segment& a, const Segment& b) { return a.seq < b.seq; });

  for (size_t i = 0; i < segment_chain_.size(); ++i) {
    Segment& seg = segment_chain_[i];

    // The chain starts at the cut segment, which a checkpoint creates before committing the cut.
    if (seg.seq != cut_seq + i) {
      LOG(ERROR) << "Gap in AOF segment seqs before " << SegmentPath(seg.seq);
      return AofError::kSegmentNonContiguous;
    }

    if (seg.size < kAofSegmentHeaderSize) {
      LOG(ERROR) << "AOF segment shorter than its header: " << SegmentPath(seg.seq);
      return AofError::kShortSegment;
    }

    auto file = OpenLinux(SegmentPath(seg.seq), O_RDONLY, 0);
    if (!file)
      return file.error();
    seg.file = std::move(*file);

    // TODO: posix_fadvise(POSIX_FADV_SEQUENTIAL) doubles the kernel's readahead for our reads.

    string hdr_bytes(kAofSegmentHeaderSize, '\0');
    iovec v{hdr_bytes.data(), hdr_bytes.size()};
    RETURN_ON_ERR(seg.file->Read(&v, 1, 0, 0));

    // The header must be valid and match the file's name.
    auto hdr = DecodeAofSegmentHeader(hdr_bytes);
    if (!hdr || hdr->shard_id != shard_id_ || hdr->seq != seg.seq) {
      LOG(ERROR) << "Invalid AOF segment header: " << SegmentPath(seg.seq);
      return AofError::kBadSegmentHeader;
    }

    // TODO: replay into a different shard count.
    if (hdr->shard_count != shard_count_) {
      LOG(ERROR) << "AOF segment written with " << hdr->shard_count << " shards, now "
                 << shard_count_ << ": " << SegmentPath(seg.seq);
      return AofError::kShardCountChanged;
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
      return nonstd::make_unexpected(make_error_code(AofError::kTornTail));
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

error_code AofChainReader::Repair() {
  DCHECK(end_ != End::kNotYet);

  // The segment holding the end of the log. A clean end read every segment to its end.
  size_t end_seg_id = AllSegmentsRead() ? segment_chain_.size() - 1 : read_seg_id_;

  // Move the later segments away first, highest seq first, before cutting anything. The damaged
  // bytes are what ends the log, so they stay until no later segment is left: a repair interrupted
  // in between then stops at the same point again and redoes the remaining renames. Cutting first
  // could hide a hole, e.g. a torn first block truncated away would let the next replay take the
  // later segments as the continuation.
  for (size_t i = segment_chain_.size(); i-- > end_seg_id + 1;) {
    string path = SegmentPath(segment_chain_[i].seq);
    if (rename(path.c_str(), absl::StrCat(path, ".discarded").c_str()) != 0)
      return LastErrno();
    RETURN_ON_ERR(AofSyncDir(dir_));
  }

  // Cut the bytes after the last valid block. Never a valid block, even one that ends inside a
  // record: rewriting it could tear the complete records it holds.
  const Segment& end_seg = segment_chain_[end_seg_id];
  if (end_ == End::kTailTorn && offset_ < end_seg.size &&
      truncate(SegmentPath(end_seg.seq).c_str(), offset_) != 0) {
    return LastErrno();
  }

  // After a process crash, the kept blocks may be only in the page cache. Sync them, so the log
  // resumes after data that a later power loss cannot remove.
  for (size_t i = 0; i <= end_seg_id; ++i)
    RETURN_ON_ERR(segment_chain_[i].file->FSync(IORING_FSYNC_DATASYNC));

  return {};
}

}  // namespace dfly
