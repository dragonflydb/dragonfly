// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/journal/aof_chain_reader.h"

#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/strip.h>
#include <fcntl.h>
// For IORING_FSYNC_DATASYNC.
#include <linux/io_uring.h>
#include <unistd.h>

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <optional>

#include "base/logging.h"
#include "facade/facade_types.h"
#include "io/file_util.h"
#include "server/error.h"
#include "server/journal/aof_segment_writer.h"
#include "util/fibers/uring_file.h"

namespace dfly {

using namespace std;
using facade::operator""_MB;
using util::fb2::LinuxFile;
using util::fb2::OpenLinux;

namespace {

constexpr size_t kMaxBlockBytes = kAofBlockHeaderSize + kAofBlockBytes;
constexpr size_t kReadChunk = 1_MB;

error_code LastErrno() {
  return {errno, system_category()};
}

class AofReadErrorCategory : public error_category {
 public:
  const char* name() const noexcept final {
    return "aof_read";
  }

  string message(int ev) const final {
    switch (static_cast<AofReadError>(ev)) {
      case AofReadError::kTornTail:
        return "AOF log ends with a damaged tail";
      case AofReadError::kNoSegments:
        return "no AOF segments";
      case AofReadError::kBadSegmentName:
        return "unexpected AOF segment name";
      case AofReadError::kSegmentGap:
        return "gap in AOF segment seqs";
      case AofReadError::kShortSegment:
        return "AOF segment shorter than its header";
      case AofReadError::kBadSegmentHeader:
        return "invalid AOF segment header";
      case AofReadError::kShardCountChanged:
        return "AOF written with a different shard count";
    }
    return "unknown AOF read error";
  }
};

error_code Corrupt(AofReadError err, string_view path) {
  error_code ec = make_error_code(err);
  LOG(ERROR) << ec.message() << ": " << path;
  return ec;
}

}  // namespace

error_code make_error_code(AofReadError e) {
  static const AofReadErrorCategory category;
  return {static_cast<int>(e), category};
}

// Iterates the valid blocks of one segment, reading the file in large chunks.
class AofChainReader::BlockScanner {
 public:
  enum class Status : uint8_t {
    kBlock,
    // Every byte of the file was a valid block.
    kEndOfFile,
    // The bytes at offset() do not form a valid block: bad CRC, a hole, or a torn length.
    kInvalid,
  };

  struct Block {
    AofBlockHeader hdr;
    // Points into the read buffer; valid until the next call to Next().
    string_view payload;
  };

  static io::Result<unique_ptr<BlockScanner>> Open(const Segment& seg) {
    auto file = OpenLinux(seg.path, O_RDONLY, 0);
    if (!file)
      return nonstd::make_unexpected(file.error());
    return unique_ptr<BlockScanner>(new BlockScanner(std::move(*file), seg));
  }

  // Fills *block on kBlock.
  io::Result<Status> Next(Block* block) {
    uint64_t off = offset();
    if (off >= size_)
      return Status::kEndOfFile;
    if (buf_.size() - pos_ < min<uint64_t>(kMaxBlockBytes, size_ - off)) {
      if (error_code ec = Refill(); ec)
        return nonstd::make_unexpected(ec);
    }
    string_view avail(buf_.data() + pos_, buf_.size() - pos_);
    auto hdr = DecodeAofBlock(avail, uid_);
    if (!hdr || hdr->total_block_bytes > kMaxBlockBytes)
      return Status::kInvalid;
    *block = {*hdr,
              avail.substr(kAofBlockHeaderSize, hdr->total_block_bytes - kAofBlockHeaderSize)};
    pos_ += hdr->total_block_bytes;
    return Status::kBlock;
  }

  // File offset right after the last returned block.
  uint64_t offset() const {
    return buf_offset_ + pos_;
  }

 private:
  BlockScanner(unique_ptr<LinuxFile> file, const Segment& seg)
      : file_(std::move(file)), size_(seg.size), uid_(seg.uid) {
  }

  error_code Refill() {
    buf_.erase(0, pos_);
    buf_offset_ += pos_;
    pos_ = 0;
    size_t have = buf_.size();
    size_t want = min<uint64_t>(kReadChunk, size_ - (buf_offset_ + have));
    buf_.resize(have + want);
    iovec v{buf_.data() + have, want};
    return file_->Read(&v, 1, buf_offset_ + have, 0);
  }

  unique_ptr<LinuxFile> file_;
  uint64_t size_;
  uint64_t uid_;
  string buf_;
  // File offset of buf_[0].
  uint64_t buf_offset_ = kAofSegmentHeaderSize;
  size_t pos_ = 0;
};

AofChainReader::AofChainReader(string dir, uint32_t shard_id, uint32_t shard_count)
    : dir_(std::move(dir)), shard_id_(shard_id), shard_count_(shard_count) {
}

AofChainReader::~AofChainReader() = default;

error_code AofChainReader::Open() {
  return DiscoverAndIndexSegments();
}

error_code AofChainReader::DiscoverAndIndexSegments() {
  auto files = io::StatFiles(absl::StrCat(dir_, "/appendonly-", shard_id_, "-*.aof"));
  if (!files)
    return files.error();

  for (const auto& file : *files) {
    string_view name = string_view(file.name).substr(file.name.rfind('/') + 1);
    string_view seq_str = name;
    uint64_t seq = 0;
    if (!absl::ConsumePrefix(&seq_str, absl::StrCat("appendonly-", shard_id_, "-")) ||
        !absl::ConsumeSuffix(&seq_str, ".aof") || !absl::SimpleAtoi(seq_str, &seq) ||
        AofSegmentWriter::SegmentName(shard_id_, seq) != name) {
      return Corrupt(AofReadError::kBadSegmentName, file.name);
    }
    segments_.push_back({seq, file.name, file.size, 0});
  }
  if (segments_.empty())
    return AofReadError::kNoSegments;

  // Small sort, chains are not big
  sort(segments_.begin(), segments_.end(),
       [](const Segment& a, const Segment& b) { return a.seq < b.seq; });

  uint64_t next_seq = segments_.front().seq;
  for (Segment& seg : segments_) {
    // Continuity check
    if (seg.seq != next_seq++)
      return Corrupt(AofReadError::kSegmentGap, seg.path);

    auto file = OpenLinux(seg.path, O_RDONLY, 0);
    if (!file)
      return file.error();
    string hdr_bytes(kAofSegmentHeaderSize, '\0');
    iovec v{hdr_bytes.data(), hdr_bytes.size()};
    // This should not really happen, a segment only gets the final name only
    // after we write and fsync the header
    if (seg.size < kAofSegmentHeaderSize)
      return Corrupt(AofReadError::kShortSegment, seg.path);
    RETURN_ON_ERR((*file)->Read(&v, 1, 0, 0));
    RETURN_ON_ERR((*file)->Close());

    auto hdr = DecodeAofSegmentHeader(hdr_bytes);
    if (!hdr || hdr->shard_id != shard_id_ || hdr->seq != seg.seq)
      return Corrupt(AofReadError::kBadSegmentHeader, seg.path);
    // TODO: replay into a different shard count.
    if (hdr->shard_count != shard_count_)
      return Corrupt(AofReadError::kShardCountChanged, seg.path);
    seg.uid = hdr->segment_uid;
  }
  return {};
}

io::Result<size_t> AofChainReader::ReadSome(const iovec* v, uint32_t len) {
  while (pending_.empty()) {
    if (finished_)
      return torn_ ? nonstd::make_unexpected(make_error_code(AofReadError::kTornTail))
                   : io::Result<size_t>(0);
    if (!scanner_) {
      auto scanner = BlockScanner::Open(segments_[read_seg_]);
      if (!scanner)
        return nonstd::make_unexpected(scanner.error());
      scanner_ = std::move(*scanner);
    }

    BlockScanner::Block block;
    auto status = scanner_->Next(&block);
    if (!status)
      return nonstd::make_unexpected(status.error());
    if (*status == BlockScanner::Status::kInvalid)
      return Finish(true);
    if (*status == BlockScanner::Status::kEndOfFile) {
      // A record cut at the end of the segment.
      if (partial_)
        return Finish(true);
      scanner_.reset();
      if (++read_seg_ == segments_.size())
        return Finish(false);
      // The previous segment ended cleanly, so the log reaches at least this segment's header.
      end_seg_ = read_seg_;
      end_offset_ = kAofSegmentHeaderSize;
      continue;
    }

    const AofBlockHeader& hdr = block.hdr;
    bool continues = hdr.flags & kStartsWithContinuation;
    if ((expected_lsn_ && hdr.first_lsn != *expected_lsn_) || continues != partial_)
      return Finish(true);
    expected_lsn_ = hdr.first_lsn + hdr.n_records;
    partial_ = hdr.flags & kEndsWithPartial;
    if (!partial_) {
      end_seg_ = read_seg_;
      end_offset_ = scanner_->offset();
      last_lsn_ = hdr.first_lsn + hdr.n_records - 1;
    }
    pending_ = block.payload;
  }

  // Copies from the scanner's buffer into v: a payload is handed out only after its block's CRC
  // is verified and without the block headers, so reading the file straight into v saves nothing.
  size_t copied = 0;
  for (uint32_t i = 0; i < len && !pending_.empty(); ++i) {
    size_t n = min(v[i].iov_len, pending_.size());
    memcpy(v[i].iov_base, pending_.data(), n);
    pending_.remove_prefix(n);
    copied += n;
  }
  return copied;
}

io::Result<size_t> AofChainReader::Finish(bool torn) {
  finished_ = true;
  torn_ = torn;
  scanner_.reset();
  discarded_bytes_ = segments_[end_seg_].size - end_offset_;
  for (size_t i = end_seg_ + 1; i < segments_.size(); ++i)
    discarded_bytes_ += segments_[i].size;
  LOG_IF(WARNING, torn) << "AOF shard " << shard_id_ << ": damaged tail of " << discarded_bytes_
                        << " bytes after lsn " << last_lsn_;
  if (torn)
    return nonstd::make_unexpected(make_error_code(AofReadError::kTornTail));
  return 0;
}

error_code AofChainReader::Repair() {
  if (discarded_bytes_ == 0)
    return {};

  const Segment& end = segments_[end_seg_];
  if (end_offset_ < end.size) {
    if (truncate(end.path.c_str(), end_offset_) != 0)
      return LastErrno();
    auto file = OpenLinux(end.path, O_WRONLY, 0);
    if (!file)
      return file.error();
    RETURN_ON_ERR((*file)->FSync(IORING_FSYNC_DATASYNC));
    RETURN_ON_ERR((*file)->Close());
  }

  // Highest seq first, so an interrupted repair leaves the remaining segments contiguous.
  for (size_t i = segments_.size(); i-- > end_seg_ + 1;) {
    const string& path = segments_[i].path;
    if (rename(path.c_str(), absl::StrCat(path, ".discarded").c_str()) != 0)
      return LastErrno();
    RETURN_ON_ERR(AofSyncDir(dir_));
  }
  return {};
}

}  // namespace dfly
