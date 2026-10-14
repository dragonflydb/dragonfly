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

  string prefix = absl::StrCat("appendonly-", shard_id_, "-");
  for (const auto& file : *files) {
    string_view seq_str = string_view(file.name).substr(file.name.rfind('/') + 1);
    uint64_t seq = 0;
    if (!absl::ConsumePrefix(&seq_str, prefix) || !absl::ConsumeSuffix(&seq_str, ".aof") ||
        !absl::SimpleAtoi(seq_str, &seq)) {
      return make_error_code(errc::illegal_byte_sequence);
    }
    segments_.push_back({seq, file.size, 0, nullptr});
  }
  if (segments_.empty())
    return make_error_code(errc::no_such_file_or_directory);

  sort(segments_.begin(), segments_.end(),
       [](const Segment& a, const Segment& b) { return a.seq < b.seq; });

  for (size_t i = 0; i < segments_.size(); ++i) {
    Segment& seg = segments_[i];
    if (seg.seq != segments_.front().seq + i || seg.size < kAofSegmentHeaderSize)
      return make_error_code(errc::illegal_byte_sequence);

    auto file = OpenLinux(SegmentPath(seg.seq), O_RDONLY, 0);
    if (!file)
      return file.error();
    seg.file = std::move(*file);
    string hdr_bytes(kAofSegmentHeaderSize, '\0');
    iovec v{hdr_bytes.data(), hdr_bytes.size()};
    RETURN_ON_ERR(seg.file->Read(&v, 1, 0, 0));

    auto hdr = DecodeAofSegmentHeader(hdr_bytes);
    if (!hdr || hdr->shard_id != shard_id_ || hdr->shard_count != shard_count_ ||
        hdr->seq != seg.seq) {
      return make_error_code(errc::illegal_byte_sequence);
    }
    seg.uid = hdr->segment_uid;
  }
  return {};
}

error_code AofChainReader::ReadBlock() {
  const Segment& seg = segments_[read_seg_];
  buf_.resize(min<uint64_t>(kMaxBlockBytes, seg.size - offset_));
  iovec v{buf_.data(), buf_.size()};
  RETURN_ON_ERR(seg.file->Read(&v, 1, offset_, 0));
  auto hdr = DecodeAofBlock(buf_, seg.uid);
  if (!hdr)
    return make_error_code(errc::illegal_byte_sequence);
  pending_ =
      string_view(buf_).substr(kAofBlockHeaderSize, hdr->total_block_bytes - kAofBlockHeaderSize);
  offset_ += hdr->total_block_bytes;
  return {};
}

io::Result<size_t> AofChainReader::ReadSome(const iovec* v, uint32_t len) {
  while (pending_.empty()) {
    if (read_seg_ == segments_.size())
      return 0;
    if (offset_ == segments_[read_seg_].size) {
      ++read_seg_;
      offset_ = kAofSegmentHeaderSize;
      continue;
    }
    if (error_code ec = ReadBlock(); ec)
      return nonstd::make_unexpected(ec);
  }

  size_t copied = 0;
  for (uint32_t i = 0; i < len && !pending_.empty(); ++i) {
    size_t n = min(v[i].iov_len, pending_.size());
    memcpy(v[i].iov_base, pending_.data(), n);
    pending_.remove_prefix(n);
    copied += n;
  }
  return copied;
}

}  // namespace dfly
