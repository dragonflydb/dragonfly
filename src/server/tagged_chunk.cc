// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/tagged_chunk.h"

#include <absl/container/inlined_vector.h>

#include "base/endian.h"

namespace dfly {

absl::InlinedVector<iovec, 4> TagStrippingSource::GetCappedVec(const iovec* v, uint32_t len) const {
  absl::InlinedVector<iovec, 4> capped(v, v + len);

  size_t curr_index = 0;
  uint32_t to_read = remaining_payload_bytes_;

  for (; curr_index < len; ++curr_index) {
    if (to_read <= v[curr_index].iov_len) {
      capped[curr_index].iov_len = to_read;
      break;
    }

    to_read -= v[curr_index].iov_len;
  }

  capped.resize(std::min(curr_index + 1, size_t{len}));

  return capped;
}

io::Result<unsigned long> TagStrippingSource::ReadSome(const iovec* v, uint32_t len) {
  while (remaining_payload_bytes_ == 0 && !eof_) {
    if (auto header_read = ReadHeader(); !header_read) {
      return header_read;
    }
  }

  if (eof_) {
    return 0;
  }

  // len is probably always 1
  auto capped = GetCappedVec(v, len);
  return upstream_->ReadSome(capped.data(), capped.size()).transform([&](auto size) {
    remaining_payload_bytes_ -= size;
    return size;
  });
}

io::Result<unsigned long> TagStrippingSource::ReadHeader() {
  const io::MutableBytes dest{reinterpret_cast<unsigned char*>(header_.data()), header_.size()};
  return upstream_->ReadAtLeast(dest, 8).and_then([&](auto size) -> io::Result<unsigned long> {
    if (size == 0) {
      eof_ = true;
      return size;
    }

    const uint32_t tag = absl::little_endian::Load32(dest.data());
    const uint32_t payload_size = absl::little_endian::Load32(dest.data() + 4);

    if (tag != static_cast<uint32_t>(ChunkHeaderTag::Baseline)) {
      return nonstd::make_unexpected(std::make_error_code(std::errc::illegal_byte_sequence));
    }

    remaining_payload_bytes_ = payload_size;
    return size;
  });
}

}  // namespace dfly
