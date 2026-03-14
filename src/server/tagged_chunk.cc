// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/tagged_chunk.h"

#include <absl/container/inlined_vector.h>

#include "base/endian.h"
#include "base/logging.h"

namespace dfly {

ChunkTag CastTag(uint32_t raw_tag) {
  // Expand when we start accepting journal entries
  CHECK_EQ(raw_tag, 0U) << "unexpected tag " << raw_tag;
  return static_cast<ChunkTag>(raw_tag);
}

std::array<char, 8> TaggedChunkHeader::Serialize() const {
  std::array<char, 8> output;
  base::LE::StoreT(static_cast<uint32_t>(tag), output.data());
  base::LE::StoreT(payload_size, output.data() + 4);
  return output;
}

TaggedChunkHeader TaggedChunkHeader::Deserialize(const char* buffer) {
  TaggedChunkHeader header;
  header.tag = CastTag(base::LE::LoadT<uint32_t>(buffer));
  header.payload_size = base::LE::LoadT<uint32_t>(buffer + 4);
  return header;
}

void PrependChunkHeader(const ChunkTag tag, std::string* payload) {
  const uint32_t payload_size = payload->size();
  const TaggedChunkHeader header{tag, payload_size};
  const auto serialized = header.Serialize();
  payload->insert(0, serialized.data(), TaggedChunkHeader::kHeaderSize);
}

absl::InlinedVector<iovec, 4> TagStrippingSource::GetCappedVec(const iovec* v, uint32_t len) const {
  // A copy of iovecs. Usually restricted to just one when the caller uses ReadAtLeast, but should
  // work for the general case.
  absl::InlinedVector<iovec, 4> capped(v, v + len);

  size_t curr_index = 0;
  uint32_t to_read = remaining_payload_bytes_;

  // Make sure that we restrict the capped container, so that the combined iovec capacity
  // matches remaining_payload_bytes_. If remaining_payload_bytes_ is too large, then effectively
  // capped is same as v.
  for (; curr_index < len; ++curr_index) {
    // Stop when remaining bytes will only partially fill the current iovec
    if (to_read <= v[curr_index].iov_len) {
      capped[curr_index].iov_len = to_read;
      break;
    }

    to_read -= v[curr_index].iov_len;
  }

  // If the loop stopped at the first element, curr_index_ = 0. Reserve 1 element.
  // If it stopped when curr_index = len + 1, ie remaining bytes don't fit, reserve len elements.
  capped.resize(std::min(curr_index + 1, size_t{len}));

  return capped;
}

io::Result<unsigned long> TagStrippingSource::ReadSome(const iovec* v, uint32_t len) {
  // Handle possible messages with 0 size payload by moving onto the next message
  while (remaining_payload_bytes_ == 0 && !eof_) {
    if (auto header_read = ReadHeader(); !header_read) {
      return header_read;
    }
  }

  // Found end of stream while parsing headers
  if (eof_) {
    return 0;
  }

  auto capped = GetCappedVec(v, len);
  auto n = upstream_->ReadSome(capped.data(), capped.size());
  if (!n) {
    return n;
  }

  remaining_payload_bytes_ -= n.value();
  return n;
}

io::Result<unsigned long> TagStrippingSource::ReadHeader() {
  const io::MutableBytes dest{reinterpret_cast<unsigned char*>(header_.data()), header_.size()};
  auto header_read = upstream_->ReadAtLeast(dest, TaggedChunkHeader::kHeaderSize);
  if (!header_read) {
    return header_read;
  }

  if (header_read.value() == 0) {
    eof_ = true;
    return header_read;
  }

  // Tag is thrown away right now. It should be stored and used later on.
  const auto [tag, payload_size] =
      TaggedChunkHeader::Deserialize(reinterpret_cast<const char*>(dest.data()));
  remaining_payload_bytes_ = payload_size;
  return header_read;
}

}  // namespace dfly
