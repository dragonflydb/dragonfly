// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/tagged_chunk.h"

#include "base/endian.h"
#include "base/logging.h"

namespace dfly {

using nonstd::make_unexpected;

std::array<char, 8> TaggedChunkHeader::Serialize() const {
  std::array<char, 8> output;
  base::LE::StoreT(static_cast<uint32_t>(tag), output.data());
  base::LE::StoreT(payload_size, output.data() + 4);
  return output;
}

io::Result<TaggedChunkHeader> TaggedChunkHeader::Deserialize(const char* buffer) {
  const auto tag = base::LE::LoadT<uint32_t>(buffer);
  if (tag != 0) {
    return make_unexpected(std::make_error_code(std::errc::illegal_byte_sequence));
  }

  const auto payload_size = base::LE::LoadT<uint32_t>(buffer + 4);
  return TaggedChunkHeader{static_cast<ChunkTag>(tag), payload_size};
}

std::error_code PrependChunkHeader(const ChunkTag tag, std::string* payload) {
  const size_t payload_size = payload->size();
  if (payload_size > std::numeric_limits<uint32_t>::max())
    return std::make_error_code(std::errc::result_out_of_range);

  const TaggedChunkHeader header{tag, static_cast<uint32_t>(payload_size)};
  const auto serialized = header.Serialize();
  payload->insert(0, serialized.data(), TaggedChunkHeader::kHeaderSize);

  return {};
}

}  // namespace dfly
