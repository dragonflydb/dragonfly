// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/tagged_chunk.h"

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

}  // namespace dfly
