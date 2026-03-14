// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <array>
#include <cstdint>
#include <string>

namespace dfly {

enum class ChunkTag : uint8_t {
  Baseline = 0,
  Journal = 1,
};

ChunkTag CastTag(uint32_t raw_tag);

struct TaggedChunkHeader {
  static constexpr std::size_t kHeaderSize = 8;
  ChunkTag tag;
  uint32_t payload_size;
  std::array<char, 8> Serialize() const;
  static TaggedChunkHeader Deserialize(const char* buffer);
};

void PrependChunkHeader(ChunkTag tag, std::string* payload);

}  // namespace dfly
