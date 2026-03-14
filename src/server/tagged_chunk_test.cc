// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/tagged_chunk.h"

#include "base/endian.h"
#include "base/gtest.h"
#include "base/logging.h"

using namespace dfly;

TEST(TaggedChunk, RoundTrip) {
  const TaggedChunkHeader header{ChunkTag::Baseline, 514};
  const auto serialized = header.Serialize();
  constexpr std::array<char, 8> expected{0, 0, 0, 0, 2, 2, 0, 0};
  EXPECT_EQ(serialized, expected);
  const auto [tag, payload_size] = TaggedChunkHeader::Deserialize(serialized.data());
  EXPECT_EQ(tag, header.tag);
  EXPECT_EQ(payload_size, header.payload_size);
}

TEST(TaggedChunk, InvalidTag) {
  std::array<char, 8> output;
  base::LE::StoreT(uint32_t{42}, output.data());
  base::LE::StoreT(uint32_t{99}, output.data() + 4);
  ASSERT_DEATH({ TaggedChunkHeader::Deserialize(output.data()); }, "unexpected tag 42");
}

TEST(TaggedChunk, PrependHeader) {
  std::string payload{"this is a string"};
  const auto original_size = payload.size();

  PrependChunkHeader(ChunkTag::Baseline, &payload);
  EXPECT_EQ(payload.size(), original_size + TaggedChunkHeader::kHeaderSize);

  const auto [tag, payload_size] = TaggedChunkHeader::Deserialize(payload.data());

  EXPECT_EQ(tag, ChunkTag::Baseline);
  EXPECT_EQ(payload_size, original_size);
}
