// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/tagged_chunk.h"

#include "base/endian.h"
#include "base/gtest.h"
#include "base/logging.h"

using namespace dfly;

namespace {

void AppendStringWithHeader(std::string_view msg, std::string* out) {
  std::string payload{msg};
  const uint32_t sz = msg.size();
  payload.insert(0, 8, '\0');
  absl::little_endian::Store32(payload.data(), 0);
  absl::little_endian::Store32(payload.data() + 4, sz);
  out->append(payload);
}

}  // namespace

TEST(TagStrippingSource, SimpleStream) {
  std::string payload;
  AppendStringWithHeader("", &payload);
  AppendStringWithHeader("this is a string", &payload);
  AppendStringWithHeader("this is another string", &payload);

  io::BytesSource upstream{payload};
  TagStrippingSource source{&upstream};

  std::string output;
  uint8_t buffer[4];

  iovec v{buffer, 4};

  size_t n = 0;
  do {
    auto result = source.ReadSome(&v, 1);
    EXPECT_TRUE(result);
    n = result.value();
    output.append(reinterpret_cast<const char*>(buffer), n);
  } while (n > 0);

  EXPECT_EQ(output, "this is a stringthis is another string");
}
