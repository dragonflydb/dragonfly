#include "server/tiering/serialized_map.h"

#include <map>

#include "base/logging.h"
#include "gmock/gmock.h"

namespace dfly::tiering {

using namespace std;

class SerializedMapTest : public ::testing::Test {};

TEST_F(SerializedMapTest, TestBasic) {
  const vector<std::pair<string_view, string_view>> kBase = {{"first key", "first value"},
                                                             {"second key", "second value"},
                                                             {"third key", "third value"},
                                                             {"fourth key", "fourth value"},
                                                             {"fifth key", "fifth value"}};

  // Serialize kBase to buffer
  std::string buffer;
  buffer.resize(SerializedMap::SerializeSize(kBase));
  size_t written = SerializedMap::Serialize(kBase, absl::MakeSpan(buffer));
  EXPECT_GT(written, 0u);

  // Build map over buffer and check size
  SerializedMap map{buffer};
  EXPECT_EQ(map.size(), kBase.size());

  // Check entries
  size_t idx = 0;
  for (auto it = map.begin(); it != map.end(); ++it, ++idx) {
    EXPECT_EQ(*it, kBase[idx]);
  }
}

}  // namespace dfly::tiering
