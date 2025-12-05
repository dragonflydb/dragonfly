#include "server/tiering/serialized_map.h"

#include <mimalloc.h>

#include <map>

#include "base/logging.h"
#include "core/detail/listpack_wrap.h"
#include "gmock/gmock.h"

extern "C" {
#include "redis/zmalloc.h"
}

namespace dfly::tiering {

using namespace std;

struct SerializedMapTest : public ::testing::Test {
  static void SetUpTestSuite() {
    init_zmalloc_threadlocal(mi_heap_get_backing());  // to use ListpackWrap
  }
};

TEST_F(SerializedMapTest, TestBasic) {
  const vector<std::pair<string, string>> kBase = {{"first key", "first value"},
                                                   {"second key", "second value"},
                                                   {"third key", "third value"},
                                                   {"fourth key", "fourth value"},
                                                   {"fifth key", "fifth value"}};
  auto lw = detail::ListpackWrap::WithCapacity(100);
  for (const auto& [k, v] : kBase)
    lw.Insert(k, v, false);
  lw.GetPointer();  // to mark as non dirty // TODO: remove

  // Serialize kBase to buffer
  std::string buffer;
  buffer.resize(SerializedMap::EstimateSize(lw.Bytes(), lw.size()));
  size_t written = SerializedMap::Serialize(lw, absl::MakeSpan(buffer));
  EXPECT_GT(written, 0u);
  buffer.resize(written);

  // Build map over buffer and check size
  SerializedMap map{buffer};
  EXPECT_EQ(map.size(), kBase.size());

  // Check entries
  size_t idx = 0;
  for (auto it = map.begin(); it != map.end(); ++it, ++idx) {
    EXPECT_EQ((*it).first, kBase[idx].first);
    EXPECT_EQ((*it).second, kBase[idx].second);
  }
}

}  // namespace dfly::tiering
