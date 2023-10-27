// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <absl/flags/reflection.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_split.h>
#include <absl/strings/strip.h>
#include <gmock/gmock.h>

#include <map>
#include <string>
#include <string_view>
#include <vector>

#include "base/flags.h"
#include "base/gtest.h"
#include "base/logging.h"
#include "server/main_service.h"
#include "server/test_utils.h"

ABSL_DECLARE_FLAG(std::string, shard_round_robin_prefix);

namespace dfly {
namespace {

using namespace std;
using testing::Contains;
using testing::Pair;

class RoundRobinSharderTest : public BaseFamilyTest {
 protected:
  RoundRobinSharderTest() : BaseFamilyTest() {
    absl::SetFlag(&FLAGS_shard_round_robin_prefix, "RR:");
    SetTestFlag("cluster_mode", "emulated");
    ResetService();
  }

  map<int, int> GetShardKeyCount() {
    map<int, int> m;

    auto res = Run({"debug", "shards"});
    for (string_view line : absl::StrSplit(res.GetString(), '\n')) {
      vector<string> parts = absl::StrSplit(line, ": ");
      if (parts.size() != 2) {
        continue;
      }

      string_view k = parts[0];
      if (!absl::StartsWith(k, "shard") || !absl::EndsWith(k, "_key_count")) {
        continue;
      }

      CHECK(absl::ConsumePrefix(&k, "shard")) << k;
      CHECK(absl::ConsumeSuffix(&k, "_key_count")) << k;
      int sid;
      CHECK(absl::SimpleAtoi(k, &sid));
      int count;
      CHECK(absl::SimpleAtoi(parts[1], &count));
      m[sid] = count;
    }
    return m;
  }
};

TEST_F(RoundRobinSharderTest, RoundRobinShard) {
  if (shard_set->size() < 2) {
    GTEST_SKIP() << "Can only test round robin with 2+ shards";
  }

  Run({"set", "{RR:key0}", "value"});
  EXPECT_THAT(GetShardKeyCount(), Contains(Pair(0, 1)));  // shard 0 has 1 key
  EXPECT_THAT(GetShardKeyCount(), Contains(Pair(1, 0)));  // shard 1 has 0 keys

  Run({"set", "{RR:key1}", "value"});
  EXPECT_THAT(GetShardKeyCount(), Contains(Pair(0, 1)));  // shard 0 has 1 key
  EXPECT_THAT(GetShardKeyCount(), Contains(Pair(1, 1)));  // shard 1 also has 1 key

  Run({"set", "{RR:key2}", "value"});
  if (shard_set->size() == 2) {
    EXPECT_THAT(GetShardKeyCount(), Contains(Pair(0, 2)));
    EXPECT_THAT(GetShardKeyCount(), Contains(Pair(1, 1)));
  } else {
    EXPECT_THAT(GetShardKeyCount(), Contains(Pair(0, 1)));
    EXPECT_THAT(GetShardKeyCount(), Contains(Pair(1, 1)));
    EXPECT_THAT(GetShardKeyCount(), Contains(Pair(2, 1)));
  }
}

}  // namespace
}  // namespace dfly
