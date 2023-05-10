// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/cluster_config.h"

#include <gmock/gmock-matchers.h>

#include "base/gtest.h"
#include "base/logging.h"

using namespace std;

namespace dfly {

class ClusterConfigTest : public ::testing::Test {
 protected:
  static constexpr string_view kMyId = "my-id";
  ClusterConfig config_{kMyId};
};

TEST_F(ClusterConfigTest, KeyTagTest) {
  string key = "{user1000}.following";
  ASSERT_EQ("user1000", ClusterConfig::KeyTag(key));

  key = " foo{}{bar}";
  ASSERT_EQ(key, ClusterConfig::KeyTag(key));

  key = "foo{{bar}}zap";
  ASSERT_EQ("{bar", ClusterConfig::KeyTag(key));

  key = "foo{bar}{zap}";
  ASSERT_EQ("bar", ClusterConfig::KeyTag(key));

  key = "{}foo{bar}{zap}";
  ASSERT_EQ(key, ClusterConfig::KeyTag(key));
}

TEST_F(ClusterConfigTest, ConfigEmpty) {
  // Test that empty-initialization causes all slots to be owned locally.
  for (SlotId i : {0, 1, 10, 100, 1'000, 10'000, 16'000, 0x3FFF}) {
    EXPECT_TRUE(config_.IsMySlot(i));
    EXPECT_EQ(config_.GetNodeForSlot(i), nullopt);
  }
}

TEST_F(ClusterConfigTest, ConfigSetEmpty) {
  // Test that empty config means all slots are owned locally.
  config_.SetConfig({});
  for (SlotId i : {0, 1, 10, 100, 1'000, 10'000, 16'000, 0x3FFF}) {
    EXPECT_TRUE(config_.IsMySlot(i));
    EXPECT_EQ(config_.GetNodeForSlot(i), nullopt);
  }
}

TEST_F(ClusterConfigTest, ConfigSetPartial) {
  config_.SetConfig({
      {.slot_ranges = {{.start = 10, .end = 15}},
       .nodes = {{.id = "other",
                  .ip = "192.168.0.100",
                  .port = 7000,
                  .role = ClusterConfig::Role::kMaster}}},
      {.slot_ranges = {{.start = 100, .end = 105}},
       .nodes = {{.id = string(kMyId),
                  .ip = "192.168.0.111",
                  .port = 7000,
                  .role = ClusterConfig::Role::kMaster}}},
  });

  EXPECT_TRUE(config_.IsMySlot(9));
  EXPECT_EQ(config_.GetNodeForSlot(9), nullopt);

  for (SlotId i = 10; i <= 15; ++i) {
    EXPECT_FALSE(config_.IsMySlot(i));
    auto node = config_.GetNodeForSlot(i);
    CHECK(node.has_value());
    EXPECT_EQ(node->id, "other");
    EXPECT_EQ(node->ip, "192.168.0.100");
    EXPECT_EQ(node->port, 7000);
    EXPECT_EQ(node->role, ClusterConfig::Role::kMaster);
  }

  EXPECT_TRUE(config_.IsMySlot(16));
  EXPECT_EQ(config_.GetNodeForSlot(16), nullopt);

  for (SlotId i = 100; i <= 105; ++i) {
    EXPECT_TRUE(config_.IsMySlot(i));
    EXPECT_EQ(config_.GetNodeForSlot(i), nullopt);
  }
}

}  // namespace dfly
