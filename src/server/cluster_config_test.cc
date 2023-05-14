// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/cluster_config.h"

#include <gmock/gmock-matchers.h>

#include "base/gtest.h"
#include "base/logging.h"

using namespace std;
using Node = dfly::ClusterConfig::Node;

namespace dfly {

MATCHER_P(NodeMatches, expected, "") {
  return arg.id == expected.id && arg.ip == expected.ip && arg.port == expected.port;
}

class ClusterConfigTest : public ::testing::Test {
 protected:
  const string kMyId = "my-id";
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
  // Test that empty-initialization causes none of the slots to be owned locally.
  for (SlotId i : {0, 1, 10, 100, 1'000, 10'000, 16'000, 0x3FFF}) {
    EXPECT_FALSE(config_.IsMySlot(i));
  }
}

TEST_F(ClusterConfigTest, ConfigSetInvalidEmpty) {
  EXPECT_FALSE(config_.SetConfig({}));
}

TEST_F(ClusterConfigTest, ConfigSetInvalidMissingSlots) {
  EXPECT_FALSE(config_.SetConfig({{.slot_ranges = {{.start = 0, .end = 16000}},
                                   .master = {.id = "other", .ip = "192.168.0.100", .port = 7000},
                                   .replicas = {}}}));
}

TEST_F(ClusterConfigTest, ConfigSetInvalidDoubleBookedSlot) {
  EXPECT_FALSE(config_.SetConfig({{.slot_ranges = {{.start = 0, .end = 0x3FFF}},
                                   .master = {.id = "other", .ip = "192.168.0.100", .port = 7000},
                                   .replicas = {}},
                                  {.slot_ranges = {{.start = 0, .end = 0}},
                                   .master = {.id = "other2", .ip = "192.168.0.101", .port = 7001},
                                   .replicas = {}}}));
}

TEST_F(ClusterConfigTest, ConfigSetOk) {
  EXPECT_TRUE(config_.SetConfig({{.slot_ranges = {{.start = 0, .end = 0x3FFF}},
                                  .master = {.id = "other", .ip = "192.168.0.100", .port = 7000},
                                  .replicas = {}}}));
  EXPECT_THAT(config_.GetMasterNodeForSlot(0),
              NodeMatches(Node{.id = "other", .ip = "192.168.0.100", .port = 7000}));
}

TEST_F(ClusterConfigTest, ConfigSetOkWithReplicas) {
  EXPECT_TRUE(config_.SetConfig(
      {{.slot_ranges = {{.start = 0, .end = 0x3FFF}},
        .master = {.id = "other-master", .ip = "192.168.0.100", .port = 7000},
        .replicas = {{.id = "other-replica", .ip = "192.168.0.101", .port = 7001}}}}));
  EXPECT_THAT(config_.GetMasterNodeForSlot(0),
              NodeMatches(Node{.id = "other-master", .ip = "192.168.0.100", .port = 7000}));
}

TEST_F(ClusterConfigTest, ConfigSetMultipleInstances) {
  EXPECT_TRUE(config_.SetConfig(
      {{.slot_ranges = {{.start = 0, .end = 5'000}},
        .master = {.id = "other-master", .ip = "192.168.0.100", .port = 7000},
        .replicas = {{.id = "other-replica", .ip = "192.168.0.101", .port = 7001}}},
       {.slot_ranges = {{.start = 5'001, .end = 10'000}},
        .master = {.id = kMyId, .ip = "192.168.0.102", .port = 7002},
        .replicas = {{.id = "other-replica2", .ip = "192.168.0.103", .port = 7003}}},
       {.slot_ranges = {{.start = 10'001, .end = 0x3FFF}},
        .master = {.id = "other-master3", .ip = "192.168.0.104", .port = 7004},
        .replicas = {{.id = "other-replica3", .ip = "192.168.0.105", .port = 7005}}}}));
  {
    for (int i = 0; i <= 5'000; ++i) {
      EXPECT_THAT(config_.GetMasterNodeForSlot(i),
                  NodeMatches(Node{.id = "other-master", .ip = "192.168.0.100", .port = 7000}));
      EXPECT_FALSE(config_.IsMySlot(i));
    }
  }
  {
    for (int i = 5'001; i <= 10'000; ++i) {
      EXPECT_THAT(config_.GetMasterNodeForSlot(i),
                  NodeMatches(Node{.id = kMyId, .ip = "192.168.0.102", .port = 7002}));
      EXPECT_TRUE(config_.IsMySlot(i));
    }
  }
  {
    for (int i = 10'001; i <= 0x3FFF; ++i) {
      EXPECT_THAT(config_.GetMasterNodeForSlot(i),
                  NodeMatches(Node{.id = "other-master3", .ip = "192.168.0.104", .port = 7004}));
      EXPECT_FALSE(config_.IsMySlot(i));
    }
  }
}

}  // namespace dfly
