// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/cluster_config.h"

#include <gmock/gmock-matchers.h>

#include "base/gtest.h"
#include "base/logging.h"

using namespace std;
using testing::Pointwise;
using Node = dfly::ClusterConfig::Node;

namespace dfly {

MATCHER(NodeMatches, "") {
  auto first = std::get<0>(arg);
  auto second = std::get<1>(arg);
  return first.id == second.id && first.ip == second.ip && first.port == second.port;
}

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
  // Test that empty-initialization causes none of the slots to be owned locally.
  for (SlotId i : {0, 1, 10, 100, 1'000, 10'000, 16'000, 0x3FFF}) {
    EXPECT_FALSE(config_.IsMySlot(i));
  }
}

TEST_F(ClusterConfigTest, ConfigSetInvalidEmpty) {
  // Test that empty config means all slots are owned locally.
  EXPECT_FALSE(config_.SetConfig({}));
}

TEST_F(ClusterConfigTest, ConfigSetInvalidMissingSlots) {
  EXPECT_FALSE(config_.SetConfig({{.slot_ranges = {{.start = 0, .end = 16000}},
                                   .master = {.id = "other", .ip = "192.168.0.100", .port = 7000},
                                   .replicas = {}}}));
  vector<Node> expected = {{}};  // 1 empty master, no replicas
  EXPECT_THAT(config_.GetNodesForSlot(0), Pointwise(NodeMatches(), expected));
}

TEST_F(ClusterConfigTest, ConfigSetInvalidDoubleBookedSlot) {
  EXPECT_FALSE(config_.SetConfig({{.slot_ranges = {{.start = 0, .end = 0x3FFF}},
                                   .master = {.id = "other", .ip = "192.168.0.100", .port = 7000},
                                   .replicas = {}},
                                  {.slot_ranges = {{.start = 0, .end = 0}},
                                   .master = {.id = "other2", .ip = "192.168.0.101", .port = 7001},
                                   .replicas = {}}}));
  vector<Node> expected = {{}};  // 1 empty master, no replicas
  EXPECT_THAT(config_.GetNodesForSlot(0), Pointwise(NodeMatches(), expected));
}

TEST_F(ClusterConfigTest, ConfigSetOk) {
  EXPECT_TRUE(config_.SetConfig({{.slot_ranges = {{.start = 0, .end = 0x3FFF}},
                                  .master = {.id = "other", .ip = "192.168.0.100", .port = 7000},
                                  .replicas = {}}}));
  std::vector<Node> expected = {
      ClusterConfig::Node{.id = "other", .ip = "192.168.0.100", .port = 7000}};
  EXPECT_THAT(config_.GetNodesForSlot(0), Pointwise(NodeMatches(), expected));
}

TEST_F(ClusterConfigTest, ConfigSetOkWithReplicas) {
  EXPECT_TRUE(config_.SetConfig(
      {{.slot_ranges = {{.start = 0, .end = 0x3FFF}},
        .master = {.id = "other-master", .ip = "192.168.0.100", .port = 7000},
        .replicas = {{.id = "other-replica", .ip = "192.168.0.101", .port = 7001}}}}));
  std::vector<Node> expected = {
      ClusterConfig::Node{.id = "other-master", .ip = "192.168.0.100", .port = 7000},
      ClusterConfig::Node{.id = "other-replica", .ip = "192.168.0.101", .port = 7001}};
  EXPECT_THAT(config_.GetNodesForSlot(0), Pointwise(NodeMatches(), expected));
}

TEST_F(ClusterConfigTest, ConfigSetMultipleInstances) {
  EXPECT_TRUE(config_.SetConfig(
      {{.slot_ranges = {{.start = 0, .end = 5'000}},
        .master = {.id = "other-master", .ip = "192.168.0.100", .port = 7000},
        .replicas = {{.id = "other-replica", .ip = "192.168.0.101", .port = 7001}}},
       {.slot_ranges = {{.start = 5'001, .end = 10'000}},
        .master = {.id = "other-master2", .ip = "192.168.0.102", .port = 7002},
        .replicas = {{.id = "other-replica2", .ip = "192.168.0.103", .port = 7003}}},
       {.slot_ranges = {{.start = 10'001, .end = 0x3FFF}},
        .master = {.id = "other-master3", .ip = "192.168.0.104", .port = 7004},
        .replicas = {{.id = "other-replica3", .ip = "192.168.0.105", .port = 7005}}}}));
  {
    std::vector<Node> expected = {
        ClusterConfig::Node{.id = "other-master", .ip = "192.168.0.100", .port = 7000},
        ClusterConfig::Node{.id = "other-replica", .ip = "192.168.0.101", .port = 7001}};
    for (int i = 0; i <= 5'000; ++i) {
      EXPECT_THAT(config_.GetNodesForSlot(i), Pointwise(NodeMatches(), expected));
    }
  }
  {
    std::vector<Node> expected = {
        ClusterConfig::Node{.id = "other-master2", .ip = "192.168.0.102", .port = 7002},
        ClusterConfig::Node{.id = "other-replica2", .ip = "192.168.0.103", .port = 7003}};
    for (int i = 5'001; i <= 10'000; ++i) {
      EXPECT_THAT(config_.GetNodesForSlot(i), Pointwise(NodeMatches(), expected));
    }
  }
  {
    std::vector<Node> expected = {
        ClusterConfig::Node{.id = "other-master3", .ip = "192.168.0.104", .port = 7004},
        ClusterConfig::Node{.id = "other-replica3", .ip = "192.168.0.105", .port = 7005}};
    for (int i = 10'001; i <= 0x3FFF; ++i) {
      EXPECT_THAT(config_.GetNodesForSlot(i), Pointwise(NodeMatches(), expected));
    }
  }
}

}  // namespace dfly
