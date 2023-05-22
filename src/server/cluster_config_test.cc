// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/cluster_config.h"

#include <gmock/gmock-matchers.h>

#include <jsoncons/json.hpp>

#include "base/gtest.h"
#include "base/logging.h"

using namespace std;
using namespace testing;
using Node = dfly::ClusterConfig::Node;

namespace dfly {

MATCHER_P(NodeMatches, expected, "") {
  return arg.id == expected.id && arg.ip == expected.ip && arg.port == expected.port;
}

class ClusterConfigTest : public ::testing::Test {
 protected:
  JsonType ParseJson(string_view json_str) {
    optional<JsonType> opt_json = JsonFromString(json_str);
    CHECK(opt_json.has_value());
    return opt_json.value();
  }

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
  EXPECT_FALSE(config_.SetConfig(ClusterConfig::ClusterShards{}));
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

TEST_F(ClusterConfigTest, ConfigSetGetDeletedSlots) {
  auto ds =
      config_.SetConfig({{.slot_ranges = {{.start = 0, .end = 5'000}},
                          .master = {.id = kMyId, .ip = "192.168.0.100", .port = 7000},
                          .replicas = {}},
                         {.slot_ranges = {{.start = 5'001, .end = 10'000}},
                          .master = {.id = "other-master", .ip = "192.168.0.102", .port = 7002},
                          .replicas = {}},
                         {.slot_ranges = {{.start = 10'001, .end = 0x3FFF}},
                          .master = {.id = "other-master2", .ip = "192.168.0.104", .port = 7004},
                          .replicas = {}}});

  EXPECT_TRUE(ds.has_value());
  EXPECT_TRUE(ds.value().empty());  // On first config set no deleted slots.

  ds = config_.SetConfig({{.slot_ranges = {{.start = 0, .end = 6'000}},
                           .master = {.id = kMyId, .ip = "192.168.0.100", .port = 7000},
                           .replicas = {}},
                          {.slot_ranges = {{.start = 6'001, .end = 10'000}},
                           .master = {.id = "other-master", .ip = "192.168.0.102", .port = 7002},
                           .replicas = {}},
                          {.slot_ranges = {{.start = 10'001, .end = 0x3FFF}},
                           .master = {.id = "other-master2", .ip = "192.168.0.104", .port = 7004},
                           .replicas = {}}});

  EXPECT_TRUE(ds.has_value());
  EXPECT_TRUE(ds.value().empty());  // On second config no slots taken from ownership

  ds = config_.SetConfig({{.slot_ranges = {{.start = 0, .end = 5'997}},
                           .master = {.id = kMyId, .ip = "192.168.0.100", .port = 7000},
                           .replicas = {}},
                          {.slot_ranges = {{.start = 5'998, .end = 10'000}},
                           .master = {.id = "other-master", .ip = "192.168.0.102", .port = 7002},
                           .replicas = {}},
                          {.slot_ranges = {{.start = 10'001, .end = 0x3FFF}},
                           .master = {.id = "other-master2", .ip = "192.168.0.104", .port = 7004},
                           .replicas = {}}});

  EXPECT_TRUE(ds.has_value());
  EXPECT_THAT(ds.value(), UnorderedElementsAre(5'998, 5'999, 6'000));
}

TEST_F(ClusterConfigTest, ConfigSetInvalidSlotRanges) {
  // Note that slot_ranges is not an object
  EXPECT_FALSE(config_.SetConfig(ParseJson(R"json(
      [
        {
          "slot_ranges": "0,16383",
          "master": {
            "id": "abcd1234",
            "ip": "10.0.0.1",
            "port": 7000
          },
          "replicas": []
        }
      ])json")));
}

TEST_F(ClusterConfigTest, ConfigSetInvalidSlotRangeStart) {
  // Note that slot_ranges.start is not a number
  EXPECT_FALSE(config_.SetConfig(ParseJson(R"json(
      [
        {
          "slot_ranges": [
            {
              "start": "0",
              "end": 16383
            }
          ],
          "master": {
            "id": "abcd1234",
            "ip": "10.0.0.1",
            "port": 7000
          },
          "replicas": []
        }
      ])json")));
}

TEST_F(ClusterConfigTest, ConfigSetInvalidSlotRangeEnd) {
  // Note that slot_ranges.end is not a number
  EXPECT_FALSE(config_.SetConfig(ParseJson(R"json(
      [
        {
          "slot_ranges": [
            {
              "start": 0,
              "end": "16383"
            }
          ],
          "master": {
            "id": "abcd1234",
            "ip": "10.0.0.1",
            "port": 7000
          },
          "replicas": []
        }
      ])json")));
}

TEST_F(ClusterConfigTest, ConfigSetInvalidMissingMaster) {
  EXPECT_FALSE(config_.SetConfig(ParseJson(R"json(
      [
        {
          "slot_ranges": [
            {
              "start": 0,
              "end": 16383
            }
          ]
        }
      ])json")));
}

TEST_F(ClusterConfigTest, ConfigSetInvalidMasterNotObject) {
  // Note that master is not an object
  EXPECT_FALSE(config_.SetConfig(ParseJson(R"json(
      [
        {
          "slot_ranges": [
            {
              "start": 0,
              "end": 16383
            }
          ],
          "master": 123,
          "replicas": []
        }
      ])json")));
}

TEST_F(ClusterConfigTest, ConfigSetInvalidMasterMissingId) {
  EXPECT_FALSE(config_.SetConfig(ParseJson(R"json(
      [
        {
          "slot_ranges": [
            {
              "start": 0,
              "end": 16383
            }
          ],
          "master": {
            "ip": "10.0.0.0",
            "port": 8000
          },
          "replicas": []
        }
      ])json")));
}

TEST_F(ClusterConfigTest, ConfigSetInvalidMasterMissingIp) {
  EXPECT_FALSE(config_.SetConfig(ParseJson(R"json(
      [
        {
          "slot_ranges": [
            {
              "start": 0,
              "end": 16383
            }
          ],
          "master": {
            "id": "abcdefg",
            "port": 8000
          },
          "replicas": []
        }
      ])json")));
}

TEST_F(ClusterConfigTest, ConfigSetInvalidMasterMissingPort) {
  EXPECT_FALSE(config_.SetConfig(ParseJson(R"json(
      [
        {
          "slot_ranges": [
            {
              "start": 0,
              "end": 16383
            }
          ],
          "master": {
            "id": "abcdefg",
            "ip": "10.0.0.0"
          },
          "replicas": []
        }
      ])json")));
}

TEST_F(ClusterConfigTest, ConfigSetInvalidMissingReplicas) {
  EXPECT_FALSE(config_.SetConfig(ParseJson(R"json(
      [
        {
          "slot_ranges": [
            {
              "start": 0,
              "end": 16383
            }
          ],
          "master": {
            "id": "abcdefg",
            "ip": "10.0.0.0",
            "port": 8000
          }
        }
      ])json")));
}

TEST_F(ClusterConfigTest, ConfigSetInvalidRepeatingMasterId) {
  EXPECT_FALSE(config_.SetConfig(ParseJson(R"json(
      [
        {
          "slot_ranges": [
            {
              "start": 0,
              "end": 10000
            }
          ],
          "master": {
            "id": "abcdefg",
            "ip": "10.0.0.0",
            "port": 8000
          },
          "replicas": []
        },
        {
          "slot_ranges": [
            {
              "start": 10001,
              "end": 16383
            }
          ],
          "master": {
            "id": "abcdefg",
            "ip": "10.0.0.0",
            "port": 8000
          },
          "replicas": []
        }
      ])json")));
}

TEST_F(ClusterConfigTest, ConfigSetInvalidRepeatingReplicaId) {
  EXPECT_FALSE(config_.SetConfig(ParseJson(R"json(
      [
        {
          "slot_ranges": [
            {
              "start": 0,
              "end": 16383
            }
          ],
          "master": {
            "id": "abcdefg",
            "ip": "10.0.0.0",
            "port": 8000
          },
          "replicas": [
            {
              "id": "xyz",
              "ip": "10.0.0.1",
              "port": 8001
            },
            {
              "id": "xyz",
              "ip": "10.0.0.2",
              "port": 8002
            }
          ]
        }
      ])json")));
}

TEST_F(ClusterConfigTest, ConfigSetInvalidRepeatingMasterAndReplicaId) {
  EXPECT_FALSE(config_.SetConfig(ParseJson(R"json(
      [
        {
          "slot_ranges": [
            {
              "start": 0,
              "end": 16383
            }
          ],
          "master": {
            "id": "abcdefg",
            "ip": "10.0.0.0",
            "port": 8000
          },
          "replicas": [
            {
              "id": "abcdefg",
              "ip": "10.0.0.1",
              "port": 8001
            }
          ]
        }
      ])json")));
}

}  // namespace dfly
