// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/cluster_config.h"

#include <gmock/gmock-matchers.h>

#include <jsoncons/json.hpp>

#include "base/gtest.h"
#include "base/logging.h"
#include "server/test_utils.h"

using namespace std;
using namespace testing;
using Node = dfly::cluster::ClusterNodeInfo;

namespace dfly::cluster {

MATCHER_P(NodeMatches, expected, "") {
  return arg.id == expected.id && arg.ip == expected.ip && arg.port == expected.port;
}

class ClusterConfigTest : public BaseFamilyTest {
 protected:
  const string kMyId = "my-id";
};

inline string_view GetTag(string_view key) {
  return LockTagOptions::instance().Tag(key);
}

TEST_F(ClusterConfigTest, KeyTagTest) {
  SetTestFlag("lock_on_hashtags", "true");

  EXPECT_EQ(GetTag("{user1000}.following"), "user1000");

  EXPECT_EQ(GetTag("foo{{bar}}zap"), "{bar");

  EXPECT_EQ(GetTag("foo{bar}{zap}"), "bar");

  string_view key = " foo{}{bar}";
  EXPECT_EQ(key, GetTag(key));

  key = "{}foo{bar}{zap}";
  EXPECT_EQ(key, GetTag(key));

  SetTestFlag("locktag_delimiter", ":");
  TEST_InvalidateLockTagOptions();

  key = "{user1000}.following";
  EXPECT_EQ(GetTag(key), key);

  EXPECT_EQ(GetTag("bull:queue1:123"), "queue1");
  EXPECT_EQ(GetTag("bull:queue:1:123"), "queue");
  EXPECT_EQ(GetTag("bull:queue:1:123:456:789:1000"), "queue");

  key = "bull::queue:1:123";
  EXPECT_EQ(GetTag(key), key);

  SetTestFlag("locktag_delimiter", ":");
  SetTestFlag("locktag_skip_n_end_delimiters", "0");
  SetTestFlag("locktag_prefix", "bull");
  TEST_InvalidateLockTagOptions();
  EXPECT_EQ(GetTag("bull:queue:123"), "queue");
  EXPECT_EQ(GetTag("bull:queue:123:456:789:1000"), "queue");

  key = "not-bull:queue1:123";
  EXPECT_EQ(GetTag(key), key);

  SetTestFlag("locktag_delimiter", ":");
  SetTestFlag("locktag_skip_n_end_delimiters", "1");
  SetTestFlag("locktag_prefix", "bull");
  TEST_InvalidateLockTagOptions();

  key = "bull:queue1:123";
  EXPECT_EQ(GetTag(key), key);
  EXPECT_EQ(GetTag("bull:queue:1:123"), "queue:1");
  EXPECT_EQ(GetTag("bull:queue:1:123:456:789:1000"), "queue:1");

  key = "bull::queue:1:123";
  EXPECT_EQ(GetTag(key), key);

  SetTestFlag("locktag_delimiter", "|");
  SetTestFlag("locktag_skip_n_end_delimiters", "2");
  SetTestFlag("locktag_prefix", "");
  TEST_InvalidateLockTagOptions();

  EXPECT_EQ(GetTag("|a|b|c|d|e"), "a|b|c");
}

TEST_F(ClusterConfigTest, ConfigSetInvalidEmpty) {
  EXPECT_EQ(ClusterConfig::CreateFromConfig(kMyId, ClusterShardInfos{}), nullptr);
}

TEST_F(ClusterConfigTest, ConfigSetInvalidMissingSlots) {
  EXPECT_EQ(ClusterConfig::CreateFromConfig(
                kMyId, {{.slot_ranges = SlotRanges({{.start = 0, .end = 16000}}),
                         .master = {.id = "other", .ip = "192.168.0.100", .port = 7000},
                         .replicas = {},
                         .migrations = {}}}),
            nullptr);
}

TEST_F(ClusterConfigTest, ConfigSetInvalidDoubleBookedSlot) {
  EXPECT_EQ(ClusterConfig::CreateFromConfig(
                kMyId,
                ClusterShardInfos({{.slot_ranges = SlotRanges({{.start = 0, .end = 0x3FFF}}),
                                    .master = {.id = "other", .ip = "192.168.0.100", .port = 7000},
                                    .replicas = {},
                                    .migrations = {}},
                                   {.slot_ranges = SlotRanges({{.start = 0, .end = 0}}),
                                    .master = {.id = "other2", .ip = "192.168.0.101", .port = 7001},
                                    .replicas = {},
                                    .migrations = {}}})),
            nullptr);
}

TEST_F(ClusterConfigTest, ConfigSetInvalidSlotId) {
  EXPECT_EQ(ClusterConfig::CreateFromConfig(
                kMyId, {{.slot_ranges = SlotRanges({{.start = 0, .end = 0x3FFF + 1}}),
                         .master = {.id = "other", .ip = "192.168.0.100", .port = 7000},
                         .replicas = {},
                         .migrations = {}}}),
            nullptr);
}

TEST_F(ClusterConfigTest, ConfigSetOk) {
  auto config = ClusterConfig::CreateFromConfig(
      kMyId, {{.slot_ranges = SlotRanges({{.start = 0, .end = 0x3FFF}}),
               .master = {.id = "other", .ip = "192.168.0.100", .port = 7000},
               .replicas = {},
               .migrations = {}}});
  EXPECT_NE(config, nullptr);
  EXPECT_THAT(config->GetMasterNodeForSlot(0),
              NodeMatches(Node{.id = "other", .ip = "192.168.0.100", .port = 7000}));
  EXPECT_TRUE(config->GetOwnedSlots().Empty());
}

TEST_F(ClusterConfigTest, ConfigSetOkWithReplica) {
  auto config = ClusterConfig::CreateFromConfig(
      kMyId, {{.slot_ranges = SlotRanges({{.start = 0, .end = 0x3FFF}}),
               .master = {.id = "other-master", .ip = "192.168.0.100", .port = 7000},
               .replicas = {{.id = "other-replica", .ip = "192.168.0.101", .port = 7001}},
               .migrations = {}}});
  EXPECT_NE(config, nullptr);
  EXPECT_THAT(config->GetMasterNodeForSlot(0),
              NodeMatches(Node{.id = "other-master", .ip = "192.168.0.100", .port = 7000}));
}

TEST_F(ClusterConfigTest, ConfigSetMultipleInstances) {
  auto config = ClusterConfig::CreateFromConfig(
      kMyId, ClusterShardInfos(
                 {{.slot_ranges = SlotRanges({{.start = 0, .end = 5'000}}),
                   .master = {.id = "other-master", .ip = "192.168.0.100", .port = 7000},
                   .replicas = {{.id = "other-replica", .ip = "192.168.0.101", .port = 7001}},
                   .migrations = {}},
                  {.slot_ranges = SlotRanges({{.start = 5'001, .end = 10'000}}),
                   .master = {.id = kMyId, .ip = "192.168.0.102", .port = 7002},
                   .replicas = {{.id = "other-replica2", .ip = "192.168.0.103", .port = 7003}},
                   .migrations = {}},
                  {.slot_ranges = SlotRanges({{.start = 10'001, .end = 0x3FFF}}),
                   .master = {.id = "other-master3", .ip = "192.168.0.104", .port = 7004},
                   .replicas = {{.id = "other-replica3", .ip = "192.168.0.105", .port = 7005}},
                   .migrations = {}}}));
  EXPECT_NE(config, nullptr);
  SlotSet owned_slots = config->GetOwnedSlots();
  EXPECT_EQ(owned_slots.ToSlotRanges().Size(), 1);
  EXPECT_EQ(owned_slots.Count(), 5'000);

  {
    for (int i = 0; i <= 5'000; ++i) {
      EXPECT_THAT(config->GetMasterNodeForSlot(i),
                  NodeMatches(Node{.id = "other-master", .ip = "192.168.0.100", .port = 7000}));
      EXPECT_FALSE(config->IsMySlot(i));
      EXPECT_FALSE(owned_slots.Contains(i));
    }
  }
  {
    for (int i = 5'001; i <= 10'000; ++i) {
      EXPECT_THAT(config->GetMasterNodeForSlot(i),
                  NodeMatches(Node{.id = kMyId, .ip = "192.168.0.102", .port = 7002}));
      EXPECT_TRUE(config->IsMySlot(i));
      EXPECT_TRUE(owned_slots.Contains(i));
    }
  }
  {
    for (int i = 10'001; i <= 0x3FFF; ++i) {
      EXPECT_THAT(config->GetMasterNodeForSlot(i),
                  NodeMatches(Node{.id = "other-master3", .ip = "192.168.0.104", .port = 7004}));
      EXPECT_FALSE(config->IsMySlot(i));
      EXPECT_FALSE(owned_slots.Contains(i));
    }
  }
}

TEST_F(ClusterConfigTest, ConfigSetInvalidSlotRanges) {
  // Note that slot_ranges is not an object
  EXPECT_EQ(ClusterConfig::CreateFromConfig(kMyId, R"json(
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
                ])json"),
            nullptr);
}

TEST_F(ClusterConfigTest, ConfigSetInvalidSlotRangeStart) {
  // Note that slot_ranges.start is not a number
  EXPECT_EQ(ClusterConfig::CreateFromConfig(kMyId, R"json(
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
                ])json"),
            nullptr);
}

TEST_F(ClusterConfigTest, ConfigSetInvalidSlotRangeEnd) {
  // Note that slot_ranges.end is not a number
  EXPECT_EQ(ClusterConfig::CreateFromConfig(kMyId, R"json(
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
                ])json"),
            nullptr);
}

TEST_F(ClusterConfigTest, ConfigSetInvalidMissingMaster) {
  EXPECT_EQ(ClusterConfig::CreateFromConfig(kMyId, R"json(
                [
                  {
                    "slot_ranges": [
                      {
                        "start": 0,
                        "end": 16383
                      }
                    ]
                  }
                ])json"),
            nullptr);
}

TEST_F(ClusterConfigTest, ConfigSetInvalidMasterNotObject) {
  // Note that master is not an object
  EXPECT_EQ(ClusterConfig::CreateFromConfig(kMyId, R"json(
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
                ])json"),
            nullptr);
}

TEST_F(ClusterConfigTest, ConfigSetInvalidMasterMissingId) {
  EXPECT_EQ(ClusterConfig::CreateFromConfig(kMyId, R"json(
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
                ])json"),
            nullptr);
}

TEST_F(ClusterConfigTest, ConfigSetInvalidMasterMissingIp) {
  EXPECT_EQ(ClusterConfig::CreateFromConfig(kMyId, R"json(
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
                ])json"),
            nullptr);
}

TEST_F(ClusterConfigTest, ConfigSetInvalidMasterMissingPort) {
  EXPECT_EQ(ClusterConfig::CreateFromConfig(kMyId, R"json(
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
                ])json"),
            nullptr);
}

TEST_F(ClusterConfigTest, ConfigSetInvalidMissingReplicas) {
  EXPECT_EQ(ClusterConfig::CreateFromConfig(kMyId, R"json(
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
                ])json"),
            nullptr);
}

TEST_F(ClusterConfigTest, ConfigSetInvalidRepeatingMasterId) {
  EXPECT_EQ(ClusterConfig::CreateFromConfig(kMyId, R"json(
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
                ])json"),
            nullptr);
}

TEST_F(ClusterConfigTest, ConfigSetInvalidRepeatingReplicaId) {
  EXPECT_EQ(ClusterConfig::CreateFromConfig(kMyId, R"json(
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
                ])json"),
            nullptr);
}

TEST_F(ClusterConfigTest, ConfigSetInvalidRepeatingMasterAndReplicaId) {
  EXPECT_EQ(ClusterConfig::CreateFromConfig(kMyId, R"json(
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
                ])json"),
            nullptr);
}

TEST_F(ClusterConfigTest, ConfigSetMigrations) {
  const auto* config_str = R"json(
  [
    {
      "slot_ranges": [ { "start": 0, "end": 8000 } ],
      "master": { "id": "id0", "ip": "localhost", "port": 3000 },
      "replicas": [],
      "migrations": [{ "slot_ranges": [ { "start": 7000, "end": 8000 } ]
                     , "ip": "127.0.0.1", "port" : 9001, "node_id": "id1" }]
    },
    {
      "slot_ranges": [ { "start": 8001, "end": 16383 } ],
      "master": { "id": "id1", "ip": "localhost", "port": 3001 },
      "replicas": []
    }
  ])json";

  auto config1 = ClusterConfig::CreateFromConfig("id0", config_str);
  EXPECT_EQ(
      config1->GetNewOutgoingMigrations(nullptr),
      (std::vector<MigrationInfo>{{.slot_ranges = SlotRanges({{7000, 8000}}),
                                   .node_info = {.id = "id1", .ip = "127.0.0.1", .port = 9001}}}));

  EXPECT_TRUE(config1->GetFinishedOutgoingMigrations(nullptr).empty());
  EXPECT_TRUE(config1->GetNewIncomingMigrations(nullptr).empty());
  EXPECT_TRUE(config1->GetFinishedIncomingMigrations(nullptr).empty());

  auto config2 = ClusterConfig::CreateFromConfig("id1", config_str);
  EXPECT_EQ(
      config2->GetNewIncomingMigrations(nullptr),
      (std::vector<MigrationInfo>{{.slot_ranges = SlotRanges({{7000, 8000}}),
                                   .node_info = {.id = "id0", .ip = "127.0.0.1", .port = 9001}}}));

  EXPECT_TRUE(config2->GetFinishedOutgoingMigrations(nullptr).empty());
  EXPECT_TRUE(config2->GetNewOutgoingMigrations(nullptr).empty());
  EXPECT_TRUE(config2->GetFinishedIncomingMigrations(nullptr).empty());

  auto config3 = ClusterConfig::CreateFromConfig("id2", config_str);
  EXPECT_TRUE(config3->GetFinishedOutgoingMigrations(nullptr).empty());
  EXPECT_TRUE(config3->GetNewIncomingMigrations(nullptr).empty());
  EXPECT_TRUE(config3->GetFinishedIncomingMigrations(nullptr).empty());
  EXPECT_TRUE(config3->GetNewOutgoingMigrations(nullptr).empty());

  const auto* config_str2 = R"json(
  [
    {
      "slot_ranges": [ { "start": 0, "end": 6999 } ],
      "master": { "id": "id0", "ip": "localhost", "port": 3000 },
      "replicas": []
    },
    {
      "slot_ranges": [ { "start": 7000, "end": 16383 } ],
      "master": { "id": "id1", "ip": "localhost", "port": 3001 },
      "replicas": []
    }
  ])json";

  auto config4 = ClusterConfig::CreateFromConfig("id0", config_str2);
  auto config5 = ClusterConfig::CreateFromConfig("id1", config_str2);

  EXPECT_EQ(
      config4->GetFinishedOutgoingMigrations(config1),
      (std::vector<MigrationInfo>{{.slot_ranges = SlotRanges({{7000, 8000}}),
                                   .node_info = {.id = "id1", .ip = "127.0.0.1", .port = 9001}}}));
  EXPECT_TRUE(config4->GetNewIncomingMigrations(config1).empty());
  EXPECT_TRUE(config4->GetFinishedIncomingMigrations(config1).empty());
  EXPECT_TRUE(config4->GetNewOutgoingMigrations(config1).empty());

  EXPECT_EQ(
      config5->GetFinishedIncomingMigrations(config2),
      (std::vector<MigrationInfo>{{.slot_ranges = SlotRanges({{7000, 8000}}),
                                   .node_info = {.id = "id0", .ip = "127.0.0.1", .port = 9001}}}));
  EXPECT_TRUE(config5->GetNewIncomingMigrations(config2).empty());
  EXPECT_TRUE(config5->GetFinishedOutgoingMigrations(config2).empty());
  EXPECT_TRUE(config5->GetNewOutgoingMigrations(config2).empty());
}

TEST_F(ClusterConfigTest, InvalidConfigMigrationsWithoutIP) {
  auto config = ClusterConfig::CreateFromConfig("id0", R"json(
  [
    {
      "slot_ranges": [ { "start": 0, "end": 8000 } ],
      "master": { "id": "id0", "ip": "localhost", "port": 3000 },
      "replicas": [],
      "migrations": [{ "slot_ranges": [ { "start": 7000, "end": 8000 } ]
                     , "port" : 9001, "node_id": "id1" }]
    },
    {
      "slot_ranges": [ { "start": 8001, "end": 16383 } ],
      "master": { "id": "id1", "ip": "localhost", "port": 3001 },
      "replicas": []
    }
  ])json");

  EXPECT_EQ(config, nullptr);
}

TEST_F(ClusterConfigTest, SlotSetAPI) {
  {
    SlotSet ss(false);
    EXPECT_EQ(ss.ToSlotRanges(), SlotRanges());
    EXPECT_FALSE(ss.All());
    EXPECT_TRUE(ss.Empty());
  }
  {
    SlotSet ss(true);
    EXPECT_EQ(ss.ToSlotRanges(), SlotRanges({{0, SlotRange::kMaxSlotId}}));
    EXPECT_TRUE(ss.All());
    EXPECT_FALSE(ss.Empty());
  }
  {
    SlotSet ss(SlotRanges({{0, 1000}, {1001, 2000}}));
    EXPECT_EQ(ss.ToSlotRanges(), SlotRanges({SlotRange{0, 2000}}));
    EXPECT_EQ(ss.Count(), 2001);

    for (uint16_t i = 0; i < 2000; ++i) {
      EXPECT_TRUE(ss.Contains(i));
    }
    for (uint16_t i = 2001; i <= SlotRange::kMaxSlotId; ++i) {
      EXPECT_FALSE(ss.Contains(i));
    }

    EXPECT_FALSE(ss.All());
    EXPECT_FALSE(ss.Empty());

    ss.Set(5010, true);
    EXPECT_EQ(ss.ToSlotRanges(), SlotRanges({{0, 2000}, {5010, 5010}}));

    ss.Set(SlotRanges({{5000, 5100}}), true);
    EXPECT_EQ(ss.ToSlotRanges(), SlotRanges({{0, 2000}, {5000, 5100}}));

    ss.Set(5050, false);
    EXPECT_EQ(ss.ToSlotRanges(), SlotRanges({{0, 2000}, {5000, 5049}, {5051, 5100}}));

    ss.Set(5500, false);
    EXPECT_EQ(ss.ToSlotRanges(), SlotRanges({{0, 2000}, {5000, 5049}, {5051, 5100}}));

    ss.Set(SlotRanges({{5090, 5100}}), false);
    EXPECT_EQ(ss.ToSlotRanges(), SlotRanges({{0, 2000}, {5000, 5049}, {5051, 5089}}));

    SlotSet ss1(SlotRanges({{1001, 2000}}));

    EXPECT_EQ(ss.GetRemovedSlots(ss1).ToSlotRanges(),
              SlotRanges({{0, 1000}, {5000, 5049}, {5051, 5089}}));
    EXPECT_EQ(ss1.GetRemovedSlots(ss).ToSlotRanges(), SlotRanges());
  }
}

TEST_F(ClusterConfigTest, ConfigComparison) {
  auto config1 = ClusterConfig::CreateFromConfig("id0", R"json(
  [
    {
      "slot_ranges": [ { "start": 0, "end": 8000 } ],
      "master": { "id": "id0", "ip": "localhost", "port": 3000 },
      "replicas": [],
      "migrations": [{ "slot_ranges": [ { "start": 7000, "end": 8000 } ]
                     , "ip": "127.0.0.1", "port" : 9001, "node_id": "id1" }]
    },
    {
      "slot_ranges": [ { "start": 8001, "end": 16383 } ],
      "master": { "id": "id1", "ip": "localhost", "port": 3001 },
      "replicas": []
    }
  ])json");

  EXPECT_EQ(config1->GetConfig(), config1->GetConfig());

  auto config2 = ClusterConfig::CreateFromConfig("id0", R"json(
  [
    {
      "slot_ranges": [ { "start": 0, "end": 16383 } ],
      "master": { "id": "id0", "ip": "localhost", "port": 3000 },
      "replicas": [],
      "migrations": [{ "slot_ranges": [ { "start": 7000, "end": 8000 } ]
                     , "ip": "127.0.0.1", "port" : 9001, "node_id": "id1" }]
    }
  ])json");
  EXPECT_NE(config1->GetConfig(), config2->GetConfig());
  EXPECT_EQ(config2->GetConfig(), config2->GetConfig());

  auto config3 = ClusterConfig::CreateFromConfig("id0", R"json(
  [
    {
      "slot_ranges": [ { "start": 0, "end": 8000 } ],
      "master": { "id": "id0", "ip": "localhost", "port": 3000 },
      "replicas": [],
      "migrations": [{ "slot_ranges": [ { "start": 7000, "end": 8000 } ]
                     , "ip": "127.0.0.1", "port" : 9002, "node_id": "id1" }]
    },
    {
      "slot_ranges": [ { "start": 8001, "end": 16383 } ],
      "master": { "id": "id1", "ip": "localhost", "port": 3001 },
      "replicas": []
    }
  ])json");
  EXPECT_NE(config1->GetConfig(), config3->GetConfig());
  EXPECT_NE(config2->GetConfig(), config3->GetConfig());
  EXPECT_EQ(config3->GetConfig(), config3->GetConfig());

  auto config4 = ClusterConfig::CreateFromConfig("id0", R"json(
  [
    {
      "slot_ranges": [ { "start": 0, "end": 8000 } ],
      "master": { "id": "id0", "ip": "localhost", "port": 3000 },
      "replicas": [],
      "migrations": [{ "slot_ranges": [ { "start": 7000, "end": 8000 } ]
                     , "ip": "127.0.0.1", "port" : 9001, "node_id": "id2" }]
    },
    {
      "slot_ranges": [ { "start": 8001, "end": 16383 } ],
      "master": { "id": "id1", "ip": "localhost", "port": 3001 },
      "replicas": []
    }
  ])json");

  EXPECT_NE(config1->GetConfig(), config4->GetConfig());
  EXPECT_NE(config2->GetConfig(), config4->GetConfig());
  EXPECT_NE(config3->GetConfig(), config4->GetConfig());
  EXPECT_EQ(config4->GetConfig(), config4->GetConfig());

  auto config5 = ClusterConfig::CreateFromConfig("id0", R"json(
  [
    {
      "slot_ranges": [ { "start": 0, "end": 8000 } ],
      "master": { "id": "id2", "ip": "localhost", "port": 3000 },
      "replicas": [],
      "migrations": [{ "slot_ranges": [ { "start": 7000, "end": 8000 } ]
                     , "ip": "127.0.0.1", "port" : 9001, "node_id": "id1" }]
    },
    {
      "slot_ranges": [ { "start": 8001, "end": 16383 } ],
      "master": { "id": "id1", "ip": "localhost", "port": 3001 },
      "replicas": []
    }
  ])json");
  EXPECT_NE(config1->GetConfig(), config5->GetConfig());
  EXPECT_NE(config2->GetConfig(), config5->GetConfig());
  EXPECT_NE(config3->GetConfig(), config5->GetConfig());
  EXPECT_NE(config4->GetConfig(), config5->GetConfig());
  EXPECT_EQ(config5->GetConfig(), config5->GetConfig());
}

}  // namespace dfly::cluster
