// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <absl/flags/reflection.h>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest-matchers.h>

#include <string>
#include <string_view>

#include "absl/strings/str_replace.h"
#include "absl/strings/substitute.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "base/flags.h"
#include "base/gtest.h"
#include "base/logging.h"
#include "core/compact_object.h"
#include "core/detail/gen_utils.h"
#include "core/page_usage/page_usage_stats.h"
#include "facade/facade_test.h"
#include "server/engine_shard_set.h"
#include "server/test_utils.h"
#include "server/tiered_storage.h"

ABSL_DECLARE_FLAG(bool, force_epoll);

namespace dfly::cluster {
namespace {

using namespace std;
using namespace testing;

class ClusterFamilyTest : public BaseFamilyTest {
 public:
  ClusterFamilyTest() = default;

 protected:
  virtual void ConfigureClusterFlags() {
    SetTestFlag("cluster_mode", "yes");
  }

  void SetUp() override {
    ConfigureClusterFlags();
    BaseFamilyTest::SetUp();
  }

  static constexpr string_view kInvalidConfiguration = "Invalid cluster configuration";

  string GetMyId() {
    return Run({"cluster", "myid"}).GetString();
  }

  void ConfigSingleNodeCluster(string id) {
    string config_template = R"json(
      [
        {
          "slot_ranges": [
            {
              "start": 0,
              "end": 16383
            }
          ],
          "master": {
            "id": "$0",
            "ip": "10.0.0.1",
            "port": 7000,
            "health": "online"
          },
          "replicas": []
        }
      ])json";
    string config = absl::Substitute(config_template, id);
    EXPECT_EQ(RunPrivileged({"dflycluster", "config", config}), "OK");
  }
};

TEST_F(ClusterFamilyTest, ClusterConfigInvalidJSON) {
  EXPECT_THAT(RunPrivileged({"dflycluster", "config", "invalid JSON"}),
              ErrArg("Invalid cluster configuration."));

  string cluster_info = Run({"cluster", "info"}).GetString();
  EXPECT_THAT(cluster_info, HasSubstr("cluster_state:fail"));
  EXPECT_THAT(cluster_info, HasSubstr("cluster_slots_assigned:0"));
  EXPECT_THAT(cluster_info, HasSubstr("cluster_slots_ok:0"));
  EXPECT_THAT(cluster_info, HasSubstr("cluster_known_nodes:0"));
  EXPECT_THAT(cluster_info, HasSubstr("cluster_size:0"));

  EXPECT_THAT(Run({"cluster", "shards"}), ErrArg("Cluster is not yet configured"));
  EXPECT_THAT(Run({"cluster", "slots"}), ErrArg("Cluster is not yet configured"));
  EXPECT_THAT(Run({"cluster", "nodes"}), ErrArg("Cluster is not yet configured"));
}

TEST_F(ClusterFamilyTest, ClusterConfigInvalidConfig) {
  EXPECT_THAT(RunPrivileged({"dflycluster", "config", "[]"}), ErrArg(kInvalidConfiguration));

  string cluster_info = Run({"cluster", "info"}).GetString();
  EXPECT_THAT(cluster_info, HasSubstr("cluster_state:fail"));
  EXPECT_THAT(cluster_info, HasSubstr("cluster_slots_assigned:0"));
  EXPECT_THAT(cluster_info, HasSubstr("cluster_slots_ok:0"));
  EXPECT_THAT(cluster_info, HasSubstr("cluster_known_nodes:0"));
  EXPECT_THAT(cluster_info, HasSubstr("cluster_size:0"));
}

TEST_F(ClusterFamilyTest, ClusterConfigInvalidMissingSlots) {
  EXPECT_THAT(RunPrivileged({"dflycluster", "config", R"json(
      [
        {
          "slot_ranges": [
            {
              "start": 0,
              "end": 100
            }
          ],
          "master": {
            "id": "abcd1234",
            "ip": "10.0.0.1",
            "port": 7000
          },
          "replicas": []
        }
      ])json"}),
              ErrArg(kInvalidConfiguration));

  string cluster_info = Run({"cluster", "info"}).GetString();
  EXPECT_THAT(cluster_info, HasSubstr("cluster_state:fail"));
  EXPECT_THAT(cluster_info, HasSubstr("cluster_slots_assigned:0"));
  EXPECT_THAT(cluster_info, HasSubstr("cluster_slots_ok:0"));
  EXPECT_THAT(cluster_info, HasSubstr("cluster_known_nodes:0"));
  EXPECT_THAT(cluster_info, HasSubstr("cluster_size:0"));
}

TEST_F(ClusterFamilyTest, ClusterConfigInvalidOverlappingSlots) {
  EXPECT_THAT(RunPrivileged({"dflycluster", "config", R"json(
      [
        {
          "slot_ranges": [
            {
              "start": 0,
              "end": 1000
            }
          ],
          "master": {
            "id": "abcd1234",
            "ip": "10.0.0.1",
            "port": 7000
          },
          "replicas": []
        },
        {
          "slot_ranges": [
            {
              "start": 800,
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
      ])json"}),
              ErrArg(kInvalidConfiguration));

  string cluster_info = Run({"cluster", "info"}).GetString();
  EXPECT_THAT(cluster_info, HasSubstr("cluster_state:fail"));
  EXPECT_THAT(cluster_info, HasSubstr("cluster_slots_assigned:0"));
  EXPECT_THAT(cluster_info, HasSubstr("cluster_slots_ok:0"));
  EXPECT_THAT(cluster_info, HasSubstr("cluster_known_nodes:0"));
  EXPECT_THAT(cluster_info, HasSubstr("cluster_size:0"));
}

TEST_F(ClusterFamilyTest, ClusterConfigNoReplicas) {
  ConfigSingleNodeCluster("abcd1234");
  string cluster_info = Run({"cluster", "info"}).GetString();
  EXPECT_THAT(cluster_info, HasSubstr("cluster_state:ok"));
  EXPECT_THAT(cluster_info, HasSubstr("cluster_slots_assigned:16384"));
  EXPECT_THAT(cluster_info, HasSubstr("cluster_slots_ok:16384"));
  EXPECT_THAT(cluster_info, HasSubstr("cluster_known_nodes:1"));
  EXPECT_THAT(cluster_info, HasSubstr("cluster_size:1"));

  EXPECT_THAT(
      Run({"cluster", "shards"}),
      RespElementsAre(RespArray(ElementsAre("slots",                                            //
                                            RespArray(ElementsAre(IntArg(0), IntArg(16'383))),  //
                                            "nodes",                                            //
                                            RespArray(ElementsAre(                              //
                                                RespArray(ElementsAre(                          //
                                                    "id", "abcd1234",                           //
                                                    "endpoint", "10.0.0.1",                     //
                                                    "ip", "10.0.0.1",                           //
                                                    "port", IntArg(7000),                       //
                                                    "role", "master",                           //
                                                    "replication-offset", IntArg(0),            //
                                                    "health", "online"))))))));

  EXPECT_THAT(Run({"get", "x"}).GetString(),
              testing::MatchesRegex(R"(MOVED [0-9]+ 10.0.0.1:7000)"));

  EXPECT_THAT(Run({"cluster", "slots"}),
              RespElementsAre(RespArray(ElementsAre(IntArg(0),              //
                                                    IntArg(16'383),         //
                                                    RespArray(ElementsAre(  //
                                                        "10.0.0.1",         //
                                                        IntArg(7'000),      //
                                                        "abcd1234"))))));

  EXPECT_EQ(Run({"cluster", "nodes"}),
            "abcd1234 10.0.0.1:7000@7000 master - 0 0 0 connected 0-16383\n");
}

TEST_F(ClusterFamilyTest, ClusterConfigFull) {
  EXPECT_EQ(RunPrivileged({"dflycluster", "config", R"json(
      [
        {
          "slot_ranges": [
            {
              "start": 0,
              "end": 16383
            }
          ],
          "master": {
            "id": "abcd1234",
            "ip": "10.0.0.1",
            "port": 7000,
            "health": "online"
          },
          "replicas": [
            {
              "id": "wxyz",
              "ip": "10.0.0.10",
              "port": 8000,
              "health": "online"
            }
          ]
        }
      ])json"}),
            "OK");

  string cluster_info = Run({"cluster", "info"}).GetString();
  EXPECT_THAT(cluster_info, HasSubstr("cluster_state:ok"));
  EXPECT_THAT(cluster_info, HasSubstr("cluster_slots_assigned:16384"));
  EXPECT_THAT(cluster_info, HasSubstr("cluster_slots_ok:16384"));
  EXPECT_THAT(cluster_info, HasSubstr("cluster_known_nodes:2"));
  EXPECT_THAT(cluster_info, HasSubstr("cluster_size:1"));

  EXPECT_THAT(
      Run({"cluster", "shards"}),
      RespElementsAre(RespArray(ElementsAre("slots",                                            //
                                            RespArray(ElementsAre(IntArg(0), IntArg(16'383))),  //
                                            "nodes",                                            //
                                            RespArray(ElementsAre(                              //
                                                RespArray(ElementsAre(                          //
                                                    "id", "abcd1234",                           //
                                                    "endpoint", "10.0.0.1",                     //
                                                    "ip", "10.0.0.1",                           //
                                                    "port", IntArg(7000),                       //
                                                    "role", "master",                           //
                                                    "replication-offset", IntArg(0),            //
                                                    "health", "online")),                       //
                                                RespArray(ElementsAre(                          //
                                                    "id", "wxyz",                               //
                                                    "endpoint", "10.0.0.10",                    //
                                                    "ip", "10.0.0.10",                          //
                                                    "port", IntArg(8000),                       //
                                                    "role", "replica",                          //
                                                    "replication-offset", IntArg(0),            //
                                                    "health", "online"))))))));

  EXPECT_THAT(Run({"cluster", "slots"}),
              RespElementsAre(RespArray(ElementsAre(IntArg(0),              //
                                                    IntArg(16'383),         //
                                                    RespArray(ElementsAre(  //
                                                        "10.0.0.1",         //
                                                        IntArg(7'000),      //
                                                        "abcd1234")),       //
                                                    RespArray(ElementsAre(  //
                                                        "10.0.0.10",        //
                                                        IntArg(8'000),      //
                                                        "wxyz"))))));

  EXPECT_EQ(Run({"cluster", "nodes"}),
            "abcd1234 10.0.0.1:7000@7000 master - 0 0 0 connected 0-16383\n"
            "wxyz 10.0.0.10:8000@8000 slave abcd1234 0 0 0 connected\n");
}

TEST_F(ClusterFamilyTest, ClusterConfigFullMultipleInstances) {
  EXPECT_EQ(RunPrivileged({"dflycluster", "config", R"json(
      [
        {
          "slot_ranges": [
            {
              "start": 0,
              "end": 10000
            }
          ],
          "master": {
            "id": "abcd1234",
            "ip": "10.0.0.1",
            "port": 7000,
            "health": "fail"
          },
          "replicas": [
            {
              "id": "wxyz",
              "ip": "10.0.0.10",
              "port": 8000,
              "health": "online"
            }
          ]
        },
        {
          "slot_ranges": [
            {
              "start": 10001,
              "end": 16383
            }
          ],
          "master": {
            "id": "efgh7890",
            "ip": "10.0.0.2",
            "port": 7001,
            "health": "online"
          },
          "replicas": [
            {
              "id": "qwerty",
              "ip": "10.0.0.11",
              "port": 8001,
              "health": "online"
            },
             {
              "id": "qwerty1",
              "ip": "10.0.0.12",
              "port": 8002,
              "health": "loading"
            },
             {
              "id": "qwerty2",
              "ip": "10.0.0.13",
              "port": 8003,
              "health": "fail"
            },
             {
              "id": "qwerty3",
              "ip": "10.0.0.14",
              "port": 8004,
              "health": "hidden"
            }
          ]
        }
      ])json"}),
            "OK");

  string cluster_info = Run({"cluster", "info"}).GetString();
  EXPECT_THAT(cluster_info, HasSubstr("cluster_state:ok"));
  EXPECT_THAT(cluster_info, HasSubstr("cluster_slots_assigned:16384"));
  EXPECT_THAT(cluster_info, HasSubstr("cluster_slots_ok:16384"));
  EXPECT_THAT(cluster_info, HasSubstr("cluster_known_nodes:7"));
  EXPECT_THAT(cluster_info, HasSubstr("cluster_size:2"));

  EXPECT_THAT(Run({"cluster", "shards"}),
              RespArray(ElementsAre(
                  RespArray(ElementsAre("slots",                                                 //
                                        RespArray(ElementsAre(IntArg(0), IntArg(10'000))),       //
                                        "nodes",                                                 //
                                        RespArray(ElementsAre(                                   //
                                            RespArray(ElementsAre(                               //
                                                "id", "abcd1234",                                //
                                                "endpoint", "10.0.0.1",                          //
                                                "ip", "10.0.0.1",                                //
                                                "port", IntArg(7000),                            //
                                                "role", "master",                                //
                                                "replication-offset", IntArg(0),                 //
                                                "health", "fail")),                              //
                                            RespArray(ElementsAre(                               //
                                                "id", "wxyz",                                    //
                                                "endpoint", "10.0.0.10",                         //
                                                "ip", "10.0.0.10",                               //
                                                "port", IntArg(8000),                            //
                                                "role", "replica",                               //
                                                "replication-offset", IntArg(0),                 //
                                                "health", "online")))))),                        //
                  RespArray(ElementsAre("slots",                                                 //
                                        RespArray(ElementsAre(IntArg(10'001), IntArg(16'383))),  //
                                        "nodes",                                                 //
                                        RespArray(ElementsAre(                                   //
                                            RespArray(ElementsAre(                               //
                                                "id", "efgh7890",                                //
                                                "endpoint", "10.0.0.2",                          //
                                                "ip", "10.0.0.2",                                //
                                                "port", IntArg(7001),                            //
                                                "role", "master",                                //
                                                "replication-offset", IntArg(0),                 //
                                                "health", "online")),                            //
                                            RespArray(ElementsAre(                               //
                                                "id", "qwerty",                                  //
                                                "endpoint", "10.0.0.11",                         //
                                                "ip", "10.0.0.11",                               //
                                                "port", IntArg(8001),                            //
                                                "role", "replica",                               //
                                                "replication-offset", IntArg(0),                 //
                                                "health", "online")),                            //
                                            RespArray(ElementsAre(                               //
                                                "id", "qwerty1",                                 //
                                                "endpoint", "10.0.0.12",                         //
                                                "ip", "10.0.0.12",                               //
                                                "port", IntArg(8002),                            //
                                                "role", "replica",                               //
                                                "replication-offset", IntArg(0),                 //
                                                "health", "loading")),                           //
                                            RespArray(ElementsAre(                               //
                                                "id", "qwerty2",                                 //
                                                "endpoint", "10.0.0.13",                         //
                                                "ip", "10.0.0.13",                               //
                                                "port", IntArg(8003),                            //
                                                "role", "replica",                               //
                                                "replication-offset", IntArg(0),                 //
                                                "health", "fail")))))))));

  EXPECT_THAT(Run({"cluster", "slots"}),
              RespArray(ElementsAre(                            //
                  RespArray(ElementsAre(IntArg(0),              //
                                        IntArg(10'000),         //
                                        RespArray(ElementsAre(  //
                                            "10.0.0.1",         //
                                            IntArg(7'000),      //
                                            "abcd1234")),       //
                                        RespArray(ElementsAre(  //
                                            "10.0.0.10",        //
                                            IntArg(8'000),      //
                                            "wxyz")))),         //
                  RespArray(ElementsAre(IntArg(10'001),         //
                                        IntArg(16'383),         //
                                        RespArray(ElementsAre(  //
                                            "10.0.0.2",         //
                                            IntArg(7'001),      //
                                            "efgh7890")),       //
                                        RespArray(ElementsAre(  //
                                            "10.0.0.11",        //
                                            IntArg(8'001),      //
                                            "qwerty")))))));

  EXPECT_THAT(Run({"cluster", "nodes"}),
              "abcd1234 10.0.0.1:7000@7000 master - 0 0 0 disconnected 0-10000\n"
              "wxyz 10.0.0.10:8000@8000 slave abcd1234 0 0 0 connected\n"
              "efgh7890 10.0.0.2:7001@7001 master - 0 0 0 connected 10001-16383\n"
              "qwerty 10.0.0.11:8001@8001 slave efgh7890 0 0 0 connected\n"
              "qwerty1 10.0.0.12:8002@8002 slave efgh7890 0 0 0 connected\n"
              "qwerty2 10.0.0.13:8003@8003 slave efgh7890 0 0 0 disconnected\n");

  absl::InsecureBitGen eng;
  while (true) {
    string random_key = GetRandomHex(eng, 40);
    SlotId slot = KeySlot(random_key);
    if (slot > 10'000) {
      continue;
    }

    EXPECT_THAT(Run({"get", random_key}).GetString(),
                testing::MatchesRegex(R"(MOVED [0-9]+ 10.0.0.1:7000)"));
    break;
  }

  while (true) {
    string random_key = GetRandomHex(eng, 40);
    SlotId slot = KeySlot(random_key);
    if (slot <= 10'000) {
      continue;
    }

    EXPECT_THAT(Run({"get", random_key}).GetString(),
                testing::MatchesRegex(R"(MOVED [0-9]+ 10.0.0.2:7001)"));
    break;
  }
}

TEST_F(ClusterFamilyTest, ClusterGetSlotInfoInvalid) {
  constexpr string_view kErr = "ERR syntax error";
  EXPECT_THAT(RunPrivileged({"dflycluster", "getslotinfo"}), ErrArg(kErr));
  EXPECT_THAT(RunPrivileged({"dflycluster", "getslotinfo", "s"}), ErrArg(kErr));
  EXPECT_THAT(RunPrivileged({"dflycluster", "getslotinfo", "slots"}), ErrArg(kErr));
}

TEST_F(ClusterFamilyTest, ClusterGetSlotInfo) {
  ConfigSingleNodeCluster(GetMyId());

  constexpr string_view kKey = "some-key";
  const SlotId slot = KeySlot(kKey);
  EXPECT_NE(slot, 0) << "We need to choose another key";

  const string value(1'000, '#');  // Long string - to use heap
  EXPECT_EQ(Run({"SET", kKey, value}), "OK");

  EXPECT_THAT(
      RunPrivileged({"dflycluster", "getslotinfo", "slots", "0", absl::StrCat(slot)}),
      RespArray(ElementsAre(
          RespArray(ElementsAre(IntArg(0), "key_count", IntArg(0), "total_reads", IntArg(0),
                                "total_writes", IntArg(0), "memory_bytes", IntArg(0))),
          RespArray(ElementsAre(IntArg(slot), "key_count", IntArg(1), "total_reads", IntArg(0),
                                "total_writes", IntArg(1), "memory_bytes", Not(IntArg(0)))))));

  EXPECT_EQ(Run({"GET", kKey}), value);

  EXPECT_THAT(
      RunPrivileged({"dflycluster", "getslotinfo", "slots", "0", absl::StrCat(slot)}),
      RespArray(ElementsAre(
          RespArray(ElementsAre(IntArg(0), "key_count", IntArg(0), "total_reads", IntArg(0),
                                "total_writes", IntArg(0), "memory_bytes", IntArg(0))),
          RespArray(ElementsAre(IntArg(slot), "key_count", IntArg(1), "total_reads", IntArg(1),
                                "total_writes", IntArg(1), "memory_bytes", Not(IntArg(0)))))));

  EXPECT_EQ(Run({"SET", kKey, "value2"}), "OK");

  EXPECT_THAT(
      RunPrivileged({"dflycluster", "getslotinfo", "slots", "0", absl::StrCat(slot)}),
      RespArray(ElementsAre(
          RespArray(ElementsAre(IntArg(0), "key_count", IntArg(0), "total_reads", IntArg(0),
                                "total_writes", IntArg(0), "memory_bytes", IntArg(0))),
          RespArray(ElementsAre(IntArg(slot), "key_count", IntArg(1), "total_reads", IntArg(1),
                                "total_writes", IntArg(2), "memory_bytes", IntArg(36))))));
}

TEST_F(ClusterFamilyTest, ClusterGetSlotInfoRanges) {
  ConfigSingleNodeCluster(GetMyId());

  // Test basic range syntax: 0-2 should return 3 slots
  auto result = RunPrivileged({"dflycluster", "getslotinfo", "slots", "0-2"});
  ASSERT_EQ(result.GetVec().size(), 3u);
  EXPECT_THAT(result.GetVec()[0], RespArray(ElementsAre(IntArg(0), _, _, _, _, _, _, _, _)));
  EXPECT_THAT(result.GetVec()[1], RespArray(ElementsAre(IntArg(1), _, _, _, _, _, _, _, _)));
  EXPECT_THAT(result.GetVec()[2], RespArray(ElementsAre(IntArg(2), _, _, _, _, _, _, _, _)));

  // Test mixed syntax: range + individual slots
  result = RunPrivileged({"dflycluster", "getslotinfo", "slots", "0-1", "5", "10-11"});
  ASSERT_EQ(result.GetVec().size(), 5u);
  EXPECT_THAT(result.GetVec()[0], RespArray(ElementsAre(IntArg(0), _, _, _, _, _, _, _, _)));
  EXPECT_THAT(result.GetVec()[1], RespArray(ElementsAre(IntArg(1), _, _, _, _, _, _, _, _)));
  EXPECT_THAT(result.GetVec()[2], RespArray(ElementsAre(IntArg(5), _, _, _, _, _, _, _, _)));
  EXPECT_THAT(result.GetVec()[3], RespArray(ElementsAre(IntArg(10), _, _, _, _, _, _, _, _)));
  EXPECT_THAT(result.GetVec()[4], RespArray(ElementsAre(IntArg(11), _, _, _, _, _, _, _, _)));

  // Test reversed range (5-2 should be treated as 2-5)
  result = RunPrivileged({"dflycluster", "getslotinfo", "slots", "5-2"});
  ASSERT_EQ(result.GetVec().size(), 4u);
  EXPECT_THAT(result.GetVec()[0], RespArray(ElementsAre(IntArg(2), _, _, _, _, _, _, _, _)));
  EXPECT_THAT(result.GetVec()[1], RespArray(ElementsAre(IntArg(3), _, _, _, _, _, _, _, _)));
  EXPECT_THAT(result.GetVec()[2], RespArray(ElementsAre(IntArg(4), _, _, _, _, _, _, _, _)));
  EXPECT_THAT(result.GetVec()[3], RespArray(ElementsAre(IntArg(5), _, _, _, _, _, _, _, _)));

  // Test invalid slot id in range
  EXPECT_THAT(RunPrivileged({"dflycluster", "getslotinfo", "slots", "0-20000"}),
              ErrArg("Invalid slot id"));

  // Test invalid range format
  EXPECT_THAT(RunPrivileged({"dflycluster", "getslotinfo", "slots", "abc-def"}),
              ErrArg("Invalid slot range format"));

  // Edge cases with dashes
  EXPECT_THAT(RunPrivileged({"dflycluster", "getslotinfo", "slots", "-1"}),
              ErrArg("value is not an integer or out of range"));
  EXPECT_THAT(RunPrivileged({"dflycluster", "getslotinfo", "slots", "1-"}),
              ErrArg("Invalid slot range format"));
  EXPECT_THAT(RunPrivileged({"dflycluster", "getslotinfo", "slots", "1--2"}),
              ErrArg("Invalid slot range format"));
  EXPECT_THAT(RunPrivileged({"dflycluster", "getslotinfo", "slots", "1-2-3"}),
              ErrArg("Invalid slot range format"));
  EXPECT_THAT(RunPrivileged({"dflycluster", "getslotinfo", "slots", "1---2"}),
              ErrArg("Invalid slot range format"));
}

TEST_F(ClusterFamilyTest, ClusterSlotsPopulate) {
  ConfigSingleNodeCluster(GetMyId());

  Run({"debug", "populate", "10000", "key", "4", "SLOTS", "0", "1000"});

  for (int i = 0; i <= 1'000; ++i) {
    EXPECT_THAT(RunPrivileged({"dflycluster", "getslotinfo", "slots", absl::StrCat(i)}),
                RespElementsAre(RespArray(
                    ElementsAre(IntArg(i), "key_count", Not(IntArg(0)), _, _, _, _, _, _))));
  }

  for (int i = 1'001; i <= 16'383; ++i) {
    EXPECT_THAT(RunPrivileged({"dflycluster", "getslotinfo", "slots", absl::StrCat(i)}),
                RespElementsAre(
                    RespArray(ElementsAre(IntArg(i), "key_count", IntArg(0), _, _, _, _, _, _))));
  }
}

TEST_F(ClusterFamilyTest, ClusterEvalCrossslot) {
  ConfigSingleNodeCluster(GetMyId());

  auto res = Run({"EVAL", "return redis.call('MSET', 'x1', 'x1', 'x2', 'x2', 'x3', 'x3');", "3",
                  "x1", "x2", "x3"});

  EXPECT_THAT(res, ErrArg("CROSSSLOT"));

  auto sha =
      Run({"SCRPIT", "LOAD", "return redis.call('MSET', 'x1', 'x1', 'x2', 'x2', 'x3', 'x3');", "3",
           "x1", "x2", "x3"});

  EXPECT_THAT(Run({"EVALSHA", sha.GetString(), "3", "x1", "x2", "x3"}), ErrArg("CROSSSLOT"));
}

TEST_F(ClusterFamilyTest, ClusterMultiExec) {
  ConfigSingleNodeCluster(GetMyId());

  Run({"MULTI"});
  Run({"SET", "X1", "X1"});
  Run({"SET", "X2", "X2"});
  Run({"SET", "X3", "X3"});

  EXPECT_THAT(Run({"EXEC"}), ErrArg("CROSSSLOT"));
}

TEST_F(ClusterFamilyTest, ClusterConfigDeleteSlots) {
  ConfigSingleNodeCluster(GetMyId());

  Run({"debug", "populate", "100000"});

  EXPECT_THAT(
      RunPrivileged({"dflycluster", "getslotinfo", "slots", "1", "2"}),
      RespArray(ElementsAre(
          RespArray(ElementsAre(IntArg(1), "key_count", Not(IntArg(0)), "total_reads", IntArg(0),
                                "total_writes", Not(IntArg(0)), "memory_bytes", IntArg(108))),
          RespArray(ElementsAre(IntArg(2), "key_count", Not(IntArg(0)), "total_reads", IntArg(0),
                                "total_writes", Not(IntArg(0)), "memory_bytes", IntArg(360))))));

  ConfigSingleNodeCluster("abc");

  ExpectConditionWithinTimeout([&]() { return CheckedInt({"dbsize"}) == 0; });

  EXPECT_THAT(
      RunPrivileged({"dflycluster", "getslotinfo", "slots", "1", "2"}),
      RespArray(ElementsAre(
          RespArray(ElementsAre(IntArg(1), "key_count", IntArg(0), "total_reads", IntArg(0),
                                "total_writes", Not(IntArg(0)), "memory_bytes", IntArg(0))),
          RespArray(ElementsAre(IntArg(2), "key_count", IntArg(0), "total_reads", IntArg(0),
                                "total_writes", Not(IntArg(0)), "memory_bytes", IntArg(0))))));
}

// SlotStats::memory_bytes tracks resident RAM.
class ClusterMemoryTest : public ClusterFamilyTest {
 protected:
  ClusterMemoryTest() {
    num_threads_ = 1;
  }

  struct SlotInfo {
    int64_t key_count = 0;
    int64_t total_reads = 0;
    int64_t total_writes = 0;
    int64_t memory_bytes = 0;
    int64_t tiered_bytes = 0;  // reported only when non-zero
  };

  SlotInfo GetSlotInfo(SlotId slot) {
    auto resp = RunPrivileged({"dflycluster", "getslotinfo", "slots", absl::StrCat(slot)});
    const auto& row = resp.GetVec()[0].GetVec();
    SlotInfo info{*row[2].GetInt(), *row[4].GetInt(), *row[6].GetInt(), *row[8].GetInt()};
    if (row.size() >= 11) {
      EXPECT_EQ(row[9], "tiered_bytes");
      info.tiered_bytes = *row[10].GetInt();
    }
    return info;
  }

  // GETSLOTINFO adds a fixed per-key table-space term.
  int64_t RawSlotMemory(SlotId slot) {
    SlotInfo info = GetSlotInfo(slot);
    return info.memory_bytes - info.key_count * int64_t(sizeof(CompactObj)) * 2;
  }

  void ExpectSlotEmpty(SlotId slot) {
    SlotInfo info = GetSlotInfo(slot);
    EXPECT_EQ(info.key_count, 0);
    EXPECT_EQ(info.memory_bytes, 0);
    EXPECT_EQ(info.tiered_bytes, 0);
  }

  // The slot ledgers must mirror the db-wide ones.
  void ExpectSlotMirrorsDb(SlotId slot) {
    auto db_stats = GetMetrics().db_stats[0];
    EXPECT_EQ(RawSlotMemory(slot), int64_t(db_stats.obj_memory_usage));
    EXPECT_EQ(GetSlotInfo(slot).tiered_bytes, int64_t(db_stats.tiered_used_bytes));
  }
};

#ifdef WITH_TIERING

class ClusterTieredTest : public ClusterMemoryTest {
 protected:
  void SetUp() override {
    if (absl::GetFlag(FLAGS_force_epoll)) {
      GTEST_SKIP() << "Tiered storage requires io_uring";
    }
    flag_saver_.emplace();
    SetTestFlag("tiered_prefix", "/tmp/cluster_tiered_test");
    SetTestFlag("tiered_offload_threshold", "1.0");
    SetTestFlag("tiered_min_value_size", "64");
    SetTestFlag("tiered_experimental_cooling", "false");
    ClusterMemoryTest::SetUp();
    ConfigSingleNodeCluster(GetMyId());
  }

  void TearDown() override {
    if (service_)
      ClusterMemoryTest::TearDown();
    flag_saver_.reset();
  }

  void WaitForOffload(size_t entries) {
    ExpectConditionWithinTimeout(
        [this, entries] { return GetMetrics().db_stats[0].tiered_entries == entries; });
  }

  // The background offloader keeps moving bytes between the counters; park it and drain
  // in-flight stashes so that multi-command assertions see a stable state.
  void StopOffloading() {
    SetTestFlag("tiered_offload_threshold", "0.0");
    pp_->at(0)->AwaitBrief([] { EngineShard::tlocal()->tiered_storage()->UpdateFromFlags(); });
    ExpectConditionWithinTimeout(
        [this] { return GetMetrics().tiered_stats.pending_stash_cnt == 0u; });
  }

  std::optional<absl::FlagSaver> flag_saver_;
};

// memory_bytes tracks resident RAM only: offloading removes the value's bytes from it, and it
// must return to zero once the slot is empty.
TEST_F(ClusterTieredTest, SlotCountersFollowOffloadAndDelete) {
  const string kKey = "tiered-del";
  const SlotId slot = KeySlot(kKey);

  EXPECT_EQ(Run({"SET", kKey, string(4096, 'x')}), "OK");
  WaitForOffload(1);

  EXPECT_EQ(RawSlotMemory(slot), 0);
  EXPECT_GT(GetSlotInfo(slot).tiered_bytes, 0);

  EXPECT_EQ(CheckedInt({"DEL", kKey}), 1);
  ExpectSlotEmpty(slot);
}

// The single key lives in one slot, so the slot counters must mirror the db-wide ones exactly.
TEST_F(ClusterTieredTest, SlotCountersFollowExternalOverwrite) {
  const string kKey = "tiered-set";
  const SlotId slot = KeySlot(kKey);

  EXPECT_EQ(Run({"SET", kKey, string(4096, 'x')}), "OK");
  WaitForOffload(1);

  // Overwriting an offloaded value releases its disk extent; the new value may get offloaded
  // again right away, so compare against the db-wide counters instead of fixed values.
  EXPECT_EQ(Run({"SET", kKey, string(4096, 'y')}), "OK");
  StopOffloading();

  ExpectSlotMirrorsDb(slot);
  // The old extent was released: at most one value's worth of disk is held.
  EXPECT_LE(GetMetrics().db_stats[0].tiered_used_bytes, 3584u);
}

TEST_F(ClusterTieredTest, SlotCountersReleasedOnFlushSlots) {
  const string kKey = "tiered-flush";
  const SlotId slot = KeySlot(kKey);

  EXPECT_EQ(Run({"SET", kKey, string(4096, 'x')}), "OK");
  WaitForOffload(1);
  ASSERT_GT(GetSlotInfo(slot).tiered_bytes, 0);

  EXPECT_EQ(RunPrivileged({"dflycluster", "flushslots", absl::StrCat(slot), absl::StrCat(slot)}),
            "OK");
  ExpectConditionWithinTimeout([&]() { return GetSlotInfo(slot).key_count == 0; });

  ExpectSlotEmpty(slot);
}

TEST_F(ClusterTieredTest, SlotCountersReleasedOnExpiry) {
  const string kKey = "tiered-ttl";
  const SlotId slot = KeySlot(kKey);

  EXPECT_EQ(Run({"SET", kKey, string(4096, 'x')}), "OK");
  WaitForOffload(1);
  ASSERT_GT(GetSlotInfo(slot).tiered_bytes, 0);

  EXPECT_EQ(CheckedInt({"PEXPIRE", kKey, "10"}), 1);
  AdvanceTime(100);
  EXPECT_THAT(Run({"GET", kKey}), ArgType(RespExpr::NIL));
  ExpectSlotEmpty(slot);
}

// Uploads and re-offloads move bytes in and out of memory_bytes; it must mirror the db-wide
// counter at every step and never wrap around zero.
TEST_F(ClusterTieredTest, SlotCountersFollowUploadAndAppend) {
  const string kKey = "tiered-append";
  const SlotId slot = KeySlot(kKey);

  EXPECT_EQ(Run({"SET", kKey, string(4000, 'x')}), "OK");
  WaitForOffload(1);

  for (int i = 0; i < 6; ++i)
    Run({"APPEND", kKey, string(512, 'y')});
  StopOffloading();

  ExpectSlotMirrorsDb(slot);

  EXPECT_EQ(CheckedInt({"DEL", kKey}), 1);
  ExpectSlotEmpty(slot);
}

TEST_F(ClusterTieredTest, SlotCountersReleasedOnConfigSlotRemoval) {
  Run({"debug", "populate", "20", "key", "3000", "SLOTS", "1", "1"});
  WaitForOffload(20);
  ASSERT_GT(GetSlotInfo(1).tiered_bytes, 0);

  ConfigSingleNodeCluster("abc");
  ExpectConditionWithinTimeout([&]() { return CheckedInt({"dbsize"}) == 0; });

  ExpectSlotEmpty(1);
}

class ClusterTieredCoolingTest : public ClusterTieredTest {
 protected:
  void SetUp() override {
    ClusterTieredTest::SetUp();
    if (!service_)  // skipped
      return;
    SetTestFlag("tiered_experimental_cooling", "true");
    pp_->at(0)->AwaitBrief([] { EngineShard::tlocal()->tiered_storage()->UpdateFromFlags(); });
  }
};

// A cool value is invisible to the RAM ledger but still holds its disk extent, so it counts
// in tiered_bytes only.
TEST_F(ClusterTieredCoolingTest, CoolSlotCountersReleasedOnFlushSlots) {
  const string kKey = "cool-flush";
  const SlotId slot = KeySlot(kKey);

  EXPECT_EQ(Run({"SET", kKey, string(4096, 'x')}), "OK");
  WaitForOffload(1);

  ExpectSlotMirrorsDb(slot);
  EXPECT_EQ(RawSlotMemory(slot), 0);
  EXPECT_GT(GetSlotInfo(slot).tiered_bytes, 0);

  EXPECT_EQ(RunPrivileged({"dflycluster", "flushslots", absl::StrCat(slot), absl::StrCat(slot)}),
            "OK");
  ExpectConditionWithinTimeout([&]() { return GetSlotInfo(slot).key_count == 0; });

  ExpectSlotEmpty(slot);
  EXPECT_EQ(GetMetrics().db_stats[0].obj_memory_usage, 0u);
}

// Warming a cool value up returns its bytes to the RAM ledger and releases the disk extent.
TEST_F(ClusterTieredCoolingTest, CoolSlotCountersReleasedOnWarmup) {
  const string kKey = "cool-warm";
  const SlotId slot = KeySlot(kKey);

  EXPECT_EQ(Run({"SET", kKey, string(4096, 'x')}), "OK");
  WaitForOffload(1);
  ASSERT_GT(GetSlotInfo(slot).tiered_bytes, 0);
  StopOffloading();

  EXPECT_EQ(Run({"GET", kKey}), string(4096, 'x'));

  ExpectSlotMirrorsDb(slot);
  EXPECT_GT(RawSlotMemory(slot), 0);
  EXPECT_EQ(GetSlotInfo(slot).tiered_bytes, 0);
}

#endif  // WITH_TIERING

TEST_F(ClusterMemoryTest, SlotMemoryFollowsDefrag) {
  ConfigSingleNodeCluster(GetMyId());

  const SlotId slot = KeySlot("{tag}0");
  const int kKeys = 120;

  // Arrays grown element by element keep spare capacity that defrag drops, so the post-defrag
  // delta is non-zero only for a non-power-of-two element count.
  for (int i = 0; i < kKeys; ++i) {
    Run({"JSON.SET", absl::StrCat("{tag}", i), "$", "[]"});
    for (int j = 0; j < 40; ++j)
      Run({"JSON.ARRAPPEND", absl::StrCat("{tag}", i), "$", absl::StrCat(j)});
  }

  shard_set->pool()->AwaitFiberOnAll([](unsigned, util::ProactorBase*) {
    auto* shard = EngineShard::tlocal();
    if (!shard)
      return;
    for (int i = 0; i < 100; ++i) {
      PageUsage page_usage{CollectPageStats::NO, 0, CycleQuota::Unlimited()};
      page_usage.SetForceReallocate(true);
      shard->DoDefrag(&page_usage);
      if (shard->GetDefragCursor() == 0)
        break;
    }
  });

  ASSERT_GT(GetMetrics().shard_stats.defrag_realloc_total, 0u);

  // Every key shares one hashtag.
  ExpectSlotMirrorsDb(slot);

  for (int i = 0; i < kKeys; ++i)
    Run({"DEL", absl::StrCat("{tag}", i)});

  ExpectSlotEmpty(slot);
}

TEST_F(ClusterMemoryTest, SlotTrafficCountersSurviveFlush) {
  ConfigSingleNodeCluster(GetMyId());

  const string kKey = "traffic-key";
  const SlotId slot = KeySlot(kKey);

  EXPECT_EQ(Run({"SET", kKey, string(1000, '#')}), "OK");
  Run({"GET", kKey});

  SlotInfo before = GetSlotInfo(slot);
  ASSERT_GT(before.total_reads, 0);
  ASSERT_GT(before.total_writes, 0);

  Run({"FLUSHALL"});

  SlotInfo after = GetSlotInfo(slot);
  EXPECT_EQ(after.key_count, 0);
  EXPECT_EQ(after.memory_bytes, 0);
  EXPECT_EQ(after.total_reads, before.total_reads);
  EXPECT_EQ(after.total_writes, before.total_writes);
}

// Test issue #1302
TEST_F(ClusterFamilyTest, ClusterConfigDeleteSlotsNoCrashOnShutdown) {
  ConfigSingleNodeCluster(GetMyId());

  Run({"debug", "populate", "100000"});

  EXPECT_THAT(
      RunPrivileged({"dflycluster", "getslotinfo", "slots", "1", "2"}),
      RespArray(ElementsAre(
          RespArray(ElementsAre(IntArg(1), "key_count", Not(IntArg(0)), "total_reads", IntArg(0),
                                "total_writes", Not(IntArg(0)), "memory_bytes", IntArg(108))),
          RespArray(ElementsAre(IntArg(2), "key_count", Not(IntArg(0)), "total_reads", IntArg(0),
                                "total_writes", Not(IntArg(0)), "memory_bytes", IntArg(360))))));

  // After running the new config we start a fiber that removes all slots from current instance
  // we immediately shut down to test that we do not crash.
  ConfigSingleNodeCluster("abc");
}

TEST_F(ClusterFamilyTest, ClusterConfigDeleteSomeSlots) {
  string config_template = R"json(
      [
        {
          "slot_ranges": [
            {
              "start": 0,
              "end": $1
            }
          ],
          "master": {
            "id": "$0",
            "ip": "10.0.0.1",
            "port": 7000
          },
          "replicas": []
        },
        {
          "slot_ranges": [
            {
              "start": $2,
              "end": 16383
            }
          ],
          "master": {
            "id": "other",
            "ip": "10.0.0.2",
            "port": 7000
          },
          "replicas": []
        }
      ])json";

  string config = absl::Substitute(config_template, GetMyId(), "8000", "8001");

  EXPECT_EQ(RunPrivileged({"dflycluster", "config", config}), "OK");

  Run({"debug", "populate", "1", "key", "4", "SLOTS", "7999", "7999"});
  Run({"debug", "populate", "2", "key", "4", "SLOTS", "8000", "8000"});

  EXPECT_THAT(RunPrivileged({"dflycluster", "getslotinfo", "slots", "7999", "8000"}),
              RespArray(ElementsAre(
                  RespArray(ElementsAre(IntArg(7999), "key_count", IntArg(1), _, _, _, _, _, _)),
                  RespArray(ElementsAre(IntArg(8000), "key_count", IntArg(2), _, _, _, _, _, _)))));
  EXPECT_THAT(Run({"dbsize"}), IntArg(3));

  // Move ownership over 8000 to other master
  config = absl::Substitute(config_template, GetMyId(), "7999", "8000");
  EXPECT_EQ(RunPrivileged({"dflycluster", "config", config}), "OK");

  // Verify that keys for slot 8000 were deleted, while key for slot 7999 was kept
  ExpectConditionWithinTimeout([&]() { return CheckedInt({"dbsize"}) == 1; });

  EXPECT_THAT(RunPrivileged({"dflycluster", "getslotinfo", "slots", "7999", "8000"}),
              RespArray(ElementsAre(
                  RespArray(ElementsAre(IntArg(7999), "key_count", IntArg(1), _, _, _, _, _, _)),
                  RespArray(ElementsAre(IntArg(8000), "key_count", IntArg(0), _, _, _, _, _, _)))));
}

TEST_F(ClusterFamilyTest, ClusterModeSelectNotAllowed) {
  EXPECT_THAT(Run({"select", "1"}), ErrArg("SELECT is not allowed in cluster mode"));
  EXPECT_EQ(Run({"select", "0"}), "OK");
}

TEST_F(ClusterFamilyTest, ClusterModePubSubNotAllowed) {
  EXPECT_THAT(Run({"PUBLISH", "ch", "message"}),
              ErrArg("PUBLISH is not supported in cluster mode yet"));
  EXPECT_THAT(Run({"SUBSCRIBE", "ch"}), ErrArg("SUBSCRIBE is not supported in cluster mode yet"));
  EXPECT_THAT(Run({"UNSUBSCRIBE", "ch"}),
              ErrArg("UNSUBSCRIBE is not supported in cluster mode yet"));
  EXPECT_THAT(Run({"PSUBSCRIBE", "ch?"}),
              ErrArg("PSUBSCRIBE is not supported in cluster mode yet"));
  EXPECT_THAT(Run({"PUNSUBSCRIBE", "ch?"}),
              ErrArg("PUNSUBSCRIBE is not supported in cluster mode yet"));
}

// SSUBSCRIBE and SPUBLISH work in cluster mode
TEST_F(ClusterFamilyTest, ClusterModePubSub) {
  single_response_ = false;
  ConfigSingleNodeCluster(GetMyId());

  // Ssubscribe works as expected
  auto resp = pp_->at(1)->Await([&] { return Run({"SSUBSCRIBE", "cluster-channel"}); });
  EXPECT_THAT(resp, RespElementsAre("ssubscribe", "cluster-channel", IntArg(1)));

  // Send-receive a single message
  resp = pp_->at(0)->Await([&] {
    return Run({"SPUBLISH", "cluster-channel", "a simple message"});
  });
  EXPECT_THAT(resp, IntArg(1));

  pp_->AwaitFiberOnAll([](util::ProactorBase* pb) {});

  ASSERT_EQ(1, SubscriberMessagesLen("IO1"));
  const auto& msg = GetPublishedMessage("IO1", 0);
  EXPECT_TRUE(msg.is_sharded);
  EXPECT_EQ("cluster-channel", msg.channel);
  EXPECT_EQ("a simple message", msg.message);

  // Sunsubscribe
  resp = pp_->at(1)->Await([&] { return Run({"SUNSUBSCRIBE", "cluster-channel"}); });
  EXPECT_THAT(resp, RespElementsAre("sunsubscribe", "cluster-channel", IntArg(0)));
}

TEST_F(ClusterFamilyTest, ClusterFirstConfigCallDropsEntriesNotOwnedByNode) {
  InitWithDbFilename();

  Run({"debug", "populate", "50000"});

  EXPECT_EQ(Run({"save", "df"}), "OK");

  auto save_info = service_->server_family().GetLastSaveInfo();
  EXPECT_EQ(Run({"dfly", "load", save_info.file_name}), "OK");
  EXPECT_EQ(CheckedInt({"dbsize"}), 50000);

  ConfigSingleNodeCluster("abcd1234");

  // Make sure `dbsize` all slots were removed
  ExpectConditionWithinTimeout([&]() { return CheckedInt({"dbsize"}) == 0; });
}

TEST_F(ClusterFamilyTest, SnapshotBiggerThanMaxMemory) {
  InitWithDbFilename();
  ConfigSingleNodeCluster(GetMyId());

  Run({"debug", "populate", "50000"});
  EXPECT_EQ(Run({"save", "df"}), "OK");

  max_memory_limit = 10000;
  auto save_info = service_->server_family().GetLastSaveInfo();
  EXPECT_EQ(Run({"dfly", "load", save_info.file_name}), "OK");
}

TEST_F(ClusterFamilyTest, Keyslot) {
  // Example from Redis' command reference: https://redis.io/commands/cluster-keyslot/
  EXPECT_THAT(Run({"cluster", "keyslot", "somekey"}), IntArg(11'058));

  // Test hash tags
  EXPECT_THAT(Run({"cluster", "keyslot", "prefix{somekey}suffix"}), IntArg(11'058));

  EXPECT_EQ(CheckedInt({"cluster", "keyslot", "abc{def}ghi"}),
            CheckedInt({"cluster", "keyslot", "123{def}456"}));
}

TEST_F(ClusterFamilyTest, FlushSlots) {
  EXPECT_EQ(Run({"debug", "populate", "100", "key", "4", "slots", "0", "1"}), "OK");

  EXPECT_THAT(RunPrivileged({"dflycluster", "getslotinfo", "slots", "0", "1"}),
              RespArray(ElementsAre(
                  RespArray(ElementsAre(IntArg(0), "key_count", Not(IntArg(0)), "total_reads", _,
                                        "total_writes", _, "memory_bytes", _)),
                  RespArray(ElementsAre(IntArg(1), "key_count", Not(IntArg(0)), "total_reads", _,
                                        "total_writes", _, "memory_bytes", _)))));

  ExpectConditionWithinTimeout([&]() {
    return RunPrivileged({"dflycluster", "flushslots", "0", "0"}) == "OK";
  });
  util::ThisFiber::SleepFor(10ms);
  EXPECT_THAT(RunPrivileged({"dflycluster", "getslotinfo", "slots", "0", "1"}),
              RespArray(ElementsAre(
                  RespArray(ElementsAre(IntArg(0), "key_count", IntArg(0), "total_reads", _,
                                        "total_writes", _, "memory_bytes", _)),
                  RespArray(ElementsAre(IntArg(1), "key_count", Not(IntArg(0)), "total_reads", _,
                                        "total_writes", _, "memory_bytes", _)))));

  EXPECT_EQ(RunPrivileged({"dflycluster", "flushslots", "0", "1"}), "OK");
  util::ThisFiber::SleepFor(10ms);
  EXPECT_THAT(
      RunPrivileged({"dflycluster", "getslotinfo", "slots", "0", "1"}),
      RespArray(ElementsAre(RespArray(ElementsAre(IntArg(0), "key_count", IntArg(0), "total_reads",
                                                  _, "total_writes", _, "memory_bytes", _)),
                            RespArray(ElementsAre(IntArg(1), "key_count", IntArg(0), "total_reads",
                                                  _, "total_writes", _, "memory_bytes", _)))));
}

TEST_F(ClusterFamilyTest, FlushSlotsOutOfBounds) {
  EXPECT_THAT(RunPrivileged({"dflycluster", "flushslots", "0", "16384"}),
              ErrArg("value is not an integer or out of range"));
  EXPECT_THAT(RunPrivileged({"dflycluster", "flushslots", "16384", "16384"}),
              ErrArg("value is not an integer or out of range"));
  EXPECT_THAT(RunPrivileged({"dflycluster", "flushslots", "100", "50"}),
              ErrArg("Invalid slot range"));
}

TEST_F(ClusterFamilyTest, FlushSlotsAndImmediatelySetValue) {
  for (int count : {1, 10, 100, 1000, 10000, 100000}) {
    ConfigSingleNodeCluster(GetMyId());

    EXPECT_EQ(Run({"debug", "populate", absl::StrCat(count), "key", "4"}), "OK");
    EXPECT_EQ(Run({"get", "key:0"}), "xxxx");

    EXPECT_THAT(Run({"cluster", "keyslot", "key:0"}), IntArg(2592));
    EXPECT_THAT(Run({"dbsize"}), IntArg(count));
    auto slot_size_response = Run({"dflycluster", "getslotinfo", "slots", "2592"});
    EXPECT_THAT(slot_size_response,
                RespElementsAre(RespArray(ElementsAre(_, "key_count", _, "total_reads", _,
                                                      "total_writes", _, "memory_bytes", _))));
    auto slot_size = slot_size_response.GetVec()[0].GetVec()[2].GetInt();
    EXPECT_TRUE(slot_size.has_value());

    EXPECT_EQ(Run({"dflycluster", "flushslots", "2592", "2592"}), "OK");
    // key:0 should have been removed, so APPEND will end up with key:0 == ZZZZ
    EXPECT_THAT(Run({"append", "key:0", "ZZZZ"}), IntArg(4));
    EXPECT_EQ(Run({"get", "key:0"}), "ZZZZ");
    // db size should be count - (size of slot 2592) + 1, where 1 is for 'key:0'
    ExpectConditionWithinTimeout(
        [&]() { return CheckedInt({"dbsize"}) == (count - *slot_size + 1); });

    ResetService();
  }
}

// Regression: FlushSlots launches an async fiber. Entries inserted after FlushSlots returns
// but before the fiber runs must survive, because they were created after the flush was
// initiated. The bug was that RegisterOnChange (which captures the version threshold) ran
// inside the detached fiber instead of synchronously in FlushSlots, so the version threshold
// was captured too late and freshly-inserted entries appeared "old" to the flush.
TEST_F(ClusterFamilyTest, FlushSlotsDoesNotDeleteEntriesInsertedAfterFlush) {
  ConfigSingleNodeCluster(GetMyId());

  // Run on a shard proactor to control fiber scheduling precisely.
  pp_->at(0)->Await([&] {
    auto* es = EngineShard::tlocal();
    ASSERT_NE(es, nullptr);

    auto& db_slice = namespaces->GetDefaultNamespace().GetDbSlice(es->shard_id());

    // Step 0: Insert pre-existing entries that must be deleted by the flush.
    // This ensures we can verify the flush fiber actually ran (not just that it was scheduled).
    DbContext cntx{&namespaces->GetDefaultNamespace(), 0, GetCurrentTimeMs()};
    for (int i = 0; i < 10; i++) {
      string key = absl::StrCat("pre:", i);
      PrimeValue val;
      val.SetString("old");
      auto res = db_slice.AddOrUpdate(cntx, key, std::move(val), 0);
      CHECK(res.ok());
    }
    EXPECT_EQ(db_slice.DbSize(0), 10u);

    // Step 1: FlushSlots creates a detached fiber. We do NOT yield, so the fiber
    // cannot run yet. Acquire the shard lock because RegisterOnChange requires it (#7153).
    cluster::SlotRanges ranges({{0, 16383}});
    es->shard_lock()->Acquire(IntentLock::EXCLUSIVE);
    db_slice.FlushSlots(ranges);
    es->shard_lock()->Release(IntentLock::EXCLUSIVE);

    // Step 2: Insert entries WITHOUT yielding — the flush fiber still has not executed.
    // Each insert calls NextVersion(), advancing the global counter.
    for (int i = 0; i < 50; i++) {
      string key = absl::StrCat("key:", i);
      PrimeValue val;
      val.SetString("val");
      auto res = db_slice.AddOrUpdate(cntx, key, std::move(val), 0);
      CHECK(res.ok());
    }
    EXPECT_EQ(db_slice.DbSize(0), 60u);

    // Step 3: Yield — the detached flush fiber gets scheduled and runs.
    // BUG: the fiber calls RegisterOnChange NOW, capturing a version AFTER the 50 inserts.
    //   All 50 entries have version < next_version → deleted.
    // FIX: version was captured in FlushSlots (step 1), so entries have version > next_version
    //   → survive.
    util::ThisFiber::SleepFor(50ms);

    // Step 4: Verify pre-existing entries were flushed (proving the fiber ran) and
    // post-flush entries survived.
    EXPECT_EQ(db_slice.DbSize(0), 50u);
  });
}

TEST_F(ClusterFamilyTest, MoveNotAllowedInClusterMode) {
  ConfigSingleNodeCluster(GetMyId());

  EXPECT_EQ(Run({"set", "key", "val"}), "OK");
  EXPECT_THAT(Run({"move", "key", "1"}), ErrArg("MOVE is not allowed in cluster mode"));
  EXPECT_EQ(Run({"get", "key"}), "val");
}

TEST_F(ClusterFamilyTest, AclSelectDbNotAllowedInClusterMode) {
  EXPECT_THAT(Run({"acl", "setuser", "u1", "on", ">pw", "~*", "+@all", "$1"}),
              ErrArg("not allowed in cluster mode"));
  EXPECT_EQ(Run({"acl", "setuser", "u2", "on", ">pw", "~*", "+@all", "$0"}), "OK");
  EXPECT_EQ(Run({"acl", "setuser", "u3", "on", ">pw", "~*", "+@all", "$ALL"}), "OK");
}

// The flush's on_change fires for every db index but must only touch db 0; the bug charged
// db 0's table for a db 1 deletion (FATAL underflow in debug).
TEST_F(ClusterFamilyTest, FlushSlotsOnChangeIgnoresNonDefaultDb) {
  ConfigSingleNodeCluster(GetMyId());

  pp_->at(0)->Await([&] {
    auto* es = EngineShard::tlocal();
    ASSERT_NE(es, nullptr);
    auto& db_slice = namespaces->GetDefaultNamespace().GetDbSlice(es->shard_id());
    db_slice.ActivateDb(1);
    DbContext cntx{&namespaces->GetDefaultNamespace(), 1, GetCurrentTimeMs()};

    PrimeValue val;
    val.SetString(string(128, 'x'));
    CHECK(db_slice.AddOrUpdate(cntx, "key", std::move(val), 0).ok());

    cluster::SlotRanges ranges({{0, 16383}});
    es->shard_lock()->Acquire(IntentLock::EXCLUSIVE);
    db_slice.FlushSlots(ranges);
    es->shard_lock()->Release(IntentLock::EXCLUSIVE);

    // Overwrite without yielding: PreUpdateBlocking fires on_change with db_index=1.
    PrimeValue val2;
    val2.SetString(string(128, 'y'));
    CHECK(db_slice.AddOrUpdate(cntx, "key", std::move(val2), 0).ok());

    util::ThisFiber::SleepFor(50ms);  // let the flush fiber finish

    // db 1 is not covered by slot operations; the entry stays.
    EXPECT_EQ(db_slice.DbSize(1), 1u);
    EXPECT_EQ(db_slice.DbSize(0), 0u);
  });
}

TEST_F(ClusterFamilyTest, ClusterCrossSlot) {
  ConfigSingleNodeCluster(GetMyId());

  EXPECT_EQ(Run({"SET", "key", "value"}), "OK");
  EXPECT_EQ(Run({"GET", "key"}), "value");

  EXPECT_EQ(Run({"MSET", "key", "value2"}), "OK");
  EXPECT_THAT(Run({"MGET", "key"}), RespElementsAre("value2"));

  EXPECT_THAT(Run({"MSET", "key", "value", "key2", "value2"}), ErrArg("CROSSSLOT"));
  EXPECT_THAT(Run({"MGET", "key", "key2"}), ErrArg("CROSSSLOT"));
  EXPECT_THAT(Run({"ZINTERSTORE", "key", "2", "key1", "key2"}), ErrArg("CROSSSLOT"));

  EXPECT_EQ(Run({"MSET", "key{tag}", "value", "key2{tag}", "value2"}), "OK");
  EXPECT_THAT(Run({"MGET", "key{tag}", "key2{tag}"}), RespArray(ElementsAre("value", "value2")));
}

class ClusterFamilyEmulatedTest : public ClusterFamilyTest {
 protected:
  void ConfigureClusterFlags() override {
    SetTestFlag("cluster_mode", "emulated");
    SetTestFlag("cluster_announce_ip", "fake-host");
    SetTestFlag("announce_port", "6379");
  }
};

// slots_stats is null outside real cluster mode; GetSlotStats must not crash.
TEST_F(ClusterFamilyEmulatedTest, GetSlotStatsWithoutClusterMode) {
  EXPECT_EQ(Run({"set", "key", "value"}), "OK");

  pp_->at(0)->Await([&] {
    auto* es = EngineShard::tlocal();
    ASSERT_NE(es, nullptr);
    auto& db_slice = namespaces->GetDefaultNamespace().GetDbSlice(es->shard_id());
    SlotStats stats = db_slice.GetSlotStats(0);
    EXPECT_EQ(stats.key_count, 0u);
  });
}

TEST_F(ClusterFamilyEmulatedTest, ClusterInfo) {
  string cluster_info = Run({"cluster", "info"}).GetString();
  EXPECT_THAT(cluster_info, HasSubstr("cluster_state:ok"));
  EXPECT_THAT(cluster_info, HasSubstr("cluster_slots_assigned:16384"));
  EXPECT_THAT(cluster_info, HasSubstr("cluster_slots_ok:16384"));
  EXPECT_THAT(cluster_info, HasSubstr("cluster_known_nodes:1"));
  EXPECT_THAT(cluster_info, HasSubstr("cluster_size:1"));
}

TEST_F(ClusterFamilyEmulatedTest, ClusterShardInfos) {
  EXPECT_THAT(
      Run({"cluster", "shards"}),
      RespElementsAre(RespArray(ElementsAre("slots",                                           //
                                            RespArray(ElementsAre(IntArg(0), IntArg(16383))),  //
                                            "nodes",                                           //
                                            RespArray(ElementsAre(                             //
                                                RespArray(ElementsAre(                         //
                                                    "id", GetMyId(),                           //
                                                    "endpoint", "fake-host",                   //
                                                    "ip", "fake-host",                         //
                                                    "port", IntArg(6379),                      //
                                                    "role", "master",                          //
                                                    "replication-offset", IntArg(0),           //
                                                    "health", "online"))))))));

  EXPECT_EQ(RunPrivileged({"config", "set", "cluster_announce_ip", "updated-host"}), "OK");
  EXPECT_EQ(RunPrivileged({"config", "set", "announce_port", "6380"}), "OK");

  EXPECT_THAT(
      Run({"cluster", "shards"}),
      RespElementsAre(RespArray(ElementsAre("slots",                                           //
                                            RespArray(ElementsAre(IntArg(0), IntArg(16383))),  //
                                            "nodes",                                           //
                                            RespArray(ElementsAre(                             //
                                                RespArray(ElementsAre(                         //
                                                    "id", GetMyId(),                           //
                                                    "endpoint", "updated-host",                //
                                                    "ip", "updated-host",                      //
                                                    "port", IntArg(6380),                      //
                                                    "role", "master",                          //
                                                    "replication-offset", IntArg(0),           //
                                                    "health", "online"))))))));
}

TEST_F(ClusterFamilyEmulatedTest, ClusterSlots) {
  EXPECT_THAT(Run({"cluster", "slots"}),
              RespElementsAre(RespArray(ElementsAre(IntArg(0),              //
                                                    IntArg(16383),          //
                                                    RespArray(ElementsAre(  //
                                                        "fake-host",        //
                                                        IntArg(6379),       //
                                                        GetMyId()))))));
}

TEST_F(ClusterFamilyEmulatedTest, ClusterNodes) {
  auto res = Run({"cluster", "nodes"});
  EXPECT_THAT(res, GetMyId() + " fake-host:6379@6379 myself,master - 0 0 0 connected 0-16383\n");
}

TEST_F(ClusterFamilyEmulatedTest, ForbidenCommands) {
  auto res = Run({"DFLYCLUSTER", "GETSLOTINFO", "SLOTS", "1"});
  EXPECT_THAT(res, ErrArg("Cluster is disabled. Use --cluster_mode=yes to enable."));
}

}  // namespace
}  // namespace dfly::cluster
