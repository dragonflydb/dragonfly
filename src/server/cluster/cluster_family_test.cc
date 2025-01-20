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
#include "base/gtest.h"
#include "base/logging.h"
#include "facade/facade_test.h"
#include "server/test_utils.h"

namespace dfly::cluster {
namespace {

using namespace std;
using namespace testing;

class ClusterFamilyTest : public BaseFamilyTest {
 public:
  ClusterFamilyTest() {
    SetTestFlag("cluster_mode", "yes");
  }

 protected:
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
            "port": 7000
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

  EXPECT_THAT(Run({"cluster", "shards"}),
              RespArray(ElementsAre("slots",                                            //
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
                                            "health", "online")))))));

  EXPECT_THAT(Run({"get", "x"}).GetString(),
              testing::MatchesRegex(R"(MOVED [0-9]+ 10.0.0.1:7000)"));

  EXPECT_THAT(Run({"cluster", "slots"}),
              RespArray(ElementsAre(IntArg(0),              //
                                    IntArg(16'383),         //
                                    RespArray(ElementsAre(  //
                                        "10.0.0.1",         //
                                        IntArg(7'000),      //
                                        "abcd1234")))));

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
            "port": 7000
          },
          "replicas": [
            {
              "id": "wxyz",
              "ip": "10.0.0.10",
              "port": 8000
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

  EXPECT_THAT(Run({"cluster", "shards"}),
              RespArray(ElementsAre("slots",                                            //
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
                                            "health", "online")))))));

  EXPECT_THAT(Run({"cluster", "slots"}),
              RespArray(ElementsAre(IntArg(0),              //
                                    IntArg(16'383),         //
                                    RespArray(ElementsAre(  //
                                        "10.0.0.1",         //
                                        IntArg(7'000),      //
                                        "abcd1234")),       //
                                    RespArray(ElementsAre(  //
                                        "10.0.0.10",        //
                                        IntArg(8'000),      //
                                        "wxyz")))));

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
            "port": 7000
          },
          "replicas": [
            {
              "id": "wxyz",
              "ip": "10.0.0.10",
              "port": 8000
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
            "port": 7001
          },
          "replicas": [
            {
              "id": "qwerty",
              "ip": "10.0.0.11",
              "port": 8001
            }
          ]
        }
      ])json"}),
            "OK");

  string cluster_info = Run({"cluster", "info"}).GetString();
  EXPECT_THAT(cluster_info, HasSubstr("cluster_state:ok"));
  EXPECT_THAT(cluster_info, HasSubstr("cluster_slots_assigned:16384"));
  EXPECT_THAT(cluster_info, HasSubstr("cluster_slots_ok:16384"));
  EXPECT_THAT(cluster_info, HasSubstr("cluster_known_nodes:4"));
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
                                                "health", "online")),                            //
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
                                                "health", "online")))))))));

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
              "abcd1234 10.0.0.1:7000@7000 master - 0 0 0 connected 0-10000\n"
              "wxyz 10.0.0.10:8000@8000 slave abcd1234 0 0 0 connected\n"
              "efgh7890 10.0.0.2:7001@7001 master - 0 0 0 connected 10001-16383\n"
              "qwerty 10.0.0.11:8001@8001 slave efgh7890 0 0 0 connected\n");

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
                                "total_writes", IntArg(2), "memory_bytes", IntArg(0))))));
}

TEST_F(ClusterFamilyTest, ClusterSlotsPopulate) {
  ConfigSingleNodeCluster(GetMyId());

  Run({"debug", "populate", "10000", "key", "4", "SLOTS", "0", "1000"});

  for (int i = 0; i <= 1'000; ++i) {
    EXPECT_THAT(RunPrivileged({"dflycluster", "getslotinfo", "slots", absl::StrCat(i)}),
                RespArray(ElementsAre(IntArg(i), "key_count", Not(IntArg(0)), _, _, _, _, _, _)));
  }

  for (int i = 1'001; i <= 16'383; ++i) {
    EXPECT_THAT(RunPrivileged({"dflycluster", "getslotinfo", "slots", absl::StrCat(i)}),
                RespArray(ElementsAre(IntArg(i), "key_count", IntArg(0), _, _, _, _, _, _)));
  }
}

TEST_F(ClusterFamilyTest, ClusterConfigDeleteSlots) {
  ConfigSingleNodeCluster(GetMyId());

  Run({"debug", "populate", "100000"});

  EXPECT_THAT(
      RunPrivileged({"dflycluster", "getslotinfo", "slots", "1", "2"}),
      RespArray(ElementsAre(
          RespArray(ElementsAre(IntArg(1), "key_count", Not(IntArg(0)), "total_reads", IntArg(0),
                                "total_writes", Not(IntArg(0)), "memory_bytes", IntArg(0))),
          RespArray(ElementsAre(IntArg(2), "key_count", Not(IntArg(0)), "total_reads", IntArg(0),
                                "total_writes", Not(IntArg(0)), "memory_bytes", IntArg(0))))));

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

// Test issue #1302
TEST_F(ClusterFamilyTest, ClusterConfigDeleteSlotsNoCrashOnShutdown) {
  ConfigSingleNodeCluster(GetMyId());

  Run({"debug", "populate", "100000"});

  EXPECT_THAT(
      RunPrivileged({"dflycluster", "getslotinfo", "slots", "1", "2"}),
      RespArray(ElementsAre(
          RespArray(ElementsAre(IntArg(1), "key_count", Not(IntArg(0)), "total_reads", IntArg(0),
                                "total_writes", Not(IntArg(0)), "memory_bytes", IntArg(0))),
          RespArray(ElementsAre(IntArg(2), "key_count", Not(IntArg(0)), "total_reads", IntArg(0),
                                "total_writes", Not(IntArg(0)), "memory_bytes", IntArg(0))))));

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
  EXPECT_THAT(Run({"PUBSUB", "CHANNELS"}), ErrArg("PUBSUB is not supported in cluster mode yet"));
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

  EXPECT_THAT(RunPrivileged({"dflycluster", "getslotinfo", "slots", "0", "1"}),
              RespArray(ElementsAre(
                  RespArray(ElementsAre(IntArg(0), "key_count", IntArg(0), "total_reads", _,
                                        "total_writes", _, "memory_bytes", _)),
                  RespArray(ElementsAre(IntArg(1), "key_count", Not(IntArg(0)), "total_reads", _,
                                        "total_writes", _, "memory_bytes", _)))));

  EXPECT_EQ(RunPrivileged({"dflycluster", "flushslots", "0", "1"}), "OK");

  EXPECT_THAT(
      RunPrivileged({"dflycluster", "getslotinfo", "slots", "0", "1"}),
      RespArray(ElementsAre(RespArray(ElementsAre(IntArg(0), "key_count", IntArg(0), "total_reads",
                                                  _, "total_writes", _, "memory_bytes", _)),
                            RespArray(ElementsAre(IntArg(1), "key_count", IntArg(0), "total_reads",
                                                  _, "total_writes", _, "memory_bytes", _)))));
}

TEST_F(ClusterFamilyTest, FlushSlotsAndImmediatelySetValue) {
  for (int count : {1, 10, 100, 1000, 10000, 100000}) {
    ConfigSingleNodeCluster(GetMyId());

    EXPECT_EQ(Run({"debug", "populate", absl::StrCat(count), "key", "4"}), "OK");
    EXPECT_EQ(Run({"get", "key:0"}), "xxxx");

    EXPECT_THAT(Run({"cluster", "keyslot", "key:0"}), IntArg(2592));
    EXPECT_THAT(Run({"dbsize"}), IntArg(count));
    auto slot_size_response = Run({"dflycluster", "getslotinfo", "slots", "2592"});
    EXPECT_THAT(slot_size_response, RespArray(ElementsAre(_, "key_count", _, "total_reads", _,
                                                          "total_writes", _, "memory_bytes", _)));
    auto slot_size = slot_size_response.GetVec()[2].GetInt();
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

TEST_F(ClusterFamilyTest, ClusterCrossSlot) {
  ConfigSingleNodeCluster(GetMyId());

  EXPECT_EQ(Run({"SET", "key", "value"}), "OK");
  EXPECT_EQ(Run({"GET", "key"}), "value");

  EXPECT_EQ(Run({"MSET", "key", "value2"}), "OK");
  EXPECT_EQ(Run({"MGET", "key"}), "value2");

  EXPECT_THAT(Run({"MSET", "key", "value", "key2", "value2"}), ErrArg("CROSSSLOT"));
  EXPECT_THAT(Run({"MGET", "key", "key2"}), ErrArg("CROSSSLOT"));
  EXPECT_THAT(Run({"ZINTERSTORE", "key", "2", "key1", "key2"}), ErrArg("CROSSSLOT"));

  EXPECT_EQ(Run({"MSET", "key{tag}", "value", "key2{tag}", "value2"}), "OK");
  EXPECT_THAT(Run({"MGET", "key{tag}", "key2{tag}"}), RespArray(ElementsAre("value", "value2")));
}

class ClusterFamilyEmulatedTest : public ClusterFamilyTest {
 public:
  ClusterFamilyEmulatedTest() {
    SetTestFlag("cluster_mode", "emulated");
    SetTestFlag("cluster_announce_ip", "fake-host");
  }
};

TEST_F(ClusterFamilyEmulatedTest, ClusterInfo) {
  string cluster_info = Run({"cluster", "info"}).GetString();
  EXPECT_THAT(cluster_info, HasSubstr("cluster_state:ok"));
  EXPECT_THAT(cluster_info, HasSubstr("cluster_slots_assigned:16384"));
  EXPECT_THAT(cluster_info, HasSubstr("cluster_slots_ok:16384"));
  EXPECT_THAT(cluster_info, HasSubstr("cluster_known_nodes:1"));
  EXPECT_THAT(cluster_info, HasSubstr("cluster_size:1"));
}

TEST_F(ClusterFamilyEmulatedTest, ClusterShardInfos) {
  EXPECT_THAT(Run({"cluster", "shards"}),
              RespArray(ElementsAre("slots",                                           //
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
                                            "health", "online")))))));
}

TEST_F(ClusterFamilyEmulatedTest, ClusterSlots) {
  EXPECT_THAT(Run({"cluster", "slots"}),
              RespArray(ElementsAre(IntArg(0),              //
                                    IntArg(16383),          //
                                    RespArray(ElementsAre(  //
                                        "fake-host",        //
                                        IntArg(6379),       //
                                        GetMyId())))));
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
