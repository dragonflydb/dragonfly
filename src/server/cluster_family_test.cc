// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <absl/flags/reflection.h>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest-matchers.h>

#include <string>
#include <string_view>

#include "absl/strings/substitute.h"
#include "base/gtest.h"
#include "base/logging.h"
#include "facade/facade_test.h"
#include "server/test_utils.h"

namespace dfly {
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
};

TEST_F(ClusterFamilyTest, DflyClusterOnlyOnAdminPort) {
  string config = R"json(
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
          "replicas": []
        }
      ])json";
  EXPECT_EQ(RunAdmin({"dflycluster", "config", config}), "OK");
  EXPECT_THAT(Run({"dflycluster", "config", config}),
              ErrArg("DFLYCLUSTER commands requires admin port"));
}

TEST_F(ClusterFamilyTest, ClusterConfigInvalidJSON) {
  EXPECT_THAT(RunAdmin({"dflycluster", "config", "invalid JSON"}),
              ErrArg("Invalid JSON cluster config"));

  string cluster_info = Run({"cluster", "info"}).GetString();
  EXPECT_THAT(cluster_info, HasSubstr("cluster_state:fail"));
  EXPECT_THAT(cluster_info, HasSubstr("cluster_slots_assigned:0"));
  EXPECT_THAT(cluster_info, HasSubstr("cluster_slots_ok:0"));
  EXPECT_THAT(cluster_info, HasSubstr("cluster_known_nodes:0"));
  EXPECT_THAT(cluster_info, HasSubstr("cluster_size:0"));

  EXPECT_THAT(Run({"cluster", "shards"}), ErrArg("Cluster is not yet configured"));
  EXPECT_THAT(Run({"cluster", "slots"}), ErrArg("Cluster is not yet configured"));
}

TEST_F(ClusterFamilyTest, ClusterConfigInvalidConfig) {
  EXPECT_THAT(RunAdmin({"dflycluster", "config", "[]"}), ErrArg(kInvalidConfiguration));

  string cluster_info = Run({"cluster", "info"}).GetString();
  EXPECT_THAT(cluster_info, HasSubstr("cluster_state:fail"));
  EXPECT_THAT(cluster_info, HasSubstr("cluster_slots_assigned:0"));
  EXPECT_THAT(cluster_info, HasSubstr("cluster_slots_ok:0"));
  EXPECT_THAT(cluster_info, HasSubstr("cluster_known_nodes:0"));
  EXPECT_THAT(cluster_info, HasSubstr("cluster_size:0"));
}

TEST_F(ClusterFamilyTest, ClusterConfigInvalidMissingSlots) {
  EXPECT_THAT(RunAdmin({"dflycluster", "config", R"json(
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
  EXPECT_THAT(RunAdmin({"dflycluster", "config", R"json(
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
  EXPECT_EQ(RunAdmin({"dflycluster", "config", R"json(
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
          "replicas": []
        }
      ])json"}),
            "OK");

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
}

TEST_F(ClusterFamilyTest, ClusterConfigFull) {
  EXPECT_EQ(RunAdmin({"dflycluster", "config", R"json(
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
}

TEST_F(ClusterFamilyTest, ClusterConfigFullMultipleInstances) {
  EXPECT_EQ(RunAdmin({"dflycluster", "config", R"json(
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

  absl::InsecureBitGen eng;
  while (true) {
    string random_key = GetRandomHex(eng, 40);
    SlotId slot = ClusterConfig::KeySlot(random_key);
    if (slot > 10'000) {
      continue;
    }

    EXPECT_THAT(Run({"get", random_key}).GetString(),
                testing::MatchesRegex(R"(MOVED [0-9]+ 10.0.0.1:7000)"));
    break;
  }

  while (true) {
    string random_key = GetRandomHex(eng, 40);
    SlotId slot = ClusterConfig::KeySlot(random_key);
    if (slot <= 10'000) {
      continue;
    }

    EXPECT_THAT(Run({"get", random_key}).GetString(),
                testing::MatchesRegex(R"(MOVED [0-9]+ 10.0.0.2:7001)"));
    break;
  }
}

TEST_F(ClusterFamilyTest, ClusterGetSlotInfo) {
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
  string config = absl::Substitute(config_template, RunAdmin({"dflycluster", "myid"}).GetString());

  EXPECT_EQ(RunAdmin({"dflycluster", "config", config}), "OK");

  Run({"debug", "populate", "100000"});

  auto slots_info = RunAdmin({"dflycluster", "getslotinfo", "slots", "1", "2"}).GetVec();
  EXPECT_EQ(slots_info.size(), 2);
  auto slot1 = slots_info[0].GetVec();
  EXPECT_THAT(slot1, ElementsAre("1", "key_count", Not("0")));

  auto slot2 = slots_info[1].GetVec();
  EXPECT_THAT(slot2, ElementsAre("2", "key_count", Not("0")));
}

TEST_F(ClusterFamilyTest, ClusterConfigDeleteSlots) {
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
  string config = absl::Substitute(config_template, RunAdmin({"dflycluster", "myid"}).GetString());

  EXPECT_EQ(RunAdmin({"dflycluster", "config", config}), "OK");

  Run({"debug", "populate", "100000"});

  auto slots_info = RunAdmin({"dflycluster", "getslotinfo", "slots", "1", "2"}).GetVec();
  EXPECT_EQ(slots_info.size(), 2);
  auto slot1 = slots_info[0].GetVec();
  EXPECT_THAT(slot1, ElementsAre("1", "key_count", Not("0")));

  auto slot2 = slots_info[1].GetVec();
  EXPECT_THAT(slot2, ElementsAre("2", "key_count", Not("0")));

  config = absl::Substitute(config_template, "abc");
  EXPECT_EQ(RunAdmin({"dflycluster", "config", config}), "OK");
  sleep(1);
  slots_info = RunAdmin({"dflycluster", "getslotinfo", "slots", "1", "2"}).GetVec();
  EXPECT_EQ(slots_info.size(), 2);
  slot1 = slots_info[0].GetVec();
  EXPECT_THAT(slot1, ElementsAre("1", "key_count", "0"));

  slot2 = slots_info[1].GetVec();
  EXPECT_THAT(slot2, ElementsAre("2", "key_count", "0"));
}

class ClusterFamilyEmulatedTest : public BaseFamilyTest {
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

TEST_F(ClusterFamilyEmulatedTest, ClusterShards) {
  EXPECT_THAT(Run({"cluster", "shards"}),
              RespArray(ElementsAre("slots",                                                      //
                                    RespArray(ElementsAre(IntArg(0), IntArg(16383))),             //
                                    "nodes",                                                      //
                                    RespArray(ElementsAre(                                        //
                                        RespArray(ElementsAre(                                    //
                                            "id", RunAdmin({"dflycluster", "myid"}).GetString(),  //
                                            "endpoint", "fake-host",                              //
                                            "ip", "fake-host",                                    //
                                            "port", IntArg(6379),                                 //
                                            "role", "master",                                     //
                                            "replication-offset", IntArg(0),                      //
                                            "health", "online")))))));
}

TEST_F(ClusterFamilyEmulatedTest, ClusterSlots) {
  EXPECT_THAT(Run({"cluster", "slots"}),
              RespArray(ElementsAre(IntArg(0),              //
                                    IntArg(16383),          //
                                    RespArray(ElementsAre(  //
                                        "fake-host",        //
                                        IntArg(6379),       //
                                        RunAdmin({"dflycluster", "myid"}).GetString())))));
}

}  // namespace
}  // namespace dfly
