// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <absl/flags/reflection.h>

#include <string>
#include <string_view>

#include "base/gtest.h"
#include "base/logging.h"
#include "facade/facade_test.h"
#include "server/test_utils.h"

namespace dfly {
namespace {
using namespace std;

class DflyFamilyTest : public BaseFamilyTest {};

class DflyClusterTest : public DflyFamilyTest {
 public:
  DflyClusterTest() {
    auto* flag = absl::FindCommandLineFlag("cluster_mode");
    CHECK_NE(flag, nullptr);
    string error;
    CHECK(flag->ParseFrom("yes", &error));
  }

 protected:
  static constexpr string_view kInvalidConfiguration = "Invalid cluster configuration";
};

TEST_F(DflyClusterTest, ClusterConfigInvalid) {
  EXPECT_THAT(Run({"dfly", "cluster", "config"}), ErrArg("syntax error"));
  EXPECT_THAT(Run({"dfly", "cluster", "config", "invalid JSON"}),
              ErrArg("Invalid JSON cluster config"));
  EXPECT_THAT(Run({"dfly", "cluster", "config", "[]"}), ErrArg(kInvalidConfiguration));
}

TEST_F(DflyClusterTest, ClusterConfigInvalidMissingSlots) {
  EXPECT_THAT(Run({"dfly", "cluster", "config", R"json(
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
}

TEST_F(DflyClusterTest, ClusterConfigInvalidOverlappingSlots) {
  EXPECT_THAT(Run({"dfly", "cluster", "config", R"json(
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
}

TEST_F(DflyClusterTest, ClusterConfigNoReplicas) {
  EXPECT_EQ(Run({"dfly", "cluster", "config", R"json(
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

  // TODO: Use "CLUSTER SLOTS" and "CLUSTER SHARDS" once implemented to verify new configuration
  // takes effect.
}

TEST_F(DflyClusterTest, ClusterConfigFull) {
  EXPECT_EQ(Run({"dfly", "cluster", "config", R"json(
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

  // TODO: Use "CLUSTER SLOTS" and "CLUSTER SHARDS" once implemented to verify new configuration
  // takes effect.
}

TEST_F(DflyClusterTest, ClusterConfigFullMultipleInstances) {
  EXPECT_EQ(Run({"dfly", "cluster", "config", R"json(
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

  // TODO: Use "CLUSTER SLOTS" and "CLUSTER SHARDS" once implemented to verify new configuration
  // takes effect.
}

}  // namespace
}  // namespace dfly
