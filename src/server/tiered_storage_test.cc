// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/tiered_storage.h"

#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "absl/flags/internal/flag.h"
#include "base/flags.h"
#include "base/logging.h"
#include "facade/facade_test.h"
#include "gtest/gtest.h"
#include "server/engine_shard_set.h"
#include "server/test_utils.h"
#include "util/fibers/fibers.h"

using namespace std;
using namespace testing;
using absl::SetFlag;
using absl::StrCat;

ABSL_DECLARE_FLAG(bool, force_epoll);
ABSL_DECLARE_FLAG(string, tiered_prefix);
ABSL_DECLARE_FLAG(string, tiered_prefix_v2);
ABSL_DECLARE_FLAG(bool, tiered_storage_v2_cache_fetched);

namespace dfly {

class TieredStorageV2Test : public BaseFamilyTest {
 protected:
  TieredStorageV2Test() {
    num_threads_ = 1;
  }

  void SetUp() override {
    if (absl::GetFlag(FLAGS_force_epoll)) {
      LOG(WARNING) << "Can't run tiered tests on EPOLL";
      exit(0);
    }

    absl::SetFlag(&FLAGS_tiered_prefix, "");
    absl::SetFlag(&FLAGS_tiered_prefix_v2, "/tmp/tiered_storage_test");
    absl::SetFlag(&FLAGS_tiered_storage_v2_cache_fetched, true);

    BaseFamilyTest::SetUp();
  }
};

// Perform simple series of SET, GETSET and GET
TEST_F(TieredStorageV2Test, SimpleGetSet) {
  const int kMax = 5000;

  // Perform SETs
  for (size_t i = 64; i < kMax; i++) {
    Run({"SET", absl::StrCat("k", i), string(i, 'A')});
  }

  // Make sure all entries were stashed, except the one few not filling a small page
  size_t stashes = 0;
  ExpectConditionWithinTimeout([this, &stashes] {
    stashes = GetMetrics().tiered_stats_v2.total_stashes;
    return stashes >= kMax - 64 - 1;
  });

  // Perform GETSETs
  for (size_t i = 64; i < kMax; i++) {
    auto resp = Run({"GETSET", absl::StrCat("k", i), string(i, 'B')});
    ASSERT_EQ(resp, string(i, 'A')) << i;
  }

  // Perform GETs
  for (size_t i = 64; i < kMax; i++) {
    auto resp = Run({"GET", absl::StrCat("k", i)});
    ASSERT_EQ(resp, string(i, 'B')) << i;
  }
}

}  // namespace dfly
