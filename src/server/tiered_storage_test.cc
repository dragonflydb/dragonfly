// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/tiered_storage.h"

#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "absl/flags/internal/flag.h"
#include "absl/flags/reflection.h"
#include "base/flags.h"
#include "base/logging.h"
#include "facade/facade_test.h"
#include "gtest/gtest.h"
#include "server/engine_shard_set.h"
#include "server/test_utils.h"
#include "util/fibers/fibers.h"

using namespace std;
using namespace testing;

ABSL_DECLARE_FLAG(bool, force_epoll);
ABSL_DECLARE_FLAG(string, tiered_prefix);
ABSL_DECLARE_FLAG(bool, tiered_storage_cache_fetched);
ABSL_DECLARE_FLAG(bool, backing_file_direct);
ABSL_DECLARE_FLAG(float, tiered_offload_threshold);
ABSL_DECLARE_FLAG(unsigned, tiered_storage_write_depth);

namespace dfly {

using absl::GetFlag;
using absl::SetFlag;

string BuildString(size_t len, char c = 'A') {
  return string(len, c);
}

class TieredStorageTest : public BaseFamilyTest {
 protected:
  TieredStorageTest() {
    num_threads_ = 1;
  }

  void SetUp() override {
    if (GetFlag(FLAGS_force_epoll)) {
      LOG(WARNING) << "Can't run tiered tests on EPOLL";
      exit(0);
    }

    SetFlag(&FLAGS_tiered_storage_write_depth, 15000);
    if (GetFlag(FLAGS_tiered_prefix).empty()) {
      SetFlag(&FLAGS_tiered_prefix, "/tmp/tiered_storage_test");
    }
    SetFlag(&FLAGS_tiered_storage_cache_fetched, true);
    SetFlag(&FLAGS_backing_file_direct, true);

    BaseFamilyTest::SetUp();
  }
};

// Perform simple series of SET, GETSET and GET
TEST_F(TieredStorageTest, SimpleGetSet) {
  absl::FlagSaver saver;
  SetFlag(&FLAGS_tiered_offload_threshold, 1.1f);  // disable offloading
  const int kMin = 256;
  const int kMax = tiering::kPageSize + 10;

  // Perform SETs
  for (size_t i = kMin; i < kMax; i++) {
    Run({"SET", absl::StrCat("k", i), BuildString(i)});
  }

  // Make sure all entries were stashed, except the one not filling a small page
  size_t stashes = 0;
  ExpectConditionWithinTimeout([this, &stashes] {
    stashes = GetMetrics().tiered_stats.total_stashes;
    return stashes >= kMax - 256 - 1;
  });

  // All entries were accounted for except that one (see comment above)
  auto metrics = GetMetrics();
  EXPECT_EQ(metrics.db_stats[0].tiered_entries, kMax - kMin - 1);
  EXPECT_EQ(metrics.db_stats[0].tiered_used_bytes, (kMax - 1 + kMin) * (kMax - kMin) / 2 - 2047);

  // Perform GETSETs
  for (size_t i = kMin; i < kMax; i++) {
    auto resp = Run({"GETSET", absl::StrCat("k", i), string(i, 'B')});
    ASSERT_EQ(resp, string(i, 'A')) << i;
  }

  // Perform GETs
  for (size_t i = kMin; i < kMax; i++) {
    auto resp = Run({"GET", absl::StrCat("k", i)});
    ASSERT_EQ(resp, string(i, 'B')) << i;
  }

  metrics = GetMetrics();
  EXPECT_EQ(metrics.db_stats[0].tiered_entries, 0);
  EXPECT_EQ(metrics.db_stats[0].tiered_used_bytes, 0);
}

TEST_F(TieredStorageTest, MGET) {
  vector<string> command = {"MGET"}, values = {};
  for (char key = 'A'; key <= 'B'; key++) {
    command.emplace_back(1, key);
    values.emplace_back(3000, key);
    Run({"SET", command.back(), values.back()});
  }

  ExpectConditionWithinTimeout(
      [this, &values] { return GetMetrics().tiered_stats.total_stashes >= values.size(); });

  auto resp = Run(absl::MakeSpan(command));
  auto elements = resp.GetVec();
  for (size_t i = 0; i < elements.size(); i++)
    EXPECT_EQ(elements[i], values[i]);
}

TEST_F(TieredStorageTest, SimpleAppend) {
  // TODO: use pipelines to issue APPEND/GET/APPEND sequence,
  // currently it's covered only for op_manager_test
  for (size_t sleep : {0, 100, 500, 1000}) {
    Run({"SET", "k0", BuildString(3000)});
    if (sleep)
      util::ThisFiber::SleepFor(sleep * 1us);
    EXPECT_THAT(Run({"APPEND", "k0", "B"}), IntArg(3001));
    EXPECT_EQ(Run({"GET", "k0"}), BuildString(3000) + 'B');
  }
}

TEST_F(TieredStorageTest, MultiDb) {
  for (size_t i = 0; i < 10; i++) {
    Run({"SELECT", absl::StrCat(i)});
    Run({"SET", absl::StrCat("k", i), BuildString(3000, char('A' + i))});
  }

  ExpectConditionWithinTimeout([this] { return GetMetrics().tiered_stats.total_stashes >= 10; });

  for (size_t i = 0; i < 10; i++) {
    Run({"SELECT", absl::StrCat(i)});
    EXPECT_EQ(GetMetrics().db_stats[i].tiered_entries, 1);
    EXPECT_EQ(Run({"GET", absl::StrCat("k", i)}), BuildString(3000, char('A' + i)));
    EXPECT_EQ(GetMetrics().db_stats[i].tiered_entries, 0);
  }
}

TEST_F(TieredStorageTest, Defrag) {
  for (char k = 'a'; k < 'a' + 8; k++) {
    Run({"SET", string(1, k), string(512, k)});
  }

  ExpectConditionWithinTimeout([this] { return GetMetrics().tiered_stats.total_stashes >= 1; });

  // 7 out 8 are in one bin, the last one made if flush and is now filling
  auto metrics = GetMetrics();
  EXPECT_EQ(metrics.tiered_stats.small_bins_cnt, 1u);
  EXPECT_EQ(metrics.tiered_stats.small_bins_entries_cnt, 7u);
  EXPECT_EQ(metrics.tiered_stats.small_bins_filling_bytes, 512 + 12);

  // Reading 3 values still leaves the bin more than half occupied
  Run({"GET", string(1, 'a')});
  Run({"GET", string(1, 'b')});
  Run({"GET", string(1, 'c')});
  metrics = GetMetrics();
  EXPECT_EQ(metrics.tiered_stats.small_bins_cnt, 1u);
  EXPECT_EQ(metrics.tiered_stats.small_bins_entries_cnt, 4u);

  // This tirggers defragmentation, as only 3 < 7/2 remain left
  Run({"GET", string(1, 'd')});
  metrics = GetMetrics();
  EXPECT_EQ(metrics.tiered_stats.total_defrags, 3u);
  EXPECT_EQ(metrics.tiered_stats.small_bins_cnt, 0u);
  EXPECT_EQ(metrics.tiered_stats.allocated_bytes, 0u);
}

TEST_F(TieredStorageTest, BackgroundOffloading) {
  absl::FlagSaver saver;
  SetFlag(&FLAGS_tiered_offload_threshold, 0.0f);  // offload all values

  const int kNum = 500;

  max_memory_limit = kNum * 4096;
  pp_->at(0)->AwaitBrief([] { EngineShard::tlocal()->TEST_EnableHeartbeat(); });

  // Stash all values
  string value = BuildString(3000);
  for (size_t i = 0; i < kNum; i++) {
    Run({"SETEX", absl::StrCat("k", i), "100", value});
  }

  ExpectConditionWithinTimeout([&] { return GetMetrics().db_stats[0].tiered_entries == kNum; });
  ASSERT_EQ(GetMetrics().tiered_stats.total_stashes, kNum);
  ASSERT_EQ(GetMetrics().db_stats[0].tiered_entries, kNum);

  // Trigger re-fetch and test TTL is preserved.
  for (size_t i = 0; i < kNum; i++) {
    string key = absl::StrCat("k", i);
    auto resp = Run({"TTL", key});
    EXPECT_THAT(resp, IntArg(100));

    resp = Run({"GET", key});
    EXPECT_EQ(resp, value);
    resp = Run({"TTL", key});
    EXPECT_THAT(resp, IntArg(100));
  }

  // Wait for offload to do it all again
  ExpectConditionWithinTimeout([&] { return GetMetrics().db_stats[0].tiered_entries == kNum; });

  auto metrics = GetMetrics();
  EXPECT_EQ(metrics.tiered_stats.total_stashes, 2 * kNum);
  EXPECT_EQ(metrics.tiered_stats.total_fetches, kNum);
  EXPECT_EQ(metrics.tiered_stats.allocated_bytes, kNum * 4096);
}

TEST_F(TieredStorageTest, FlushAll) {
  absl::FlagSaver saver;
  SetFlag(&FLAGS_tiered_offload_threshold, 0.0f);  // offload all values

  const int kNum = 500;
  for (size_t i = 0; i < kNum; i++) {
    Run({"SET", absl::StrCat("k", i), BuildString(3000)});
  }
  ExpectConditionWithinTimeout([&] { return GetMetrics().db_stats[0].tiered_entries == kNum; });

  // Start reading random entries
  atomic_bool done = false;
  auto reader = pp_->at(0)->LaunchFiber([&] {
    while (!done) {
      Run("reader", {"GET", absl::StrCat("k", rand() % kNum)});
      util::ThisFiber::Yield();
    }
  });

  util::ThisFiber::SleepFor(50ms);
  Run({"FLUSHALL"});

  done = true;
  util::ThisFiber::SleepFor(50ms);
  reader.Join();

  auto metrics = GetMetrics();
  EXPECT_EQ(metrics.db_stats.front().tiered_entries, 0u);
  EXPECT_GT(metrics.tiered_stats.total_fetches, 2u);
}

TEST_F(TieredStorageTest, FlushPending) {
  absl::FlagSaver saver;
  SetFlag(&FLAGS_tiered_offload_threshold, 0.0f);  // offload all values

  const int kNum = 10;
  for (size_t i = 0; i < kNum; i++) {
    Run({"SET", absl::StrCat("k", i), BuildString(256)});
  }
  ExpectConditionWithinTimeout(
      [&] { return GetMetrics().tiered_stats.small_bins_filling_bytes > 0; });
  Run({"FLUSHALL"});
  EXPECT_EQ(GetMetrics().tiered_stats.small_bins_filling_bytes, 0u);
}

}  // namespace dfly
