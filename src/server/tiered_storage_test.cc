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
#include "server/engine_shard_set.h"
#include "server/test_utils.h"
#include "strings/human_readable.h"
#include "util/fibers/fibers.h"

using namespace std;
using namespace testing;
using namespace util;

ABSL_DECLARE_FLAG(bool, force_epoll);
ABSL_DECLARE_FLAG(string, tiered_prefix);
ABSL_DECLARE_FLAG(float, tiered_offload_threshold);
ABSL_DECLARE_FLAG(float, tiered_upload_threshold);
ABSL_DECLARE_FLAG(strings::MemoryBytesFlag, tiered_max_pending_stash_bytes);
ABSL_DECLARE_FLAG(bool, tiered_experimental_cooling);
ABSL_DECLARE_FLAG(uint64_t, registered_buffer_size);
ABSL_DECLARE_FLAG(bool, tiered_experimental_hash_support);
ABSL_DECLARE_FLAG(bool, tiered_experimental_list_support);
ABSL_DECLARE_FLAG(bool, tiered_writes_cooling_bypass);
ABSL_DECLARE_FLAG(unsigned, list_tiering_threshold);
ABSL_DECLARE_FLAG(int32_t, list_max_listpack_size);

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

    // Disable registered buffers in half of the runs to use only small heap allocated buffers
    // to possibly catch out of bounds reads/writes with sanitizers
    if (absl::InsecureBitGen{}() % 2) {
      SetFlag(&FLAGS_registered_buffer_size, 0);
    }

    SetFlag(&FLAGS_tiered_max_pending_stash_bytes, 32_MB);
    if (GetFlag(FLAGS_tiered_prefix).empty()) {
      SetFlag(&FLAGS_tiered_prefix, "/tmp/tiered_storage_test");
    }

    BaseFamilyTest::SetUp();
  }

  void UpdateFromFlags() {
    pp_->at(0)->AwaitBrief([] { EngineShard::tlocal()->tiered_storage()->UpdateFromFlags(); });
  }

  // A single huge PFADD stays sparse and too small to offload; batch to force dense.
  void BuildDenseHll(string_view key) {
    for (int base = 0; base < 6000; base += 100) {
      vector<string> add = {"PFADD", string(key)};
      for (int i = base; i < base + 100; ++i)
        add.push_back(absl::StrCat(key, ":", i));
      Run(absl::MakeSpan(add));
    }
  }
};

// Test that should run with both modes of "cooling"
class LatentCoolingTSTest : public TieredStorageTest, public testing::WithParamInterface<bool> {
  void SetUp() override {
    fs.emplace();
    SetFlag(&FLAGS_tiered_experimental_cooling, GetParam());
    TieredStorageTest::SetUp();
  }

  optional<absl::FlagSaver> fs;
};

INSTANTIATE_TEST_SUITE_P(TS, LatentCoolingTSTest, testing::Values(true, false));

// Disabled cooling and all values are offloaded
class PureDiskTSTest : public TieredStorageTest {
  void SetUp() override {
    fs.emplace();
    SetFlag(&FLAGS_tiered_offload_threshold, 1.0);
    SetFlag(&FLAGS_tiered_experimental_cooling, false);
    TieredStorageTest::SetUp();
  }

  optional<absl::FlagSaver> fs;
};

// Perform simple series of SET, GETSET and GET
TEST_P(LatentCoolingTSTest, SimpleGetSet) {
  absl::FlagSaver saver;
  SetFlag(&FLAGS_tiered_offload_threshold, 0.0f);  // disable offloading
  UpdateFromFlags();

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
    return stashes >= kMax - kMin - 1;
  });

  // All entries were accounted for except that one (see comment above)
  auto metrics = GetMetrics();
  EXPECT_EQ(metrics.db_stats[0].tiered_entries, kMax - kMin - 1);
  EXPECT_LE(metrics.db_stats[0].tiered_used_bytes, (kMax - 1 + kMin) * (kMax - kMin) / 2 - 2047);

  // Perform GETSETs
  for (size_t i = kMin; i < kMax; i++) {
    auto resp = Run({"GETSET", absl::StrCat("k", i), string(i, 'B')});
    ASSERT_EQ(resp, BuildString(i)) << i;
  }

  // Perform GETs
  for (size_t i = kMin; i < kMax; i++) {
    auto resp = Run({"GET", absl::StrCat("k", i)});
    ASSERT_EQ(resp, string(i, 'B')) << i;
    Run({"GET", absl::StrCat("k", i)});  // To enforce uploads.
  }

  metrics = GetMetrics();
  EXPECT_EQ(metrics.db_stats[0].tiered_entries, 0);
  EXPECT_EQ(metrics.db_stats[0].tiered_used_bytes, 0);
}

// With cooling enabled, a written+stashed value normally lands in the in-RAM cooling pool. When
// --tiered_writes_cooling_bypass is on, the write path requests a cooling-pool bypass so the stash
// goes straight to disk, leaving the cool pool (and thus read-hot entries) untouched.
TEST_F(TieredStorageTest, WritesCoolingBypass) {
  absl::FlagSaver saver;
  SetFlag(&FLAGS_tiered_offload_threshold, 1.0f);     // force offloading
  SetFlag(&FLAGS_tiered_experimental_cooling, true);  // stashes land in the cooling pool
  UpdateFromFlags();

  const int kLen = 4000;

  // Baseline: cold overwrites disabled -> the stashed value is retained in the cooling pool, so
  // cold_storage_bytes grows.
  SetFlag(&FLAGS_tiered_writes_cooling_bypass, false);
  UpdateFromFlags();
  Run({"SET", "hot", BuildString(kLen)});
  ExpectConditionWithinTimeout([this] { return GetMetrics().tiered_stats.total_stashes >= 1; });
  size_t cool_baseline = GetMetrics().tiered_stats.cold_storage_bytes;
  EXPECT_GT(cool_baseline, 0u);

  // Cold overwrites enabled -> the stashed value bypasses the cooling pool and goes disk-only, so
  // cold_storage_bytes does not grow for it.
  SetFlag(&FLAGS_tiered_writes_cooling_bypass, true);
  UpdateFromFlags();
  Run({"SET", "cold", BuildString(kLen)});
  ExpectConditionWithinTimeout([this] { return GetMetrics().tiered_stats.total_stashes >= 2; });

  auto metrics = GetMetrics();
  EXPECT_EQ(metrics.tiered_stats.cold_storage_bytes, cool_baseline);  // unchanged by the cold SET
  EXPECT_EQ(metrics.db_stats[0].tiered_entries, 2);

  // The cold value is still readable (uploaded back from disk on access).
  EXPECT_EQ(Run({"GET", "cold"}), BuildString(kLen));
}

TEST_P(LatentCoolingTSTest, StrLen) {
  absl::FlagSaver saver;
  SetFlag(&FLAGS_tiered_offload_threshold, 1.0f);  // force offloading
  UpdateFromFlags();

  // Edge case: Non-existent key
  EXPECT_EQ(Run({"STRLEN", "nonexistent"}), 0);

  const int kLen = 4000;
  Run({"SET", "k1", BuildString(kLen)});

  // Make sure it's stashed
  ExpectConditionWithinTimeout([this] { return GetMetrics().tiered_stats.total_stashes == 1; });

  auto metrics_before = GetMetrics();

  // Perform STRLEN - should be optimized (no fetch)
  auto resp = Run({"STRLEN", "k1"});
  EXPECT_EQ(resp, kLen);

  auto metrics_after = GetMetrics();
  EXPECT_EQ(metrics_after.tiered_stats.total_fetches, metrics_before.tiered_stats.total_fetches);

  // Now perform APPEND which should trigger a modification pending
  Run({"APPEND", "k1", "suffix"});

  // STRLEN now should still work but will fetch if modification IS pending
  // NOTE: with experimental cooling enabled, the value might still be in memory (CoolQueue)
  // so it might NOT trigger a disk fetch if it was just appended to.
  resp = Run({"STRLEN", "k1"});
  EXPECT_EQ(resp, kLen + 6);

  // Verify fetch happened IF cooling is disabled (direct to disk)
  metrics_after = GetMetrics();
  LOG(INFO) << "GetParam: " << GetParam()
            << " Before: " << metrics_before.tiered_stats.total_fetches
            << " After: " << metrics_after.tiered_stats.total_fetches;
  if (!GetParam()) {
    EXPECT_GT(metrics_after.tiered_stats.total_fetches, metrics_before.tiered_stats.total_fetches);
  }

  // Edge case: Empty string offloaded (if possible, though usually too small)
  // Small strings aren't offloaded by default, but we can test a string just above the limit
  const int kSmallLen = 100;  // Above default tiered_min_value_size
  Run({"SET", "k2", BuildString(kSmallLen)});
  ExpectConditionWithinTimeout([this] { return GetMetrics().tiered_stats.total_stashes >= 2; });

  EXPECT_EQ(Run({"STRLEN", "k2"}), kSmallLen);
}

TEST_F(TieredStorageTest, IntStrings) {
  absl::FlagSaver saver;
  SetFlag(&FLAGS_tiered_upload_threshold,
          0.0f);  // do not stop uploads based on free-memory threshold (this test does not itself
                  // trigger uploads)
  UpdateFromFlags();

  // STRING object can be encoded as LONG LONG internally
  string short_int_string = BuildString(18, '1');
  Run({"SET", "k1", short_int_string});

  // STRING object is not offloaded due to its small size
  string long_int_string = BuildString(32, '1');
  Run({"SET", "k2", long_int_string});

  // Long STRING object that is offloaded
  string tiered_int_string = BuildString(4096, '1');
  Run({"SET", "k3", tiered_int_string});

  ExpectConditionWithinTimeout([this] { return GetMetrics().tiered_stats.total_stashes == 1; });
}

// Use MGET to load multiple offloaded values
TEST_P(LatentCoolingTSTest, MGET) {
  vector<string> command = {"MGET"}, values = {};
  for (char key = 'A'; key <= 'Z'; key++) {
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

// Test that squashed GET/MGET commands over offloaded values run their disk reads concurrently.
TEST_F(PureDiskTSTest, DISABLED_MGETParallel) {
  // Create kMax strings and offload them. Each value fills its own page so each key maps to a
  // distinct pending read, making concurrent reads observable via pending_read_cnt.
  const int kMax = 500;
  std::string value(3000, 'c');
  for (int i = 0; i < kMax; i++)
    Run({"SET", absl::StrCat("key:", i), value});
  ExpectConditionWithinTimeout([this] { return GetMetrics().tiered_stats.total_stashes >= kMax; });

  // Build a batch of GET/MGET commands of varying widths
  vector<vector<string>> cmds;
  for (int i = 0; i < 500; i++) {
    int width = 1 + (i * 7) % 13;  // 1..13 keys
    vector<string> cmd = {width == 1 ? "GET" : "MGET"};
    for (int j = 0; j < width; j++)
      cmd.emplace_back(absl::StrCat("key:", (i * 7 + j) % kMax));
    cmds.push_back(std::move(cmd));
  }

  // Sample the peak number of in-flight tiered reads on the shard while the batch runs
  uint32_t peak_reads = 0;
  uint64_t sample_count = 0;
  atomic_bool sampling{true};
  auto sampler = pp_->at(0)->LaunchFiber([&] {
    while (sampling.load(memory_order_relaxed)) {
      uint32_t cur = EngineShard::tlocal()->tiered_storage()->GetStats().pending_read_cnt;
      sample_count++;
      peak_reads = std::max(cur, peak_reads);
      ThisFiber::Yield();
    }
  });

  // Dispatch them all at once through the squashing pipeline.
  RunMany(cmds);

  sampling.store(false, memory_order_relaxed);
  sampler.JoinIfNeeded();
  auto m = GetMetrics();
  LOG(INFO) << "peak_reads=" << peak_reads << " samples=" << sample_count
            << " total_fetches=" << m.tiered_stats.total_fetches
            << " total_stashes=" << m.tiered_stats.total_stashes;

  // Must be more than max arguments
  if (sample_count > 0) {
    EXPECT_GT(peak_reads, 10u)
        << "expected concurrent tiered reads while executing the squashed batch";
  }
}

// Issue many APPEND commands to an offloaded value that are executed at once (with CLIENT PAUSE).
// They should all finish within the same io completion loop.
TEST_F(TieredStorageTest, AppendStorm) {
  const size_t kAppends = 20;

  absl::FlagSaver saver;
  absl::SetFlag(&FLAGS_tiered_offload_threshold, 1.0);
  absl::SetFlag(&FLAGS_tiered_upload_threshold, 0.0);
  absl::SetFlag(&FLAGS_tiered_experimental_cooling, false);
  UpdateFromFlags();

  // Offload single value
  string base_value(4096, 'a');
  Run({"SET", "key", base_value});
  ExpectConditionWithinTimeout([this] { return GetMetrics().tiered_stats.total_stashes == 1; });

  // Accumulate APPENDs
  Run({"CLIENT", "pause", "1000"});
  vector<Fiber> fibs;
  for (size_t i = 0; i < kAppends; i++) {
    fibs.emplace_back(pp_->at(0)->LaunchFiber([this, i] {
      Run(absl::StrCat(i), {"APPEND", "key", string(96, 'b')});
    }));
  }

  // Throw in a SETRANGE
  fibs.emplace_back(pp_->at(0)->LaunchFiber([this] {
    Run("range", {"SETRANGE", "key", "0", string(96, 'x')});
  }));

  // Throw in a GETRANGE to a range that keeps constant
  string get_range;
  fibs.emplace_back(pp_->at(0)->LaunchFiber([this, &get_range] {
    get_range = Run("get", {"GETRANGE", "key", "96", "191"}).GetString();
  }));

  // Unlock and wait
  Run({"CLIENT", "unpause"});
  for (auto& f : fibs)
    f.JoinIfNeeded();

  // Check partial result is right
  EXPECT_EQ(get_range, string(96, 'a'));

  // Get value and verify it
  auto value = Run({"GET", "key"});
  EXPECT_EQ(value, string(96, 'x') + string(4000, 'a') + string(kAppends * 96, 'b'));

  // Check value was read no more than once for APPENDs and once for GET
  auto metrics = GetMetrics();
  EXPECT_LE(metrics.tiered_stats.total_fetches, 2u);
  EXPECT_LE(metrics.tiered_stats.total_uploads, 2u);
}

// SETRANGE and GETRANGE
TEST_P(LatentCoolingTSTest, Ranges) {
  Run({"SET", "key", string(3000, 'a')});
  ExpectConditionWithinTimeout([this] { return GetMetrics().tiered_stats.total_stashes >= 1; });

  Run({"SETRANGE", "key", "1000", string(1000, 'b')});
  auto resp = Run({"GET", "key"});
  EXPECT_EQ(resp, string(1000, 'a') + string(1000, 'b') + string(1000, 'a'));

  Run({"DEL", "key"});
  Run({"SET", "key", string(1500, 'c') + string(1500, 'd')});
  ExpectConditionWithinTimeout([this] { return GetMetrics().tiered_stats.total_stashes >= 2; });

  resp = Run({"GETRANGE", "key", "1000", "1999"});
  EXPECT_EQ(resp, string(500, 'c') + string(500, 'd'));
}

// Stash values from different databases and read them back
TEST_P(LatentCoolingTSTest, MultiDb) {
  for (size_t i = 0; i < 10; i++) {
    Run({"SELECT", absl::StrCat(i)});
    Run({"SET", absl::StrCat("k", i), BuildString(3000, char('A' + i))});
  }

  ExpectConditionWithinTimeout([this] { return GetMetrics().tiered_stats.total_stashes >= 10; });

  for (size_t i = 0; i < 10; i++) {
    Run({"SELECT", absl::StrCat(i)});
    EXPECT_EQ(GetMetrics().db_stats[i].tiered_entries, 1);
    string key = absl::StrCat("k", i);
    EXPECT_EQ(Run({"GET", key}), BuildString(3000, char('A' + i)));
    Run({"GET", key});
    EXPECT_EQ(GetMetrics().db_stats[i].tiered_entries, 0);
  }
}

// Trigger defragmentation
TEST_F(TieredStorageTest, Defrag) {
  for (char k = 'a'; k < 'a' + 8; k++) {
    Run({"SET", string(1, k), string(600, k)});
  }

  ExpectConditionWithinTimeout([this] { return GetMetrics().tiered_stats.total_stashes >= 1; });

  // 7 out 8 are in one bin, the last one made if flush and is now filling
  auto metrics = GetMetrics();
  ASSERT_EQ(metrics.tiered_stats.small_bins_cnt, 1u);
  ASSERT_EQ(metrics.tiered_stats.small_bins_entries_cnt, 7u);

  // Distorted due to encoded values.
  ASSERT_EQ(metrics.tiered_stats.small_bins_filling_bytes, 537);

  // Reading 3 values still leaves the bin more than half occupied
  for (unsigned j = 0; j < 2; ++j) {
    Run({"GET", string(1, 'a')});
    Run({"GET", string(1, 'b')});
    Run({"GET", string(1, 'c')});
  }
  metrics = GetMetrics();
  EXPECT_EQ(metrics.tiered_stats.small_bins_cnt, 1u);
  EXPECT_EQ(metrics.tiered_stats.small_bins_entries_cnt, 4u);

  // This tirggers defragmentation, as only 3 < 7/2 remain left
  Run({"GET", string(1, 'd')});

  // Wait that any reads caused by defrags has been finished.
  ExpectConditionWithinTimeout([this] { return GetMetrics().tiered_stats.pending_read_cnt == 0; });
  metrics = GetMetrics();
  EXPECT_EQ(metrics.tiered_stats.total_defrags, 3u);
  EXPECT_EQ(metrics.tiered_stats.small_bins_cnt, 0u);
  EXPECT_EQ(metrics.tiered_stats.allocated_bytes, 0u);
}

TEST_F(PureDiskTSTest, BackgroundOffloading) {
  absl::FlagSaver saver;
  SetFlag(&FLAGS_tiered_upload_threshold, 0.0f);  // upload all values
  UpdateFromFlags();

  const int kNum = 500;

  max_memory_limit = kNum * 4096;

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
    Run({"GET", key});  // enforce uploads
  }

  // Wait for offload to do it all again
  ExpectConditionWithinTimeout([&] { return GetMetrics().db_stats[0].tiered_entries == kNum; });
  auto resp = Run({"INFO", "ALL"});
  VLOG(1) << "INFO " << resp.GetString();
  auto metrics = GetMetrics();

  // Not all values were necessary uploaded during GET calls, but all that were uploaded
  // should be re-stashed again.
  EXPECT_EQ(metrics.tiered_stats.total_stashes, kNum + metrics.tiered_stats.total_uploads)
      << resp.GetString();
  EXPECT_EQ(metrics.tiered_stats.allocated_bytes, kNum * 4096);
}

// Verify correctness of our offloading startegy, offloading values only after second access.
TEST_F(PureDiskTSTest, OffloadingStrategy) {
  // Create value and wait to be offlaoded
  string value = BuildString(3000);
  Run({"set", "key", value});
  ExpectConditionWithinTimeout([&] { return GetMetrics().db_stats[0].tiered_entries == 1; });

  // Check base values
  auto metrics = GetMetrics();
  EXPECT_EQ(metrics.tiered_stats.total_fetches, 0);
  EXPECT_EQ(metrics.tiered_stats.total_uploads, 0);
  EXPECT_EQ(metrics.tiered_stats.total_stashes, 1);

  // Repeat a few times
  for (size_t i = 1; i <= 3; i++) {
    // Value is not uploaded after first read
    Run({"get", "key"});
    metrics = GetMetrics();
    EXPECT_EQ(metrics.tiered_stats.total_fetches, 2 * i - 1);
    EXPECT_EQ(metrics.tiered_stats.total_uploads, i - 1);

    // But on second read upload should happend at the end of chain due to two touches
    Run({"get", "key"});
    ExpectConditionWithinTimeout([&] { return GetMetrics().tiered_stats.total_uploads == i; });
    metrics = GetMetrics();
    EXPECT_EQ(metrics.tiered_stats.total_fetches, 2 * i);

    // Wait for offloading again
    ExpectConditionWithinTimeout([&] { return GetMetrics().db_stats[0].tiered_entries == 1; });
    metrics = GetMetrics();
    EXPECT_EQ(metrics.tiered_stats.total_offloading_stashes, i);
    EXPECT_EQ(metrics.tiered_stats.total_stashes, i + 1);
  }
}

// Test FLUSHALL while reading entries
TEST_F(PureDiskTSTest, FlushAll) {
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

  Metrics metrics;
  ExpectConditionWithinTimeout([&] {
    metrics = GetMetrics();

    // Note that metrics.events.hits is not consistent with total_fetches
    // and it can happen that hits is greater than total_fetches due to in-progress reads.
    return metrics.tiered_stats.total_fetches > 2;
  });
  LOG(INFO) << FormatMetrics(metrics);

  Run({"FLUSHALL"});

  done = true;
  util::ThisFiber::SleepFor(100ms);
  reader.Join();

  metrics = GetMetrics();
  LOG(INFO) << FormatMetrics(metrics);

  EXPECT_EQ(metrics.db_stats.front().tiered_entries, 0u);
}

// Check FLUSHALL clears filling bytes of small bins
TEST_F(TieredStorageTest, FlushPending) {
  absl::FlagSaver saver;
  SetFlag(&FLAGS_tiered_offload_threshold, 1.0f);  // offload all values

  const int kNum = 10;
  for (size_t i = 0; i < kNum; i++) {
    Run({"SET", absl::StrCat("k", i), BuildString(256)});
  }
  ExpectConditionWithinTimeout(
      [&] { return GetMetrics().tiered_stats.small_bins_filling_bytes > 0; });
  Run({"FLUSHALL"});
  EXPECT_EQ(GetMetrics().tiered_stats.small_bins_filling_bytes, 0u);
}

// Test that clients are throttled if many stashes are issued.
// Stashes are released with CLIENT UNPAUSE to occur at the same time
TEST_F(PureDiskTSTest, ThrottleClients) {
  absl::FlagSaver saver;
  absl::SetFlag(&FLAGS_tiered_upload_threshold, 0.0);
  // Set low limit so all clients are throttled
  absl::SetFlag(&FLAGS_tiered_max_pending_stash_bytes, 1);
  UpdateFromFlags();

  // issue client pause to accumualte SETs
  Run({"CLIENT", "PAUSE", "1000"});

  string value(4096, 'a');
  vector<Fiber> fibs;
  for (size_t i = 0; i < 100; i++) {
    fibs.emplace_back(pp_->at(0)->LaunchFiber([this, i, &value] {
      string key = absl::StrCat("k", i);
      Run(key, {"SET", key, value});
    }));
  }
  ThisFiber::Yield();

  // Unpause
  Run({"CLIENT", "UNPAUSE"});

  // Check that we provided backpressure for all the clients.
  auto metrics = GetMetrics();
  EXPECT_EQ(metrics.tiered_stats.total_clients_throttled, fibs.size());

  for (auto& fib : fibs)
    fib.JoinIfNeeded();

  // Because of the 5ms max wait time for backpressure, we can't rely on the stashes to have
  // finished even after all the fibers joined, so expect the condition with a timeout
  ExpectConditionWithinTimeout(
      [&] { return GetMetrics().tiered_stats.total_stashes == fibs.size(); });
}

TEST_F(TieredStorageTest, Expiry) {
  string val = BuildString(100);
  Run({"psetex", "key1", "1", val});
  AdvanceTime(10);
  Run({"psetex", "key1", "1", val});
  auto resp = Run({"get", "key1"});
  EXPECT_EQ(resp, val);
}

TEST_F(TieredStorageTest, ShortTtlOffloadUnderMemoryPressure) {
  absl::FlagSaver saver;
  // Without memory pressure, short-lived keys are not worth offloading.
  SetFlag(&FLAGS_tiered_offload_threshold, 0.1f);
  UpdateFromFlags();

  const string value = BuildString(3000);

  Run({"SET", "k1", value, "EX", "60"});
  Run({"SET", "k2", value, "EX", "2"});
  Run({"SET", "k3", value});

  auto IsOffloaded = [&](string_view key) {
    bool result = false;
    pp_->at(0)->AwaitBrief([&] {
      EngineShard* shard = EngineShard::tlocal();
      if (!shard)
        return;
      auto& db = namespaces->GetDefaultNamespace().GetDbSlice(shard->shard_id());
      auto it = db.GetDBTable(0)->prime.Find(key);
      result = IsValid(it) && it->second.IsExternal();
    });
    return result;
  };

  // k1 has enough TTL and k3 has none, while k2 expires too soon.
  ExpectConditionWithinTimeout([&] { return IsOffloaded("k1") && IsOffloaded("k3"); });
  EXPECT_FALSE(IsOffloaded("k2"));

  // Simulate low memory so offloading protects the system despite short TTL.
  SetFlag(&FLAGS_tiered_offload_threshold, 1.0f);
  UpdateFromFlags();

  // k4 has the same short TTL as k2, but low memory should still offload it.
  Run({"SET", "k4", value, "EX", "2"});
  ExpectConditionWithinTimeout([&] { return IsOffloaded("k4"); });
}

TEST_F(PureDiskTSTest, SetExistingExpire) {
  const int kNum = 20;
  for (size_t i = 0; i < kNum; i++) {
    Run({"SETEX", absl::StrCat("k", i), "100", BuildString(256)});
  }
  ExpectConditionWithinTimeout([&] { return GetMetrics().tiered_stats.total_stashes > 1; });

  for (size_t i = 0; i < kNum; i++) {
    Run({"SETEX", absl::StrCat("k", i), "100", BuildString(256)});
  }

  for (size_t i = 0; i < kNum; i++) {
    auto resp = Run({"TTL", absl::StrCat("k", i)});
    EXPECT_THAT(resp, IntArg(100));
  }
}

TEST_F(PureDiskTSTest, Dump) {
  const int kNum = 10;
  for (size_t i = 0; i < kNum; i++) {
    Run({"SET", absl::StrCat("k", i), BuildString(3000)});  // big enough to trigger offloading.
  }

  ExpectConditionWithinTimeout([&] { return GetMetrics().tiered_stats.total_stashes == kNum; });

  auto resp = Run({"DUMP", "k0"});
  EXPECT_THAT(Run({"del", "k0"}), IntArg(1));
  resp = Run({"restore", "k0", "0", facade::ToSV(resp.GetBuf())});
  EXPECT_EQ(resp, "OK");
}

TEST_P(LatentCoolingTSTest, SimpleHash) {
  absl::FlagSaver saver;
  absl::SetFlag(&FLAGS_tiered_experimental_hash_support, true);
  absl::SetFlag(&FLAGS_tiered_upload_threshold, 0.0);  // never upload
  UpdateFromFlags();

  static constexpr size_t kNUM = 100;

  auto build_command = [](string_view key) {
    vector<string> cmd = {"HSET", string{key}};
    for (char c = 'a'; c <= 'z'; c++) {
      cmd.push_back(string{1, c});
      cmd.push_back(string{31, 'x'} + c);
    }
    return cmd;
  };

  // Create some hashes
  for (size_t i = 0; i < kNUM; i++) {
    Run(build_command(absl::StrCat("k", i)));
  }

  // Wait for all to be stashed or in end up in bins
  ExpectConditionWithinTimeout([this] {
    auto metrics = GetMetrics();
    size_t sum =
        metrics.tiered_stats.total_stashes + metrics.tiered_stats.small_bins_filling_entries_cnt;
    return sum == kNUM;
  });

  // Verify correctness
  for (size_t i = 0; i < kNUM; i++) {
    string key = absl::StrCat("k", i);
    EXPECT_THAT(Run({"HLEN", key}), IntArg(26));

    auto resp = Run({"HGET", key, string{1, 'f'}});
    auto v = string{31, 'x'} + 'f';
    EXPECT_EQ(resp, v);
  }

  // Start offloading
  SetFlag(&FLAGS_tiered_offload_threshold, 1.0);
  UpdateFromFlags();
  auto wait_offloaded = [this] {
    auto metrics = GetMetrics();
    size_t sum =
        metrics.db_stats[0].tiered_entries + metrics.tiered_stats.small_bins_filling_entries_cnt;
    return sum == kNUM;
  };

  // Wait for all offloads again
  ExpectConditionWithinTimeout(wait_offloaded);

  auto c_key = string{1, 'c'};

  // HDEL
  for (size_t i = 0; i < kNUM; i++) {
    string key = absl::StrCat("k", i);
    EXPECT_THAT(Run({"HDEL", key, c_key}), IntArg(1));
    EXPECT_THAT(Run({"HLEN", key}), IntArg(25));
  }

  // Wait for all offloads again
  ExpectConditionWithinTimeout(wait_offloaded);

  // HSET new field
  for (size_t i = 0; i < kNUM; i++) {
    string key = absl::StrCat("k", i);
    EXPECT_THAT(Run({"HSET", key, c_key, "Some new value"}), IntArg(1));
    EXPECT_THAT(Run({"HLEN", key}), IntArg(26));
    EXPECT_EQ(Run({"HGET", key, c_key}), "Some new value");
  }

  // Wait for all offloads again
  ExpectConditionWithinTimeout(wait_offloaded);

  // HSET replace field
  for (size_t i = 0; i < kNUM; i++) {
    string key = absl::StrCat("k", i);
    EXPECT_THAT(Run({"HSET", key, c_key, "Even more recent value"}), IntArg(0));
    EXPECT_EQ(Run({"HGET", key, c_key}), "Even more recent value");
  }
}

// Regression: the hash field-TTL commands read/mutate the StringMap directly via pv.RObjPtr().
// On an offloaded (external, non-cool) hash that pointer is invalid, so they used to SIGSEGV in
// FieldExpireTime()/SetFieldsExpireTime(). They must now fail gracefully like HGETEX/HSETEX.
TEST_F(TieredStorageTest, HashFieldExpiryOnOffloaded) {
  absl::FlagSaver saver;
  absl::SetFlag(&FLAGS_tiered_experimental_hash_support, true);
  absl::SetFlag(&FLAGS_tiered_experimental_cooling, false);  // no in-memory copy -> truly external
  absl::SetFlag(&FLAGS_tiered_upload_threshold, 0.0);        // never upload back to memory
  UpdateFromFlags();

  static constexpr size_t kNUM = 100;
  auto build = [](string_view key) {
    vector<string> cmd = {"HSET", string{key}};
    for (char c = 'a'; c <= 'z'; c++) {
      cmd.push_back(string{1, c});
      cmd.push_back(string{31, 'x'} + c);
    }
    return cmd;
  };
  for (size_t i = 0; i < kNUM; i++)
    Run(build(absl::StrCat("k", i)));
  ExpectConditionWithinTimeout([this] {
    auto m = GetMetrics();
    return m.tiered_stats.total_stashes + m.tiered_stats.small_bins_filling_entries_cnt == kNUM;
  });

  SetFlag(&FLAGS_tiered_offload_threshold, 1.0);  // force eviction from memory
  UpdateFromFlags();
  auto offloaded = [this] {
    auto m = GetMetrics();
    return m.db_stats[0].tiered_entries + m.tiered_stats.small_bins_filling_entries_cnt == kNUM;
  };
  ExpectConditionWithinTimeout(offloaded);

  // Exercise every key: an offloaded (external) hash can't be inspected/mutated synchronously, so
  // the command must fail gracefully; an in-memory one returns a normal reply. Neither may crash.
  // FIELDTTL/FIELDEXPIRE go through the same FieldExpireTime()/SetFieldsExpireTime() path.
  size_t external_errors = 0;
  for (size_t i = 0; i < kNUM; i++) {
    string key = absl::StrCat("k", i);
    auto httl = Run({"HTTL", key, "FIELDS", "1", "a"});
    Run({"HPEXPIRETIME", key, "FIELDS", "1", "a"});
    Run({"HGETEX", key, "FIELDS", "1", "a"});
    Run({"HEXPIRE", key, "100", "FIELDS", "1", "a"});
    Run({"FIELDTTL", key, "a"});
    Run({"FIELDEXPIRE", key, "100", "a"});
    if (httl.type == RespExpr::ERROR)
      ++external_errors;
  }
  // At least one hash was genuinely external, so the guard (not a crash) handled it.
  EXPECT_GT(external_errors, 0u);

  // The server is still alive and the data is intact (read back through the tiered path).
  EXPECT_THAT(Run({"HLEN", "k0"}), IntArg(26));
  string expected_a = string{31, 'x'} + 'a';
  EXPECT_EQ(Run({"HGET", "k0", string{1, 'a'}}), expected_a);
}

class ListNodeTieringTest : public TieredStorageTest {
  void SetUp() override {
    fs.emplace();
    SetFlag(&FLAGS_tiered_offload_threshold, 1.0f);
    SetFlag(&FLAGS_tiered_experimental_cooling, false);
    // One entry per QList node
    SetFlag(&FLAGS_list_max_listpack_size, 1);
    // Offload nodes at depth >= 2 from both ends
    SetFlag(&FLAGS_list_tiering_threshold, 2u);
    SetFlag(&FLAGS_tiered_experimental_list_support, true);
    TieredStorageTest::SetUp();
  }
  optional<absl::FlagSaver> fs;
};

// Push 6 large values; verify that at least one QList node is offloaded to disk.
TEST_F(ListNodeTieringTest, StashOccurs) {
  // 2048-byte values ensure QList creation on the very first RPUSH.
  // 6 pushes produce 6 nodes; CoolOff on the 5th push offloads node 2.
  const int kItems = 6;
  for (int i = 0; i < kItems; i++) {
    Run({"RPUSH", "mylist", BuildString(2048, static_cast<char>('a' + i))});
  }

  ExpectConditionWithinTimeout([this] { return GetMetrics().tiered_stats.total_stashes >= 1; });

  EXPECT_GE(GetMetrics().tiered_stats.total_stashes, 1u);
}

TEST_F(ListNodeTieringTest, LLenCorrectAfterStash) {
  const int kItems = 6;
  for (int i = 0; i < kItems; i++) {
    Run({"RPUSH", "mylist", BuildString(2048, static_cast<char>('a' + i))});
  }

  ExpectConditionWithinTimeout([this] { return GetMetrics().tiered_stats.total_stashes >= 1; });

  EXPECT_THAT(Run({"LLEN", "mylist"}), IntArg(kItems));

  ExpectConditionWithinTimeout([this] { return GetMetrics().tiered_stats.total_stashes >= 1; });
}

// DEL a list that has fully-offloaded nodes.
TEST_F(ListNodeTieringTest, DeleteAfterStash) {
  const int kItems = 6;
  for (int i = 0; i < kItems; i++) {
    Run({"RPUSH", "mylist", BuildString(2048, static_cast<char>('a' + i))});
  }

  // Wait until stash is complete (io_pending cleared, offloaded=1) before DEL
  // to avoid the use-after-free path for a pending stash.
  ExpectConditionWithinTimeout([this] { return GetMetrics().tiered_stats.total_stashes >= 1; });

  EXPECT_THAT(Run({"DEL", "mylist"}), IntArg(1));
  EXPECT_THAT(Run({"EXISTS", "mylist"}), IntArg(0));
}

// LRANGE all elements forces the QList iterator to call AccessForReads on every
// node, including those that were offloaded to disk. The onload_cb must
// synchronously restore node->entry before returning so LRANGE returns the
// correct values.
TEST_F(ListNodeTieringTest, LoadOccurs) {
  const int kItems = 6;
  vector<string> expected;
  for (int i = 0; i < kItems; i++) {
    string val = BuildString(2048, static_cast<char>('a' + i));
    expected.push_back(val);
    Run({"RPUSH", "mylist", val});
  }

  // Wait for at least one interior node to be offloaded.
  ExpectConditionWithinTimeout([this] { return GetMetrics().tiered_stats.total_stashes >= 1; });

  // LRANGE 0 -1 must traverse all nodes, triggering onload_cb for offloaded
  // interior nodes, and return the correct values.
  auto resp = Run({"LRANGE", "mylist", "0", "-1"});
  const auto& elements = resp.GetVec();
  ASSERT_EQ(static_cast<int>(elements.size()), kItems);
  for (int i = 0; i < kItems; i++) {
    EXPECT_EQ(elements[i].GetString(), expected[i]) << "index " << i;
  }

  // LLEN must still be correct after the load.
  EXPECT_THAT(Run({"LLEN", "mylist"}), IntArg(kItems));
}

// Verify that every element read back from a list with offloaded matches the original.
TEST_F(ListNodeTieringTest, StashedDataMatchesOnLoad) {
  const int kItems = 8;

  // Build values with a unique fill character per position so any swap or
  // corruption is immediately visible.
  vector<string> expected;
  for (int i = 0; i < kItems; i++) {
    expected.push_back(BuildString(2048, static_cast<char>('A' + i)));
    Run({"RPUSH", "mylist", expected.back()});
  }

  // Wait until at least one interior node has been flushed to disk.
  ExpectConditionWithinTimeout([this] { return GetMetrics().tiered_stats.total_stashes >= 1; });

  const auto fetches_before = GetMetrics().tiered_stats.total_fetches;

  // Read every element individually via LINDEX — this forces node-level access
  // and triggers onload_cb for each offloaded node.
  for (int i = 0; i < kItems; i++) {
    auto resp = Run({"LINDEX", "mylist", absl::StrCat(i)});
    EXPECT_EQ(resp, expected[i]) << "LINDEX mismatch at position " << i;
  }

  // At least one fetch must have gone to disk .
  EXPECT_GT(GetMetrics().tiered_stats.total_fetches, fetches_before);

  // Also verify the bulk read path returns identical data.
  auto resp = Run({"LRANGE", "mylist", "0", "-1"});
  const auto& elements = resp.GetVec();
  ASSERT_EQ(static_cast<int>(elements.size()), kItems);
  for (int i = 0; i < kItems; i++) {
    EXPECT_EQ(elements[i].GetString(), expected[i]) << "LRANGE mismatch at position " << i;
  }

  EXPECT_THAT(Run({"LLEN", "mylist"}), IntArg(kItems));
}

// DEL a list whose interior nodes have io_pending=1 (stash in flight).
TEST_F(ListNodeTieringTest, DeleteWhileNodePending) {
  const int kItems = 6;
  for (int i = 0; i < kItems; i++) {
    Run({"RPUSH", "mylist", BuildString(2048, static_cast<char>('a' + i))});
  }

  // DEL immediately — do NOT wait for stashes to complete.
  EXPECT_THAT(Run({"DEL", "mylist"}), IntArg(1));
  EXPECT_THAT(Run({"EXISTS", "mylist"}), IntArg(0));

  // Drain any still-in-flight stash I/Os so allocation counters are final.
  ExpectConditionWithinTimeout([this] { return GetMetrics().tiered_stats.pending_stash_cnt == 0; });

  auto metrics = GetMetrics();
  EXPECT_EQ(metrics.db_stats[0].tiered_entries, 0u);

  ExpectConditionWithinTimeout([this] { return GetMetrics().tiered_stats.allocated_bytes == 0; });
}

// RENAME a list whose interior nodes are fully offloaded.
TEST_F(ListNodeTieringTest, RenameWithStashedNodes) {
  const int kItems = 6;
  vector<string> expected;
  for (int i = 0; i < kItems; i++) {
    expected.push_back(BuildString(2048, static_cast<char>('a' + i)));
    Run({"RPUSH", "src", expected.back()});
  }

  // Wait for interior nodes to be fully offloaded to disk.
  ExpectConditionWithinTimeout([this] { return GetMetrics().tiered_stats.total_stashes >= 1; });

  // RENAME fetches data from disk (for offloaded nodes), creates the destination
  // key, and deletes the source — all node delete_cb calls must not crash.
  EXPECT_EQ(Run({"RENAME", "src", "dst"}), "OK");
  EXPECT_THAT(Run({"EXISTS", "src"}), IntArg(0));
  EXPECT_THAT(Run({"LLEN", "dst"}), IntArg(kItems));

  // Verify every element is intact after the rename.
  for (int i = 0; i < kItems; i++) {
    EXPECT_EQ(Run({"LINDEX", "dst", absl::StrCat(i)}), expected[i]) << "index " << i;
  }

  auto metrics = GetMetrics();
  EXPECT_EQ(metrics.db_stats[0].tiered_entries, 0u);  // after reads nodes come back to memory
}

// RENAME a list while its nodes are still io_pending (stash in flight).
TEST_F(ListNodeTieringTest, RenameWhileNodesPending) {
  const int kItems = 6;
  vector<string> expected;
  for (int i = 0; i < kItems; i++) {
    expected.push_back(BuildString(2048, static_cast<char>('a' + i)));
    Run({"RPUSH", "src", expected.back()});
  }

  // RENAME immediately without waiting for stash I/Os to complete.
  EXPECT_EQ(Run({"RENAME", "src", "dst"}), "OK");
  EXPECT_THAT(Run({"EXISTS", "src"}), IntArg(0));
  EXPECT_THAT(Run({"LLEN", "dst"}), IntArg(kItems));

  // Verify data integrity in the renamed key.
  for (int i = 0; i < kItems; i++) {
    EXPECT_EQ(Run({"LINDEX", "dst", absl::StrCat(i)}), expected[i]) << "index " << i;
  }

  ExpectConditionWithinTimeout([this] { return GetMetrics().tiered_stats.pending_stash_cnt == 0; });
}

// PEXPIRE a list while its nodes are fully offloaded.
TEST_F(ListNodeTieringTest, ExpireListWithStashedNodes) {
  const int kItems = 6;
  for (int i = 0; i < kItems; i++) {
    Run({"RPUSH", "mylist", BuildString(2048, static_cast<char>('a' + i))});
  }

  ExpectConditionWithinTimeout([this] { return GetMetrics().tiered_stats.total_stashes >= 1; });

  // Set a 1 ms TTL and advance time past it.
  Run({"PEXPIRE", "mylist", "1"});
  AdvanceTime(10);

  // Trigger expiry collection.
  Run({"SET", "trigger", "x"});

  EXPECT_THAT(Run({"EXISTS", "mylist"}), IntArg(0));

  ExpectConditionWithinTimeout([this] { return GetMetrics().tiered_stats.pending_stash_cnt == 0; });

  auto metrics = GetMetrics();
  EXPECT_EQ(metrics.db_stats[0].tiered_entries, 0u);

  ExpectConditionWithinTimeout([this] { return GetMetrics().tiered_stats.allocated_bytes == 0; });
}

// PEXPIRE a list while its nodes are still io_pending (stash in flight)
TEST_F(ListNodeTieringTest, ExpireListWhileNodesPending) {
  const int kItems = 6;
  for (int i = 0; i < kItems; i++) {
    Run({"RPUSH", "mylist", BuildString(2048, static_cast<char>('a' + i))});
  }

  // Set TTL and advance time before awaiting any stash completion.
  Run({"PEXPIRE", "mylist", "1"});
  AdvanceTime(10);
  Run({"SET", "trigger", "x"});

  EXPECT_THAT(Run({"EXISTS", "mylist"}), IntArg(0));

  ExpectConditionWithinTimeout([this] { return GetMetrics().tiered_stats.pending_stash_cnt == 0; });

  auto metrics = GetMetrics();
  EXPECT_EQ(metrics.db_stats[0].tiered_entries, 0u);

  // Wait so that all allocated bytes for stashed nodes are freed
  ExpectConditionWithinTimeout([this] { return GetMetrics().tiered_stats.allocated_bytes == 0; });
}

// FLUSHALL while list nodes are io_pending (stash in flight) or fully offloaded.
TEST_F(ListNodeTieringTest, FlushAllWithTieredListNodes) {
  const int kLists = 4;
  const int kItems = 6;

  for (int l = 0; l < kLists; l++) {
    string key = absl::StrCat("list", l);
    for (int i = 0; i < kItems; i++) {
      Run({"RPUSH", key, BuildString(2048, static_cast<char>('a' + i))});
    }
  }

  // Wait for at least some nodes to be stashed across any list.
  ExpectConditionWithinTimeout([this] { return GetMetrics().tiered_stats.total_stashes >= 1; });

  // FLUSHALL while more stashes may be in flight (mixed pending/stashed state).
  Run({"FLUSHALL"});

  for (int l = 0; l < kLists; l++) {
    EXPECT_THAT(Run({"EXISTS", absl::StrCat("list", l)}), IntArg(0));
  }

  ExpectConditionWithinTimeout([this] { return GetMetrics().tiered_stats.pending_stash_cnt == 0; });

  auto metrics = GetMetrics();
  EXPECT_EQ(metrics.db_stats[0].tiered_entries, 0u);

  ExpectConditionWithinTimeout([this] { return GetMetrics().tiered_stats.allocated_bytes == 0; });
}

// LPOP exhausts a list whose interior nodes were offloaded to disk..
TEST_F(ListNodeTieringTest, LPopStashedNodes) {
  const int kItems = 8;
  vector<string> expected;
  for (int i = 0; i < kItems; i++) {
    expected.push_back(BuildString(2048, static_cast<char>('A' + i)));
    Run({"RPUSH", "mylist", expected.back()});
  }

  ExpectConditionWithinTimeout([this] { return GetMetrics().tiered_stats.total_stashes >= 1; });

  // Pop every element and verify order and content.
  for (int i = 0; i < kItems; i++) {
    EXPECT_EQ(Run({"LPOP", "mylist"}), expected[i]) << "pop index " << i;
  }

  // List must be gone after all elements are popped.
  EXPECT_THAT(Run({"EXISTS", "mylist"}), IntArg(0));
}

// MOVE a list whose interior nodes are fully offloaded to a different DB.
TEST_F(ListNodeTieringTest, MoveWithStashedNodes) {
  const int kItems = 6;
  vector<string> expected;
  for (int i = 0; i < kItems; i++) {
    expected.push_back(BuildString(2048, static_cast<char>('a' + i)));
    Run({"RPUSH", "src", expected.back()});
  }

  // Wait for interior nodes to be fully offloaded to disk.
  ExpectConditionWithinTimeout([this] { return GetMetrics().tiered_stats.total_stashes >= 1; });

  // MOVE to db 1 — SetDbIndex materializes all offloaded nodes.
  EXPECT_THAT(Run({"MOVE", "src", "1"}), IntArg(1));
  EXPECT_THAT(Run({"EXISTS", "src"}), IntArg(0));

  // Verify the key exists in db 1 with correct length and contents.
  Run({"SELECT", "1"});
  EXPECT_THAT(Run({"LLEN", "src"}), IntArg(kItems));
  for (int i = 0; i < kItems; i++) {
    EXPECT_EQ(Run({"LINDEX", "src", absl::StrCat(i)}), expected[i]) << "index " << i;
  }

  // No nodes should remain offloaded after the move.
  // auto metrics = GetMetrics();
  // EXPECT_EQ(metrics.db_stats[1].tiered_entries, 0u);
}

// MOVE a list whose interior nodes are still io_pending (stash in flight).
TEST_F(ListNodeTieringTest, MoveWhileNodesPending) {
  const int kItems = 6;
  vector<string> expected;
  for (int i = 0; i < kItems; i++) {
    expected.push_back(BuildString(2048, static_cast<char>('a' + i)));
    Run({"RPUSH", "src", expected.back()});
  }

  // MOVE immediately without waiting for stash I/Os to complete.
  EXPECT_THAT(Run({"MOVE", "src", "1"}), IntArg(1));
  EXPECT_THAT(Run({"EXISTS", "src"}), IntArg(0));

  // Drain any in-flight stash I/Os.
  ExpectConditionWithinTimeout([this] { return GetMetrics().tiered_stats.pending_stash_cnt == 0; });

  // Verify the key exists in db 1 with correct length and contents.
  Run({"SELECT", "1"});
  EXPECT_THAT(Run({"LLEN", "src"}), IntArg(kItems));
  for (int i = 0; i < kItems; i++) {
    EXPECT_EQ(Run({"LINDEX", "src", absl::StrCat(i)}), expected[i]) << "index " << i;
  }

  ExpectConditionWithinTimeout([this] { return GetMetrics().tiered_stats.allocated_bytes == 0; });
}

// RPOP exhausts a list from the tail side
TEST_F(ListNodeTieringTest, RPopStashedNodes) {
  const int kItems = 8;
  vector<string> expected;
  for (int i = 0; i < kItems; i++) {
    expected.push_back(BuildString(2048, static_cast<char>('A' + i)));
    Run({"RPUSH", "mylist", expected.back()});
  }

  ExpectConditionWithinTimeout([this] { return GetMetrics().tiered_stats.total_stashes >= 1; });

  // Pop from tail → values arrive in reverse order.
  for (int i = kItems - 1; i >= 0; i--) {
    EXPECT_EQ(Run({"RPOP", "mylist"}), expected[i]) << "pop index " << i;
  }

  EXPECT_THAT(Run({"EXISTS", "mylist"}), IntArg(0));
}

TEST_F(ListNodeTieringTest, PushAfterOffloadedNodePromotedToTail) {
  // Allow appending into an existing tail listpack after it is promoted by LTRIM.
  SetFlag(&FLAGS_list_max_listpack_size, -2);

  const int kItems = 10;
  for (int i = 0; i < kItems; i++) {
    Run({"RPUSH", "mylist", BuildString(3000, static_cast<char>('A' + i))});
  }

  ExpectConditionWithinTimeout([this] { return GetMetrics().tiered_stats.total_stashes >= 1; });

  const int kTrimmedItems = 6;
  EXPECT_EQ(Run({"LTRIM", "mylist", "0", "5"}), "OK");

  string appended = BuildString(64, 'z');
  EXPECT_THAT(Run({"RPUSH", "mylist", appended}), IntArg(kTrimmedItems + 1));
  EXPECT_EQ(Run({"RPOP", "mylist"}), appended);
  EXPECT_THAT(Run({"LLEN", "mylist"}), IntArg(kTrimmedItems));
}

// Test MEMORY DECOMMIT COOL command flushes the cool queue
TEST_P(LatentCoolingTSTest, MemoryDecommitCool) {
  absl::FlagSaver saver;
  SetFlag(&FLAGS_tiered_experimental_cooling, true);  // Ensure cooling is enabled
  SetFlag(&FLAGS_tiered_offload_threshold, 0.0f);     // Force offloading
  UpdateFromFlags();

  const int kMin = 256;
  const int kMax = tiering::kPageSize + 10;

  // Create entries that will be cooled down
  for (size_t i = kMin; i < kMax; i++) {
    Run({"SET", absl::StrCat("k", i), BuildString(i)});
  }

  // Wait for entries to be stashed and cooled
  ExpectConditionWithinTimeout(
      [this] { return GetMetrics().tiered_stats.total_stashes >= kMax - kMin - 1; });

  // Get metrics before decommit
  auto metrics_before = GetMetrics();
  ASSERT_GT(metrics_before.tiered_stats.cold_storage_bytes, 0)
      << "Should have cool storage data before decommit";

  // Call MEMORY DECOMMIT COOL
  auto response = Run({"MEMORY", "DECOMMIT", "COOL"});
  ASSERT_EQ(response, "OK");

  // Get metrics after decommit and verify cool queue was flushed
  auto metrics_after = GetMetrics();
  EXPECT_EQ(metrics_after.tiered_stats.cold_storage_bytes, 0)
      << "Cool queue should be flushed after MEMORY DECOMMIT COOL";
}

// bitops commands must work on values that tiering has offloaded to disk.
// PureDiskTSTest makes a stashed value fully external (no cool in-memory copy).

namespace {
// A `len`-byte string (large enough to offload) with the given bytes set.
string MakeBitmap(std::initializer_list<std::pair<size_t, uint8_t>> set_bytes, size_t len = 4096) {
  string s(len, '\0');
  for (auto [idx, val] : set_bytes)
    s[idx] = static_cast<char>(val);
  return s;
}
}  // namespace

TEST_F(PureDiskTSTest, BitopsGetBitOnExternal) {
  Run({"SET", "bm", MakeBitmap({{0, 0x80}, {1, 0x01}})});
  ExpectConditionWithinTimeout([this] { return GetMetrics().tiered_stats.total_stashes >= 1; });

  EXPECT_EQ(1, CheckedInt({"GETBIT", "bm", "0"}));
  EXPECT_EQ(0, CheckedInt({"GETBIT", "bm", "1"}));
  EXPECT_EQ(1, CheckedInt({"GETBIT", "bm", "15"}));
  EXPECT_EQ(0, CheckedInt({"GETBIT", "bm", "100"}));
  EXPECT_EQ(0, CheckedInt({"GETBIT", "bm", "999999"}));
}

TEST_F(PureDiskTSTest, BitopsSetBitOnExternal) {
  Run({"SET", "bm", MakeBitmap({{0, 0x80}})});
  ExpectConditionWithinTimeout([this] { return GetMetrics().tiered_stats.total_stashes >= 1; });

  EXPECT_EQ(0, CheckedInt({"SETBIT", "bm", "5", "1"}));
  EXPECT_EQ(1, CheckedInt({"SETBIT", "bm", "0", "0"}));
  EXPECT_EQ(0, CheckedInt({"GETBIT", "bm", "0"}));
  EXPECT_EQ(1, CheckedInt({"GETBIT", "bm", "5"}));
}

TEST_F(PureDiskTSTest, BitopsBitCountOnExternal) {
  Run({"SET", "bm", MakeBitmap({{0, 0xFF}, {1, 0x0F}})});
  ExpectConditionWithinTimeout([this] { return GetMetrics().tiered_stats.total_stashes >= 1; });

  EXPECT_EQ(12, CheckedInt({"BITCOUNT", "bm"}));
  EXPECT_EQ(8, CheckedInt({"BITCOUNT", "bm", "0", "0"}));
  EXPECT_EQ(4, CheckedInt({"BITCOUNT", "bm", "1", "1"}));
}

TEST_F(PureDiskTSTest, BitopsBitPosOnExternal) {
  Run({"SET", "bm", MakeBitmap({{2, 0x40}})});
  ExpectConditionWithinTimeout([this] { return GetMetrics().tiered_stats.total_stashes >= 1; });

  EXPECT_EQ(17, CheckedInt({"BITPOS", "bm", "1"}));
  EXPECT_EQ(0, CheckedInt({"BITPOS", "bm", "0"}));
}

TEST_F(PureDiskTSTest, BitopsBitOpAndOnExternal) {
  Run({"SET", "k1", MakeBitmap({{0, 0xFF}})});
  Run({"SET", "k2", MakeBitmap({{0, 0x0F}})});
  ExpectConditionWithinTimeout([this] { return GetMetrics().tiered_stats.total_stashes >= 2; });

  EXPECT_EQ(4096, CheckedInt({"BITOP", "AND", "dest", "k1", "k2"}));
  EXPECT_EQ(4, CheckedInt({"BITCOUNT", "dest", "0", "0"}));
}

TEST_F(PureDiskTSTest, BitopsBitOpNotOnExternal) {
  Run({"SET", "k1", MakeBitmap({{0, 0xFF}})});
  ExpectConditionWithinTimeout([this] { return GetMetrics().tiered_stats.total_stashes >= 1; });

  EXPECT_EQ(4096, CheckedInt({"BITOP", "NOT", "dest", "k1"}));
  EXPECT_EQ(0, CheckedInt({"BITCOUNT", "dest", "0", "0"}));
  EXPECT_EQ(8, CheckedInt({"BITCOUNT", "dest", "1", "1"}));
}

TEST_F(PureDiskTSTest, BitopsBitFieldGetOnExternal) {
  Run({"SET", "bm", MakeBitmap({{0, 0xFF}})});
  ExpectConditionWithinTimeout([this] { return GetMetrics().tiered_stats.total_stashes >= 1; });

  EXPECT_THAT(Run({"BITFIELD", "bm", "GET", "u8", "0"}), RespArray(ElementsAre(IntArg(255))));
}

TEST_F(PureDiskTSTest, BitopsBitFieldModifyOnExternal) {
  Run({"SET", "bm", MakeBitmap({{0, 0x00}})});
  ExpectConditionWithinTimeout([this] { return GetMetrics().tiered_stats.total_stashes >= 1; });

  EXPECT_THAT(Run({"BITFIELD", "bm", "SET", "u8", "0", "200"}), RespArray(ElementsAre(IntArg(0))));
  EXPECT_THAT(Run({"BITFIELD", "bm", "INCRBY", "u8", "0", "5"}),
              RespArray(ElementsAre(IntArg(205))));
}

TEST_F(PureDiskTSTest, BitopsBitFieldRoOnExternal) {
  Run({"SET", "bm", MakeBitmap({{0, 0x2A}})});
  ExpectConditionWithinTimeout([this] { return GetMetrics().tiered_stats.total_stashes >= 1; });

  EXPECT_THAT(Run({"BITFIELD_RO", "bm", "GET", "u8", "0"}), RespArray(ElementsAre(IntArg(42))));
}

// INCRBYFLOAT must not GetSlice() an offloaded value. The value is a valid float
// padded to fill a page so it offloads.
TEST_F(PureDiskTSTest, IncrByFloatOnExternal) {
  const string val = "3.5" + string(4093, '0');
  Run({"SET", "num", val});
  ExpectConditionWithinTimeout([this] { return GetMetrics().tiered_stats.total_stashes >= 1; });
  ASSERT_GE(GetMetrics().db_stats[0].tiered_entries, 1u);

  EXPECT_EQ(Run({"INCRBYFLOAT", "num", "1.5"}), "5");
  EXPECT_EQ(Run({"GET", "num"}), "5");
}

// A prior read marks the value touched, so the INCRBYFLOAT read warms it back into
// memory (non-external); Delete must be gated on a fresh IsExternal() re-check.
TEST_F(PureDiskTSTest, IncrByFloatOnExternalWarmedUp) {
  const string val = "3.5" + string(4093, '0');
  Run({"SET", "num", val});
  ExpectConditionWithinTimeout([this] { return GetMetrics().tiered_stats.total_stashes >= 1; });
  ASSERT_GE(GetMetrics().db_stats[0].tiered_entries, 1u);
  Run({"GET", "num"});  // touch: the next read uploads the value

  EXPECT_EQ(Run({"INCRBYFLOAT", "num", "1.5"}), "5");
  EXPECT_EQ(Run({"GET", "num"}), "5");
}

// Memcached APPEND goes through ExtendOrSkip -> ExtendExisting -> GetSlice()
// (unlike RESP APPEND, which is tiered-aware via OpExtend).
TEST_F(PureDiskTSTest, McAppendOnExternal) {
  Run({"SET", "k", string(4096, 'x')});
  ExpectConditionWithinTimeout([this] { return GetMetrics().tiered_stats.total_stashes >= 1; });
  ASSERT_GE(GetMetrics().db_stats[0].tiered_entries, 1u);

  RunMC(MemcacheParser::APPEND, "k", MCArgs{"suffix", 0});
  EXPECT_EQ(Run({"STRLEN", "k"}), 4102);
}

// Memcached PREPEND shares the ExtendOrSkip path and prepends at the front.
TEST_F(PureDiskTSTest, McPrependOnExternal) {
  Run({"SET", "k", string(4096, 'x')});
  ExpectConditionWithinTimeout([this] { return GetMetrics().tiered_stats.total_stashes >= 1; });
  ASSERT_GE(GetMetrics().db_stats[0].tiered_entries, 1u);

  RunMC(MemcacheParser::PREPEND, "k", MCArgs{"pre", 0});
  EXPECT_EQ(Run({"GETRANGE", "k", "0", "2"}), "pre");
  EXPECT_EQ(Run({"STRLEN", "k"}), 4099);
}

// A prior read warms the value in on the APPEND read, so ExtendOrSkip must gate
// Delete on a fresh IsExternal() re-check.
TEST_F(PureDiskTSTest, McAppendOnExternalWarmedUp) {
  Run({"SET", "k", string(4096, 'x')});
  ExpectConditionWithinTimeout([this] { return GetMetrics().tiered_stats.total_stashes >= 1; });
  ASSERT_GE(GetMetrics().db_stats[0].tiered_entries, 1u);
  Run({"GET", "k"});  // touch: the next read uploads the value

  RunMC(MemcacheParser::APPEND, "k", MCArgs{"suffix", 0});
  EXPECT_EQ(Run({"STRLEN", "k"}), 4102);
}

// JSON.GET reads plain string keys with GetString() before parsing.
TEST_F(PureDiskTSTest, JsonGetOnExternalString) {
  const string doc = absl::StrCat("\"", string(4094, 'a'), "\"");
  Run({"SET", "doc", doc});
  ExpectConditionWithinTimeout([this] { return GetMetrics().tiered_stats.total_stashes >= 1; });
  ASSERT_GE(GetMetrics().db_stats[0].tiered_entries, 1u);

  EXPECT_EQ(Run({"JSON.GET", "doc"}), doc);
}

// DEBUG OBJECT COMPRESS reads via a raw prime-table lookup (no warmup) + GetSlice().
TEST_F(PureDiskTSTest, DebugObjectCompressOnExternal) {
  Run({"SET", "k", string(4096, 'x')});
  ExpectConditionWithinTimeout([this] { return GetMetrics().tiered_stats.total_stashes >= 1; });
  ASSERT_GE(GetMetrics().db_stats[0].tiered_entries, 1u);

  auto resp = Run({"DEBUG", "OBJECT", "k", "COMPRESS"});
  EXPECT_THAT(resp.GetString(), testing::HasSubstr("raw_size: 4096"));
}

// HLL reads its value synchronously (GetSlice/GetString), which must handle
// an offloaded value instead of tripping CHECK(!IsExternal()).
TEST_F(PureDiskTSTest, PFCountOnExternal) {
  BuildDenseHll("hll");  // 6000 distinct elements
  ExpectConditionWithinTimeout([this] { return GetMetrics().tiered_stats.total_stashes >= 1; });

  // Estimate is within a few percent; a wrong error->0 mapping would fail here.
  EXPECT_NEAR(CheckedInt({"PFCOUNT", "hll"}), 6000, 600);
}

TEST_F(PureDiskTSTest, PFMergeOnExternal) {
  BuildDenseHll("src1");  // src1:* elements
  BuildDenseHll("src2");  // disjoint src2:* elements
  ExpectConditionWithinTimeout([this] { return GetMetrics().tiered_stats.total_stashes >= 2; });

  EXPECT_EQ(Run({"PFMERGE", "dst", "src1", "src2"}), "OK");
  // Disjoint sources -> merged cardinality is the sum (~12000).
  EXPECT_NEAR(CheckedInt({"PFCOUNT", "dst"}), 12000, 1200);
}

// PFCOUNT touches the HLL, so the PFADD read warms it back into memory; AddToHll
// must gate Delete on a fresh IsExternal() re-check.
TEST_F(PureDiskTSTest, PFAddOnExternalWarmedUp) {
  BuildDenseHll("hll");
  // Wait for the final HLL (not just an intermediate build value) to be offloaded.
  ExpectConditionWithinTimeout([this] { return GetMetrics().db_stats[0].tiered_entries >= 1; });
  Run({"PFCOUNT", "hll"});  // touch: the next read uploads the value

  Run({"PFADD", "hll", "extra-element"});
  EXPECT_NEAR(CheckedInt({"PFCOUNT", "hll"}), 6000, 600);
}

// RunMany squashes the batch (like Connection::SquashPipeline); a squashed read
// suspending for a tiered read while values offload concurrently must not
// corrupt the shared transaction.
TEST_F(PureDiskTSTest, MGetSquashOnExternal) {
  const int kNum = 12;
  const string big(5000, 'A');
  auto val = [&](int i) { return absl::StrCat(big, "-", i); };  // distinct per key
  for (int rep = 0; rep < 200; ++rep) {
    vector<vector<string>> batch;
    for (int i = 0; i < kNum; ++i)
      batch.push_back({"SET", absl::StrCat("k", i), val(i)});
    for (int i = 0; i < kNum; ++i)
      batch.push_back({"GET", absl::StrCat("k", i)});  // second access -> offload
    for (int j = 0; j < 8; ++j) {
      vector<string> mget = {"MGET"};
      for (int i = 0; i < kNum; ++i)
        mget.push_back(absl::StrCat("k", i));
      batch.push_back(std::move(mget));
      for (int i = 0; i < kNum; ++i)
        batch.push_back({"SETNX", absl::StrCat("k", i), "x"});  // skipped, keys exist
    }
    RunMany(batch);
  }

  EXPECT_EQ(Run({"PING"}), "PONG");
  // MGET must return each value in argument order (the reorder path corruption
  // that #7816/#7817 hit would scramble or drop entries).
  vector<string> mget = {"MGET"};
  for (int i = 0; i < kNum; ++i)
    mget.push_back(absl::StrCat("k", i));
  auto vec = Run(absl::MakeSpan(mget)).GetVec();
  ASSERT_EQ(vec.size(), size_t(kNum));
  for (int i = 0; i < kNum; ++i)
    EXPECT_EQ(vec[i], val(i));
}

// PFMERGE overwrites its destination; an offloaded destination must not hit
// CompactObj::SetString's CHECK(!IsExternal()).
TEST_F(PureDiskTSTest, PFMergeIntoExternal) {
  BuildDenseHll("dst");
  BuildDenseHll("src");
  ExpectConditionWithinTimeout([this] { return GetMetrics().tiered_stats.total_stashes >= 2; });

  EXPECT_EQ(Run({"PFMERGE", "dst", "src"}), "OK");
}

// A squashed async command reads its result after the shared transaction was
// reused by a later command; SETNX must not observe that command's status.
TEST_F(PureDiskTSTest, SetNxSquashResult) {
  Run({"RPUSH", "mylist", "a"});  // wrong type for INCR
  for (int rep = 0; rep < 300; ++rep) {
    vector<vector<string>> batch;
    for (int i = 0; i < 8; ++i) {
      batch.push_back({"SETNX", absl::StrCat("k", i), "v"});
      batch.push_back({"INCR", "mylist"});  // leaves WRONG_TYPE
    }
    RunMany(batch);
  }
  EXPECT_EQ(Run({"PING"}), "PONG");
  // SETNX still reports its own outcome, not a neighbour's status.
  EXPECT_EQ(CheckedInt({"SETNX", "fresh", "v"}), 1);
  EXPECT_EQ(CheckedInt({"SETNX", "fresh", "w"}), 0);
}

}  // namespace dfly
