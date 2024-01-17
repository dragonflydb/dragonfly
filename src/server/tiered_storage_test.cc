// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>

#include "base/flags.h"
#include "base/logging.h"
#include "facade/facade_test.h"
#include "server/test_utils.h"

using namespace std;
using namespace testing;
using absl::SetFlag;
using absl::StrCat;

ABSL_DECLARE_FLAG(string, tiered_prefix);

namespace dfly {

class TieredStorageTest : public BaseFamilyTest {
 protected:
  TieredStorageTest() : BaseFamilyTest() {
    num_threads_ = 1;
  }

  void FillExternalKeys(unsigned count, int val_size = 256);
  void FillKeysWithExpire(unsigned count, int val_size = 256, uint32_t expire = 3);

  static void SetUpTestSuite();
};

void TieredStorageTest::SetUpTestSuite() {
  BaseFamilyTest::SetUpTestSuite();
  SetFlag(&FLAGS_tiered_prefix, "/tmp/spill");

  auto* force_epoll = absl::FindCommandLineFlag("force_epoll");
  if (force_epoll->CurrentValue() == "true") {
    LOG(WARNING) << "Tiered storage only works with io uring, skipping tests.";
    // Exiting directly, as otherwise EngineShardSet will exit with error status.
    exit(0);
  }
}

void TieredStorageTest::FillExternalKeys(unsigned count, int val_size) {
  string val(val_size, 'a');

  unsigned batch_cnt = count / 50;
  for (unsigned i = 0; i < batch_cnt; ++i) {
    vector<string> cmd;
    cmd.push_back("mset");

    for (unsigned j = 0; j < 50; ++j) {
      string key = StrCat("k", i * 50 + j);
      cmd.push_back(key);
      cmd.push_back(val);
    }
    Run(absl::Span<string>{cmd});
  }

  for (unsigned i = batch_cnt * 50; i < count; ++i) {
    Run({"set", StrCat("k", i), val});
  }
}

void TieredStorageTest::FillKeysWithExpire(unsigned count, int val_size, uint32_t expire) {
  string val(val_size, 'a');
  for (unsigned i = 0; i < count; ++i) {
    Run({"set", StrCat("k", i), val, "ex", StrCat(expire)});
  }
}

TEST_F(TieredStorageTest, Basic) {
  FillExternalKeys(5000);
  EXPECT_EQ(5000, CheckedInt({"dbsize"}));

  usleep(20000);  // 20 milliseconds

  Metrics m = GetMetrics();
  unsigned tiered_entries = m.db_stats[0].tiered_entries;

  EXPECT_GT(tiered_entries, 0u);
  string resp = CheckedString({"debug", "object", "k1"});
  EXPECT_THAT(resp, HasSubstr("spill_len"));
  m = GetMetrics();
  ASSERT_EQ(tiered_entries, m.db_stats[0].tiered_entries);

  Run({"del", "k1"});
  m = GetMetrics();
  EXPECT_EQ(m.db_stats[0].tiered_entries, tiered_entries - 1);
}

TEST_F(TieredStorageTest, DelBeforeOffload) {
  FillExternalKeys(100);
  EXPECT_EQ(100, CheckedInt({"dbsize"}));

  usleep(20000);  // 20 milliseconds
  Metrics m = GetMetrics();
  EXPECT_GT(m.db_stats[0].tiered_entries, 0u);
  EXPECT_LT(m.db_stats[0].tiered_entries, 100);

  for (unsigned i = 0; i < 100; ++i) {
    Run({"del", StrCat("k", i)});
  }
  m = GetMetrics();
  EXPECT_EQ(m.db_stats[0].tiered_entries, 0u);

  FillExternalKeys(100);
  usleep(20000);  // 20 milliseconds
  m = GetMetrics();
  EXPECT_GT(m.db_stats[0].tiered_entries, 0u);
  EXPECT_LT(m.db_stats[0].tiered_entries, 100);
}

TEST_F(TieredStorageTest, AddMultiDb) {
  Run({"select", "1"});
  FillExternalKeys(100);
  EXPECT_EQ(100, CheckedInt({"dbsize"}));
  Run({"select", "5"});
  FillExternalKeys(100);
  EXPECT_EQ(100, CheckedInt({"dbsize"}));

  usleep(20000);  // 20 milliseconds

  Metrics m = GetMetrics();
  EXPECT_GT(m.db_stats[1].tiered_entries, 0u);
  EXPECT_LT(m.db_stats[1].tiered_entries, 100);
  EXPECT_GT(m.db_stats[5].tiered_entries, 0u);
  EXPECT_LT(m.db_stats[4].tiered_entries, 100);
}

TEST_F(TieredStorageTest, FlushDBAfterSet) {
  Run({"select", "5"});
  FillExternalKeys(100);
  EXPECT_EQ(100, CheckedInt({"dbsize"}));

  Run({"flushdb"});
  Metrics m = GetMetrics();
  EXPECT_EQ(m.db_stats[5].tiered_entries, 0u);

  FillExternalKeys(100);
  EXPECT_EQ(100, CheckedInt({"dbsize"}));

  usleep(20000);  // 20 milliseconds
  m = GetMetrics();
  EXPECT_GT(m.db_stats[5].tiered_entries, 0u);
  EXPECT_LT(m.db_stats[5].tiered_entries, 100);
}

TEST_F(TieredStorageTest, FlushAllAfterSet) {
  Run({"select", "5"});
  FillExternalKeys(100);
  EXPECT_EQ(100, CheckedInt({"dbsize"}));

  Run({"flushall"});
  Metrics m = GetMetrics();
  EXPECT_EQ(m.db_stats[5].tiered_entries, 0u);

  FillExternalKeys(100);
  EXPECT_EQ(100, CheckedInt({"dbsize"}));

  usleep(20000);  // 20 milliseconds
  m = GetMetrics();
  EXPECT_GT(m.db_stats[5].tiered_entries, 0u);
  EXPECT_LT(m.db_stats[5].tiered_entries, 100);
}

TEST_F(TieredStorageTest, AddBigValues) {
  FillExternalKeys(100, 5000);
  EXPECT_EQ(100, CheckedInt({"dbsize"}));

  Run({"flushall"});
  Metrics m = GetMetrics();
  EXPECT_EQ(m.db_stats[0].tiered_entries, 0u);

  FillExternalKeys(100, 5000);
  EXPECT_EQ(100, CheckedInt({"dbsize"}));

  usleep(20000);  // 20 milliseconds
  m = GetMetrics();
  EXPECT_GT(m.db_stats[0].tiered_entries, 0u);
}

TEST_F(TieredStorageTest, DelBigValues) {
  FillExternalKeys(100, 5000);
  EXPECT_EQ(100, CheckedInt({"dbsize"}));

  for (unsigned i = 0; i < 100; ++i) {
    Run({"del", StrCat("k", i)});
  }
  Metrics m = GetMetrics();
  EXPECT_EQ(m.db_stats[0].tiered_entries, 0u);

  FillExternalKeys(100, 5000);
  EXPECT_EQ(100, CheckedInt({"dbsize"}));

  usleep(20000);  // 20 milliseconds
  m = GetMetrics();
  EXPECT_GT(m.db_stats[0].tiered_entries, 0u);
}

TEST_F(TieredStorageTest, AddBigValuesWithExpire) {
  const int kKeyNum = 10;

  FillKeysWithExpire(kKeyNum, 8000);
  usleep(20000);  // 20 milliseconds

  Metrics m = GetMetrics();
  EXPECT_EQ(m.db_stats[0].tiered_entries, 10);

  for (int i = 0; i < kKeyNum; ++i) {
    auto resp = Run({"ttl", StrCat("k", i)});
    EXPECT_GT(resp.GetInt(), 0);
  }

  m = GetMetrics();
  EXPECT_EQ(m.db_stats[0].tiered_entries, 10);
}

TEST_F(TieredStorageTest, AddSmallValuesWithExpire) {
  const int kKeyNum = 100;

  FillKeysWithExpire(kKeyNum);
  usleep(20000);  // 20 milliseconds

  Metrics m = GetMetrics();
  EXPECT_GT(m.db_stats[0].tiered_entries, 0);

  for (int i = 0; i < kKeyNum; ++i) {
    auto resp = Run({"ttl", StrCat("k", i)});
    EXPECT_GT(resp.GetInt(), 0);
  }
  m = GetMetrics();
  EXPECT_GT(m.db_stats[0].tiered_entries, 0);
}

TEST_F(TieredStorageTest, SetAndExpire) {
  string val(5000, 'a');
  Run({"set", "key", val});
  usleep(20000);  // 20 milliseconds
  Metrics m = GetMetrics();
  EXPECT_EQ(m.db_stats[0].tiered_entries, 1);
  Run({"expire", "key", "3"});
  m = GetMetrics();
  EXPECT_EQ(m.db_stats[0].tiered_entries, 1);

  Run({"set", "key", val});
  usleep(20000);  // 20 milliseconds

  m = GetMetrics();
  EXPECT_EQ(m.db_stats[0].tiered_entries, 0);
  Run({"expire", "key", "3"});
}

TEST_F(TieredStorageTest, SetAndGet) {
  string val1(5000, 'a');
  string val2(5000, 'a');

  Run({"set", "key1", val1});
  Run({"set", "key2", val1});
  usleep(20000);  // 20 milliseconds
  Metrics m = GetMetrics();
  EXPECT_EQ(m.db_stats[0].tiered_entries, 2);
  EXPECT_EQ(m.db_stats[0].obj_memory_usage, 0);

  EXPECT_EQ(Run({"get", "key1"}), val1);
  usleep(20000);  // 20 milliseconds
  m = GetMetrics();
  EXPECT_EQ(m.db_stats[0].tiered_entries, 1);
  EXPECT_GT(m.db_stats[0].obj_memory_usage, 0);

  Run({"set", "key1", val2});
  usleep(20000);  // 20 milliseconds
  m = GetMetrics();
  EXPECT_EQ(m.db_stats[0].tiered_entries, 1);
  EXPECT_GT(m.db_stats[0].obj_memory_usage, 0);

  Run({"set", "key2", val2});
  usleep(20000);  // 20 milliseconds
  m = GetMetrics();
  EXPECT_EQ(m.db_stats[0].tiered_entries, 0);
  EXPECT_GT(m.db_stats[0].obj_memory_usage, 0);

  EXPECT_EQ(Run({"get", "key1"}), val2);
  EXPECT_EQ(Run({"get", "key2"}), val2);

  Run({"set", "key3", val1});
  usleep(20000);  // 20 milliseconds
  m = GetMetrics();
  EXPECT_EQ(m.db_stats[0].tiered_entries, 1);

  Run({"del", "key1"});
  Run({"del", "key2"});
  Run({"del", "key3"});
  m = GetMetrics();
  EXPECT_EQ(m.db_stats[0].tiered_entries, 0);
  EXPECT_EQ(m.db_stats[0].obj_memory_usage, 0);
}

TEST_F(TieredStorageTest, GetValueValidation) {
  string val1(5000, 'a');
  string val2(5000, 'b');

  Run({"set", "key1", val1});
  Run({"set", "key2", val2});
  usleep(20000);  // 20 milliseconds
  Metrics m = GetMetrics();
  EXPECT_EQ(m.db_stats[0].tiered_entries, 2);

  EXPECT_EQ(Run({"get", "key1"}), val1);
  EXPECT_EQ(Run({"get", "key2"}), val2);
  m = GetMetrics();
  EXPECT_EQ(m.db_stats[0].tiered_entries, 0);

  for (unsigned i = 0; i < 100; ++i) {
    string val(100, i);  // small entries
    Run({"set", StrCat("k", i), val});
  }
  usleep(20000);  // 20 milliseconds
  m = GetMetrics();
  EXPECT_GE(m.db_stats[0].tiered_entries, 0);

  for (unsigned i = 0; i < 100; ++i) {
    string val(100, i);  // small entries
    EXPECT_EQ(Run({"get", StrCat("k", i)}), val);
  }
  m = GetMetrics();
  EXPECT_EQ(m.db_stats[0].tiered_entries, 0);
}

}  // namespace dfly
