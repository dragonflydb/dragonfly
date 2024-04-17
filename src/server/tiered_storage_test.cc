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

ABSL_DECLARE_FLAG(string, tiered_prefix);
ABSL_DECLARE_FLAG(bool, tiered_storage_v2_cache_fetched);

namespace dfly {

class TieredStorageTest : public BaseFamilyTest {
 protected:
  TieredStorageTest() : BaseFamilyTest() {
    num_threads_ = 1;
  }

  void FillExternalKeys(unsigned count, int val_size = 256);
  void FillKeysWithExpire(unsigned count, int val_size = 256, uint32_t expire = 3);
  bool WaitUntilTieredEntriesGT(size_t value, int db_index = 0);
  bool WaitUntilTieredEntriesEQ(size_t value, int db_index = 0);

  static void SetUpTestSuite();
};

class TieredStorageV2Test : public BaseFamilyTest {
 protected:
  TieredStorageV2Test() {
    num_threads_ = 1;
  }

  void SetUp() override {
    // TODO: Use FlagSaver if there is need to run V1 tests after V2
    absl::SetFlag(&FLAGS_tiered_prefix, "");
    absl::SetFlag(&FLAGS_tiered_storage_v2_cache_fetched, true);

    BaseFamilyTest::SetUp();
    auto* shard = shard_set->Await(0, [] { return EngineShard::tlocal(); });
    storage_.emplace(&shard->db_slice());
    shard_set->Await(0, [storage = &*storage_] {
      auto ec = storage->Open(absl::StrCat("/tmp/tiered_storage_test", 1));
      EXPECT_FALSE(ec);
    });
  }

  void TearDown() override {
    shard_set->Await(0, [storage = &*storage_] { storage->Close(); });
    BaseFamilyTest::TearDown();
  }

 public:
  std::optional<TieredStorageV2> storage_;
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

bool TieredStorageTest::WaitUntilTieredEntriesGT(size_t value, int db_index) {
  auto cb = [&, value]() {
    auto tiered_entries = GetMetrics().db_stats[db_index].tiered_entries;
    return tiered_entries > value;
  };
  return WaitUntilCondition(std::move(cb));
}

bool TieredStorageTest::WaitUntilTieredEntriesEQ(size_t value, int db_index) {
  auto cb = [&, value]() {
    auto tiered_entries = GetMetrics().db_stats[db_index].tiered_entries;
    return tiered_entries == value;
  };
  return WaitUntilCondition(std::move(cb));
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

  EXPECT_TRUE(WaitUntilTieredEntriesGT(0));

  Metrics m = GetMetrics();
  unsigned tiered_entries = m.db_stats[0].tiered_entries;

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

  EXPECT_TRUE(WaitUntilTieredEntriesGT(0));
  Metrics m = GetMetrics();
  EXPECT_LT(m.db_stats[0].tiered_entries, 100);

  for (unsigned i = 0; i < 100; ++i) {
    Run({"del", StrCat("k", i)});
  }
  m = GetMetrics();
  EXPECT_EQ(m.db_stats[0].tiered_entries, 0u);

  FillExternalKeys(100);
  EXPECT_TRUE(WaitUntilTieredEntriesGT(0));
  m = GetMetrics();
  EXPECT_LT(m.db_stats[0].tiered_entries, 100);
}

TEST_F(TieredStorageTest, AddMultiDb) {
  Run({"select", "1"});
  FillExternalKeys(100);
  EXPECT_EQ(100, CheckedInt({"dbsize"}));
  Run({"select", "5"});
  FillExternalKeys(100);
  EXPECT_EQ(100, CheckedInt({"dbsize"}));

  EXPECT_TRUE(WaitUntilTieredEntriesGT(0, 1));
  EXPECT_TRUE(WaitUntilTieredEntriesGT(0, 5));

  Metrics m = GetMetrics();
  EXPECT_LT(m.db_stats[1].tiered_entries, 100);
  EXPECT_LT(m.db_stats[5].tiered_entries, 100);
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

  EXPECT_TRUE(WaitUntilTieredEntriesGT(0, 5));
  m = GetMetrics();
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

  EXPECT_TRUE(WaitUntilTieredEntriesGT(0, 5));
  m = GetMetrics();
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

  EXPECT_TRUE(WaitUntilTieredEntriesGT(0));
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

  EXPECT_TRUE(WaitUntilTieredEntriesGT(0));
}

TEST_F(TieredStorageTest, AddBigValuesWithExpire) {
  const int kKeyNum = 10;

  FillKeysWithExpire(kKeyNum, 8000);
  EXPECT_TRUE(WaitUntilTieredEntriesEQ(10));

  for (int i = 0; i < kKeyNum; ++i) {
    auto resp = Run({"ttl", StrCat("k", i)});
    EXPECT_GT(resp.GetInt(), 0);
  }

  Metrics m = GetMetrics();
  EXPECT_EQ(m.db_stats[0].tiered_entries, 10);
}

TEST_F(TieredStorageTest, AddSmallValuesWithExpire) {
  const int kKeyNum = 100;

  FillKeysWithExpire(kKeyNum);
  EXPECT_TRUE(WaitUntilTieredEntriesGT(0));

  for (int i = 0; i < kKeyNum; ++i) {
    auto resp = Run({"ttl", StrCat("k", i)});
    EXPECT_GT(resp.GetInt(), 0);
  }
  Metrics m = GetMetrics();
  EXPECT_GT(m.db_stats[0].tiered_entries, 0);
}

TEST_F(TieredStorageTest, SetAndExpire) {
  string val(5000, 'a');
  Run({"set", "key", val});
  EXPECT_TRUE(WaitUntilTieredEntriesEQ(1));

  Run({"expire", "key", "3"});
  Metrics m = GetMetrics();
  EXPECT_EQ(m.db_stats[0].tiered_entries, 1);

  Run({"set", "key", val});
  EXPECT_TRUE(WaitUntilTieredEntriesEQ(0));

  Run({"expire", "key", "3"});
}

TEST_F(TieredStorageTest, SetAndGet) {
  GTEST_SKIP();
  string val1(5000, 'a');
  string val2(5000, 'a');

  Run({"set", "key1", val1});
  Run({"set", "key2", val1});
  EXPECT_TRUE(WaitUntilTieredEntriesEQ(2));
  Metrics m = GetMetrics();
  EXPECT_EQ(m.db_stats[0].obj_memory_usage, 0);

  EXPECT_EQ(Run({"get", "key1"}), val1);
  EXPECT_TRUE(WaitUntilTieredEntriesEQ(1));
  m = GetMetrics();
  EXPECT_GT(m.db_stats[0].obj_memory_usage, 0);

  Run({"set", "key1", val2});
  EXPECT_TRUE(WaitUntilTieredEntriesEQ(1));
  m = GetMetrics();
  EXPECT_GT(m.db_stats[0].obj_memory_usage, 0);

  Run({"set", "key2", val2});
  EXPECT_TRUE(WaitUntilTieredEntriesEQ(0));
  m = GetMetrics();
  EXPECT_GT(m.db_stats[0].obj_memory_usage, 0);

  EXPECT_EQ(Run({"get", "key1"}), val2);
  EXPECT_EQ(Run({"get", "key2"}), val2);

  Run({"set", "key3", val1});
  EXPECT_TRUE(WaitUntilTieredEntriesEQ(1));

  Run({"del", "key1"});
  Run({"del", "key2"});
  Run({"del", "key3"});
  m = GetMetrics();
  EXPECT_EQ(m.db_stats[0].tiered_entries, 0);
  EXPECT_EQ(m.db_stats[0].obj_memory_usage, 0);
}

TEST_F(TieredStorageTest, GetValueValidation) {
  GTEST_SKIP();
  string val1(5000, 'a');
  string val2(5000, 'b');

  Run({"set", "key1", val1});
  Run({"set", "key2", val2});
  EXPECT_TRUE(WaitUntilTieredEntriesEQ(2));

  EXPECT_EQ(Run({"get", "key1"}), val1);
  EXPECT_EQ(Run({"get", "key2"}), val2);
  Metrics m = GetMetrics();
  EXPECT_EQ(m.db_stats[0].tiered_entries, 0);

  for (unsigned i = 0; i < 100; ++i) {
    string val(100, i);  // small entries
    Run({"set", StrCat("k", i), val});
  }
  EXPECT_TRUE(WaitUntilTieredEntriesGT(0));

  for (unsigned i = 0; i < 100; ++i) {
    string val(100, i);  // small entries
    EXPECT_EQ(Run({"get", StrCat("k", i)}), val);
  }
  m = GetMetrics();
  EXPECT_EQ(m.db_stats[0].tiered_entries, 0);
}

TEST_F(TieredStorageV2Test, SimpleStash) {
  // Create simple values
  vector<pair<string, string>> values(20);
  for (unsigned i = 0; i < values.size(); i++) {
    // 3 kb is above small bins size
    values[i] = {absl::StrCat("key", i), string(3_KB, char('A' + i))};
    Run({"set", values[i].first, values[i].second});
  }

  vector<util::fb2::Future<string>> futures;
  shard_set->Await(0, [this, &values, &futures] {
    auto& db_slice = EngineShard::tlocal()->db_slice();

    // Schedule STASH for values
    for (const auto& [key, _] : values) {
      auto it = db_slice.FindMutable(DbContext{}, key);
      storage_->Stash(key, &it.it->second);
    }

    // Wait for all values to be stashed
    ExpectConditionWithinTimeout([&values, &db_slice] {
      for (auto [key, _] : values)
        if (db_slice.FindMutable(DbContext{}, key).it->second.HasIoPending())
          return false;
      return true;
    });

    // Now read all the values
    for (const auto& [key, _] : values) {
      auto it = db_slice.FindMutable(DbContext{}, key);
      EXPECT_TRUE(it.it->second.IsExternal());
      futures.emplace_back(storage_->Read(key, it.it->second));
    }
  });

  // Wait for futures and assert correct values were read
  for (unsigned i = 0; i < values.size(); i++)
    EXPECT_EQ(futures[i].get(), values[i].second);

  shard_set->Await(0, [&values] {
    auto& db_slice = EngineShard::tlocal()->db_slice();

    // Make sure all values were loaded back to memory
    for (const auto& [key, value] : values) {
      auto it = db_slice.FindMutable(DbContext{}, key);
      std::string buf;
      EXPECT_EQ(it.it->second.GetSlice(&buf), value);
    }
  });
}

}  // namespace dfly
