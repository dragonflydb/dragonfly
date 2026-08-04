// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/merge_table.h"

#include <absl/strings/str_cat.h>
#include <mimalloc.h>

#include <algorithm>
#include <bit>
#include <cstring>
#include <random>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_set>
#include <vector>

#include "base/gtest.h"
#include "base/logging.h"
#include "core/mi_memory_resource.h"
#include "core/oah_set.h"

extern "C" {
#include "redis/zmalloc.h"
}

namespace dfly {

using namespace std;

class MergeTableTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    init_zmalloc_threadlocal(mi_heap_get_backing());
    InitTLStatelessAllocMR(PMR_NS::get_default_resource());
  }

  void SetUp() override {
    table_ = new MergeTable;
  }

  void TearDown() override {
    delete table_;
    EXPECT_EQ(zmalloc_used_memory_tl, 0);
  }

  MergeTable* table_ = nullptr;
};

static string MakeKey(size_t id, size_t length) {
  string key(length, 'x');
  const string suffix = absl::StrCat(id);
  assert(suffix.size() <= length);
  memcpy(key.data() + length - suffix.size(), suffix.data(), suffix.size());
  return key;
}

static vector<string> MakeKeys(size_t count, size_t key_size) {
  vector<string> keys;
  keys.reserve(count);
  for (size_t i = 0; i < count; ++i)
    keys.push_back(MakeKey(i, key_size));
  return keys;
}

TEST_F(MergeTableTest, BucketLayoutAndKeyLimit) {
  static_assert(sizeof(MergeTable::Bucket) == 16);

  const string max_key(MergeTable::kMaxKeySize, 'm');
  const string too_long(MergeTable::kMaxKeySize + 1, 'x');

  EXPECT_TRUE(table_->Add(""sv));
  EXPECT_TRUE(table_->Add(max_key));
  EXPECT_FALSE(table_->Add(max_key));
  EXPECT_FALSE(table_->Add(too_long));
  EXPECT_TRUE(table_->Contains(""sv));
  EXPECT_TRUE(table_->Contains(max_key));
  EXPECT_FALSE(table_->Contains(too_long));
  EXPECT_EQ(table_->UpperBoundSize(), 2u);

  vector<string_view> keys = {"batch"sv, too_long, "batch"sv};
  EXPECT_EQ(table_->AddMany(absl::MakeSpan(keys)), 1u);
  EXPECT_TRUE(table_->Contains("batch"));
  EXPECT_EQ(table_->UpperBoundSize(), 3u);
}

TEST_F(MergeTableTest, BasicAddFindErase) {
  EXPECT_TRUE(table_->Add("foo"));
  EXPECT_TRUE(table_->Add("bar"));
  EXPECT_FALSE(table_->Add("foo"));
  EXPECT_EQ(table_->UpperBoundSize(), 2u);

  auto found = table_->Find("foo");
  ASSERT_NE(found, table_->end());
  EXPECT_EQ(*found, "foo");
  EXPECT_TRUE(table_->Contains("bar"));
  EXPECT_FALSE(table_->Contains("missing"));

  EXPECT_TRUE(table_->Erase("foo"));
  EXPECT_FALSE(table_->Erase("foo"));
  EXPECT_FALSE(table_->Contains("foo"));
  EXPECT_TRUE(table_->Contains("bar"));
  EXPECT_EQ(table_->UpperBoundSize(), 1u);
}

TEST_F(MergeTableTest, AsciiPackingAndMaximumKeySize) {
  const string ascii_64(MergeTable::kMaxKeySize, 'a');
  string raw_64(MergeTable::kMaxKeySize, 'b');
  raw_64[31] = static_cast<char>(0x80);
  const string too_long(MergeTable::kMaxKeySize + 1, 'c');

  EXPECT_TRUE(table_->Add(ascii_64));
  EXPECT_TRUE(table_->Add(raw_64));
  EXPECT_FALSE(table_->Add(ascii_64));
  EXPECT_FALSE(table_->Add(too_long));

  auto ascii = table_->Find(ascii_64);
  auto raw = table_->Find(raw_64);
  ASSERT_NE(ascii, table_->end());
  ASSERT_NE(raw, table_->end());
  EXPECT_EQ(*ascii, ascii_64);
  EXPECT_EQ(*raw, raw_64);

  // The hash operates on the packed representation, so a logical-key lookup and insertion use
  // the same representation even at the 64-byte boundary.
  EXPECT_EQ(MergeTable::Hash(ascii_64), MergeTable::Hash(*ascii));

  MergeTable packed;
  MergeTable unpacked;
  ASSERT_TRUE(packed.Add(ascii_64));
  ASSERT_TRUE(unpacked.Add(raw_64));
  EXPECT_LE(packed.ObjMallocUsed(), unpacked.ObjMallocUsed());
}

TEST_F(MergeTableTest, CollisionChainsSurviveInsertionAndErase) {
  table_->Reserve(MergeTable::kMinBucketCount * MergeTable::kBlockCapacity);
  ASSERT_EQ(table_->BucketCount(), MergeTable::kMinBucketCount);

  const uint32_t bucket_log = std::bit_width(table_->BucketCount()) - 1;
  const size_t target_bucket = MergeTable::Hash("chain_seed") >> (64 - bucket_log);
  vector<string> colliding;
  for (size_t i = 0; colliding.size() < 25; ++i) {
    string key = MakeKey(i, 32);
    if ((MergeTable::Hash(key) >> (64 - bucket_log)) == target_bucket)
      colliding.push_back(std::move(key));
  }

  for (const string& key : colliding)
    EXPECT_TRUE(table_->Add(key));
  EXPECT_EQ(table_->UpperBoundSize(), colliding.size());
  for (const string& key : colliding)
    EXPECT_TRUE(table_->Contains(key)) << key;

  unordered_set<string> expected(colliding.begin(), colliding.end());
  for (size_t i = 0; i < colliding.size(); i += 3) {
    EXPECT_TRUE(table_->Erase(colliding[i]));
    expected.erase(colliding[i]);
  }

  EXPECT_EQ(table_->UpperBoundSize(), expected.size());
  for (const string& key : colliding)
    EXPECT_EQ(table_->Contains(key), expected.contains(key)) << key;
}

TEST_F(MergeTableTest, ResizeAtEightEntriesPerBucket) {
  for (size_t i = 0; i < MergeTable::kMinBucketCount * MergeTable::kBlockCapacity; ++i)
    EXPECT_TRUE(table_->Add(MakeKey(i, 24)));

  EXPECT_EQ(table_->BucketCount(), MergeTable::kMinBucketCount);
  EXPECT_TRUE(table_->Add(MakeKey(MergeTable::kMinBucketCount * MergeTable::kBlockCapacity, 24)));
  EXPECT_EQ(table_->BucketCount(), MergeTable::kMinBucketCount * 2);

  constexpr size_t kEntries = 10000;
  for (size_t i = MergeTable::kMinBucketCount * MergeTable::kBlockCapacity + 1; i < kEntries; ++i) {
    EXPECT_TRUE(table_->Add(MakeKey(i, 24)));
  }

  EXPECT_EQ(table_->UpperBoundSize(), kEntries);
  EXPECT_EQ(table_->BucketCount(), 2048u);
  for (size_t i = 0; i < kEntries; ++i)
    EXPECT_TRUE(table_->Contains(MakeKey(i, 24))) << i;
}

TEST_F(MergeTableTest, IterationAndScan) {
  unordered_set<string> expected;
  for (size_t i = 0; i < 512; ++i) {
    string key = MakeKey(i, 64);
    ASSERT_TRUE(table_->Add(key));
    expected.insert(std::move(key));
  }

  unordered_set<string> iterated;
  for (string_view key : *table_)
    iterated.emplace(key);
  EXPECT_EQ(iterated, expected);

  unordered_set<string> scanned;
  uint32_t cursor = 0;
  do {
    cursor = table_->Scan(cursor, [&](string_view key) { scanned.emplace(key); });
  } while (cursor != 0);
  EXPECT_EQ(scanned, expected);

  auto random = table_->GetRandomMember();
  ASSERT_NE(random, table_->end());
  EXPECT_TRUE(expected.contains(string(*random)));
}

TEST_F(MergeTableTest, ScanRemainsValidAcrossRehashes) {
  const vector<string> keys = MakeKeys(1024, 32);
  table_->Reserve(2048 * MergeTable::kBlockCapacity);
  for (const string& key : keys)
    ASSERT_TRUE(table_->Add(key));

  unordered_set<string> seen;
  uint32_t cursor = 0;
  auto scan = [&] { return table_->Scan(cursor, [&](string_view key) { seen.emplace(key); }); };

  cursor = scan();
  ASSERT_NE(cursor, 0u);

  table_->Reserve(4096 * MergeTable::kBlockCapacity);
  cursor = scan();
  table_->Shrink(2048);
  while (cursor != 0)
    cursor = scan();

  EXPECT_EQ(seen, (unordered_set<string>(keys.begin(), keys.end())));
}

TEST_F(MergeTableTest, ReserveShrinkAndFill) {
  vector<string> keys = MakeKeys(1024, 32);
  for (const string& key : keys)
    ASSERT_TRUE(table_->Add(key));

  table_->Reserve(8192);
  ASSERT_EQ(table_->BucketCount(), 1024u);
  table_->Shrink(256);
  EXPECT_EQ(table_->BucketCount(), 256u);
  for (const string& key : keys)
    EXPECT_TRUE(table_->Contains(key)) << key;

  MergeTable copy;
  table_->Fill(&copy);
  EXPECT_EQ(copy.UpperBoundSize(), table_->UpperBoundSize());
  for (const string& key : keys)
    EXPECT_TRUE(copy.Contains(key)) << key;
}

TEST_F(MergeTableTest, TtlAndIteratorExpiry) {
  EXPECT_FALSE(table_->ExpirationUsed());
  EXPECT_TRUE(table_->Add("with_ttl", 100));
  EXPECT_TRUE(table_->ExpirationUsed());

  auto with_ttl = table_->Find("with_ttl");
  ASSERT_NE(with_ttl, table_->end());
  EXPECT_TRUE(with_ttl.HasExpiry());
  EXPECT_EQ(with_ttl.ExpiryTime(), 100u);
  with_ttl.SetExpiryTime(10);
  EXPECT_TRUE(with_ttl.HasExpiry());
  EXPECT_EQ(with_ttl.ExpiryTime(), 10u);

  EXPECT_TRUE(table_->Add("without_ttl"));
  auto without_ttl = table_->Find("without_ttl");
  ASSERT_NE(without_ttl, table_->end());
  EXPECT_FALSE(without_ttl.HasExpiry());
  without_ttl.SetExpiryTime(20);
  EXPECT_TRUE(without_ttl.HasExpiry());
  EXPECT_EQ(without_ttl.ExpiryTime(), 20u);

  // An expired duplicate is reaped and can be inserted again.
  EXPECT_TRUE(table_->Add("reinsert", 1));
  EXPECT_TRUE(table_->Add("permanent"));
  table_->set_time(1);
  EXPECT_TRUE(table_->Add("reinsert", 1));
  EXPECT_EQ(table_->UpperBoundSize(), 4u);
  EXPECT_NE(table_->Find("with_ttl"), table_->end());
  EXPECT_EQ(table_->Find("reinsert").ExpiryTime(), 2u);

  table_->set_time(20);
  EXPECT_EQ(table_->SizeSlow(), 1u);
  EXPECT_TRUE(table_->Contains("permanent"));
}

TEST_F(MergeTableTest, ScanRandomAndShrinkSkipExpiredEntries) {
  EXPECT_TRUE(table_->Add("dead", 1));
  EXPECT_TRUE(table_->Add("alive", 100));
  EXPECT_TRUE(table_->Add("persistent"));
  table_->set_time(50);

  unordered_set<string> seen;
  uint32_t cursor = 0;
  do {
    cursor = table_->Scan(cursor, [&](string_view key) { seen.emplace(key); });
  } while (cursor != 0);
  EXPECT_EQ(seen, (unordered_set<string>{"alive", "persistent"}));
  EXPECT_EQ(table_->UpperBoundSize(), 2u);

  for (size_t i = 0; i < 100; ++i) {
    auto member = table_->GetRandomMember();
    ASSERT_NE(member, table_->end());
    EXPECT_NE(*member, "dead");
  }

  table_->Clear();
  table_->set_time(0);
  vector<string> expired;
  vector<string> live;
  for (size_t i = 0; i < 256; ++i) {
    const string key = MakeKey(i, 32);
    if ((i & 1) == 0) {
      ASSERT_TRUE(table_->Add(key, 1));
      expired.push_back(key);
    } else {
      ASSERT_TRUE(table_->Add(key, 100));
      live.push_back(key);
    }
  }

  table_->Reserve(1024 * MergeTable::kBlockCapacity);
  table_->set_time(50);
  table_->Shrink(MergeTable::kMinBucketCount);
  EXPECT_EQ(table_->BucketCount(), MergeTable::kMinBucketCount);
  EXPECT_EQ(table_->SizeSlow(), live.size());
  for (const string& key : expired)
    EXPECT_FALSE(table_->Contains(key));
  for (const string& key : live) {
    auto it = table_->Find(key);
    ASSERT_NE(it, table_->end());
    EXPECT_TRUE(it.HasExpiry());
    EXPECT_GT(it.ExpiryTime(), 50u);
  }
}

TEST_F(MergeTableTest, AddManyTtlPolicyAndClearStep) {
  EXPECT_TRUE(table_->Add("k1", 100));
  EXPECT_TRUE(table_->Add("k2"));

  string_view members[] = {"k1", "k2", "k3"};
  EXPECT_EQ(table_->AddMany(absl::MakeSpan(members), 200, /*keepttl=*/true), 1u);
  EXPECT_EQ(table_->Find("k1").ExpiryTime(), 100u);
  EXPECT_FALSE(table_->Find("k2").HasExpiry());
  EXPECT_EQ(table_->Find("k3").ExpiryTime(), 200u);

  EXPECT_EQ(table_->AddMany(absl::MakeSpan(members), 300, /*keepttl=*/false), 0u);
  for (string_view key : members) {
    auto it = table_->Find(key);
    ASSERT_NE(it, table_->end());
    EXPECT_TRUE(it.HasExpiry());
    EXPECT_EQ(it.ExpiryTime(), 300u);
  }

  // A no-TTL AddMany leaves existing expiration untouched even with keepttl=false.
  EXPECT_EQ(table_->AddMany(absl::MakeSpan(members), UINT32_MAX, /*keepttl=*/false), 0u);
  EXPECT_EQ(table_->Find("k1").ExpiryTime(), 300u);

  uint32_t cursor = 0;
  while (cursor < table_->Capacity())
    cursor = table_->ClearStep(cursor, 3);
  EXPECT_EQ(table_->UpperBoundSize(), 0u);
  EXPECT_EQ(table_->ObjMallocUsed(), 0u);
  EXPECT_FALSE(table_->ExpirationUsed());
}

TEST_F(MergeTableTest, FillPreservesAsciiAndTtl) {
  const string ascii_64(MergeTable::kMaxKeySize, 'p');
  string raw_64(MergeTable::kMaxKeySize, 'r');
  raw_64[17] = static_cast<char>(0x80);

  table_->set_time(7);
  EXPECT_TRUE(table_->Add(ascii_64, 20));
  EXPECT_TRUE(table_->Add(raw_64));

  MergeTable copy;
  table_->Fill(&copy);
  EXPECT_EQ(copy.time_now(), 7u);
  EXPECT_EQ(copy.UpperBoundSize(), 2u);
  EXPECT_EQ(*copy.Find(ascii_64), ascii_64);
  EXPECT_EQ(*copy.Find(raw_64), raw_64);
  EXPECT_TRUE(copy.Find(ascii_64).HasExpiry());
  EXPECT_EQ(copy.Find(ascii_64).ExpiryTime(), 27u);

  copy.set_time(27);
  EXPECT_FALSE(copy.Contains(ascii_64));
  EXPECT_TRUE(copy.Contains(raw_64));
}

TEST_F(MergeTableTest, RandomOperationsMatchUnorderedSet) {
  vector<string> keys = MakeKeys(1000, 64);
  unordered_set<string> expected;
  mt19937 generator(0);

  for (size_t i = 0; i < 20000; ++i) {
    const string& key = keys[generator() % keys.size()];
    if ((generator() & 1) == 0) {
      EXPECT_EQ(table_->Add(key), expected.insert(key).second) << key;
    } else {
      EXPECT_EQ(table_->Erase(key), expected.erase(key) != 0) << key;
    }

    if (i % 257 == 0) {
      EXPECT_EQ(table_->UpperBoundSize(), expected.size());
      for (const string& expected_key : expected)
        EXPECT_TRUE(table_->Contains(expected_key)) << expected_key;
    }
  }

  EXPECT_EQ(table_->UpperBoundSize(), expected.size());
  for (const string& key : keys)
    EXPECT_EQ(table_->Contains(key), expected.contains(key)) << key;
}

TEST_F(MergeTableTest, ClearReleasesPackedBlocks) {
  for (size_t i = 0; i < 1000; ++i)
    ASSERT_TRUE(table_->Add(MakeKey(i, 48)));

  EXPECT_GT(table_->ObjMallocUsed(), 0u);
  EXPECT_GT(table_->SetMallocUsed(), 0u);
  table_->Clear();
  EXPECT_TRUE(table_->Empty());
  EXPECT_EQ(table_->BucketCount(), 0u);
  EXPECT_EQ(table_->ObjMallocUsed(), 0u);
}

// The benchmarks mirror OAHSet's set operations. Both implementations use identical key sets,
// and all comparison sizes are at or below MergeTable's 64-byte maximum.

template <typename Table> static size_t MemUsed(const Table& table) {
  return table.ObjMallocUsed() + table.SetMallocUsed();
}

template <typename Table> static void BenchClone(benchmark::State& state) {
  const vector<string> keys = MakeKeys(state.range(0), state.range(1));
  Table source;
  Table destination;
  for (const string& key : keys)
    source.Add(key);

  while (state.KeepRunning()) {
    source.Fill(&destination);
    state.PauseTiming();
    destination.Clear();
    state.ResumeTiming();
  }
}

template <typename Table> static void BenchClear(benchmark::State& state) {
  const vector<string> keys = MakeKeys(state.range(0), state.range(1));
  Table table;

  while (state.KeepRunning()) {
    state.PauseTiming();
    for (const string& key : keys)
      table.Add(key);
    state.ResumeTiming();
    table.Clear();
  }
}

template <typename Table> static void BenchAdd(benchmark::State& state) {
  const vector<string> keys = MakeKeys(state.range(0), state.range(1));
  Table table;
  table.Reserve(keys.size());
  size_t memory_used = 0;

  while (state.KeepRunning()) {
    for (const string& key : keys)
      table.Add(key);
    state.PauseTiming();
    memory_used += MemUsed(table);
    table.Clear();
    table.Reserve(keys.size());
    state.ResumeTiming();
  }
  state.counters["Memory_Used"] = memory_used / state.iterations();
}

template <typename Table> static void BenchAddMany(benchmark::State& state) {
  const vector<string> keys = MakeKeys(state.range(0), state.range(1));
  vector<string_view> views;
  views.reserve(keys.size());
  for (const string& key : keys)
    views.push_back(key);

  Table table;
  table.Reserve(keys.size());
  size_t memory_used = 0;
  while (state.KeepRunning()) {
    table.AddMany(absl::MakeSpan(views));
    state.PauseTiming();
    memory_used += MemUsed(table);
    table.Clear();
    table.Reserve(keys.size());
    state.ResumeTiming();
  }
  state.counters["Memory_Used"] = memory_used / state.iterations();
}

template <typename Table> static void BenchErase(benchmark::State& state) {
  const vector<string> keys = MakeKeys(state.range(0), state.range(1));
  Table table;
  for (const string& key : keys)
    table.Add(key);

  state.counters["Memory_Before_Erase"] = MemUsed(table);
  size_t memory_used = 0;
  while (state.KeepRunning()) {
    for (const string& key : keys)
      table.Erase(key);
    state.PauseTiming();
    memory_used += MemUsed(table);
    for (const string& key : keys)
      table.Add(key);
    state.ResumeTiming();
  }
  state.counters["Memory_After_Erase"] = memory_used / state.iterations();
}

template <typename Table> static void BenchGet(benchmark::State& state) {
  const vector<string> keys = MakeKeys(state.range(0), state.range(1));
  Table table;
  for (const string& key : keys)
    table.Add(key);

  while (state.KeepRunning()) {
    for (const string& key : keys)
      benchmark::DoNotOptimize(table.Find(key));
  }
}

template <typename Table> static void BenchGrow(benchmark::State& state) {
  constexpr size_t kElements = 1 << 15;
  constexpr size_t kExtraKeys = 128;
  const vector<string> keys = MakeKeys(kElements, 16);
  vector<string> extra_keys;
  extra_keys.reserve(kExtraKeys);
  for (size_t i = 0; i < kExtraKeys; ++i)
    extra_keys.push_back(MakeKey(kElements + i, 16));
  Table source;
  Table destination;
  for (const string& key : keys)
    source.Add(key);

  while (state.KeepRunning()) {
    state.PauseTiming();
    destination.Clear();
    source.Fill(&destination);
    const size_t before = destination.BucketCount();
    state.ResumeTiming();
    for (const string& key : extra_keys) {
      CHECK(destination.Add(key));
      if (destination.BucketCount() > before)
        break;
    }
    benchmark::DoNotOptimize(destination.BucketCount());
    CHECK_GT(destination.BucketCount(), before);
  }
}

template <typename Table> static void BenchGetRandomMember(benchmark::State& state) {
  const vector<string> keys = MakeKeys(state.range(0), state.range(1));
  Table table;
  for (const string& key : keys)
    table.Add(key);

  while (state.KeepRunning())
    benchmark::DoNotOptimize(table.GetRandomMember());
}

template <typename Table> static void BenchScan(benchmark::State& state) {
  const vector<string> keys = MakeKeys(state.range(0), state.range(1));
  Table table;
  for (const string& key : keys)
    table.Add(key);

  while (state.KeepRunning()) {
    uint32_t cursor = 0;
    size_t seen = 0;
    do {
      cursor = table.Scan(cursor, [&](string_view key) {
        benchmark::DoNotOptimize(key.size());
        ++seen;
      });
    } while (cursor != 0);
    benchmark::DoNotOptimize(seen);
  }
}

template <typename Table> static size_t MembersForBuckets(size_t buckets) {
  if constexpr (is_same_v<Table, MergeTable>) {
    return buckets * MergeTable::kBlockCapacity;
  } else {
    return buckets * OAHSet::kOverloadFactor;
  }
}

template <typename Table> static void BenchShrink(benchmark::State& state) {
  const vector<string> keys = MakeKeys(state.range(0), state.range(1));
  Table source;
  Table destination;
  for (const string& key : keys)
    source.Add(key);

  while (state.KeepRunning()) {
    state.PauseTiming();
    destination.Clear();
    source.Fill(&destination);
    const size_t shrink_to = destination.BucketCount();
    destination.Reserve(MembersForBuckets<Table>(shrink_to * 4));
    state.ResumeTiming();
    destination.Shrink(shrink_to);
  }
}

#define REGISTER_COMPARISON_BENCHMARK(operation)            \
  BENCHMARK_TEMPLATE(Bench##operation, MergeTable)          \
      ->Name("MergeTable/" #operation)                      \
      ->ArgNames({"elements", "key_size"})                  \
      ->ArgsProduct({{1000, 10000, 100000}, {10, 32, 64}}); \
  BENCHMARK_TEMPLATE(Bench##operation, OAHSet)              \
      ->Name("OAHSet/" #operation)                          \
      ->ArgNames({"elements", "key_size"})                  \
      ->ArgsProduct({{1000, 10000, 100000}, {10, 32, 64}})

REGISTER_COMPARISON_BENCHMARK(Clone);
REGISTER_COMPARISON_BENCHMARK(Clear);
REGISTER_COMPARISON_BENCHMARK(Add);
REGISTER_COMPARISON_BENCHMARK(AddMany);
REGISTER_COMPARISON_BENCHMARK(Erase);
REGISTER_COMPARISON_BENCHMARK(Get);
REGISTER_COMPARISON_BENCHMARK(GetRandomMember);
REGISTER_COMPARISON_BENCHMARK(Scan);
REGISTER_COMPARISON_BENCHMARK(Shrink);

BENCHMARK_TEMPLATE(BenchGrow, MergeTable)->Name("MergeTable/Grow");
BENCHMARK_TEMPLATE(BenchGrow, OAHSet)->Name("OAHSet/Grow");

#undef REGISTER_COMPARISON_BENCHMARK

}  // namespace dfly
