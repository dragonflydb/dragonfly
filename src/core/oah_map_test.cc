// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/oah_map.h"

#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>
#include <mimalloc.h>

#include <random>
#include <string>

#include "base/gtest.h"
#include "base/logging.h"
#include "core/mi_memory_resource.h"
#include "core/page_usage/page_usage_stats.h"

extern "C" {
#include "redis/zmalloc.h"
}

namespace dfly {

using namespace std;

// Mirrors string_map_test.cc for the OAHMap (OAHTable<OAHPair>) container. The StringMap SdsPair
// iterator (it->first/it->second) maps to the OAHPair accessor (it->Key()/it->Value()), and
// AddOrExchange/Extract return an OwnedOAHPair (RAII, frees on destruction) instead of an SdsEntry.
class OAHMapTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    auto* tlh = mi_heap_get_backing();
    init_zmalloc_threadlocal(tlh);
    InitTLStatelessAllocMR(PMR_NS::get_default_resource());
  }

  static void TearDownTestSuite() {
  }

  void SetUp() override {
    m_ = new OAHMap;
    generator_.seed(0);
  }

  void TearDown() override {
    delete m_;
    EXPECT_EQ(zmalloc_used_memory_tl, 0);
  }

  OAHMap* m_;
  mt19937 generator_;
};

static string random_string(mt19937& rand, unsigned len) {
  const string_view alpanum = "1234567890abcdefghijklmnopqrstuvwxyz";
  string ret;
  ret.reserve(len);
  for (size_t i = 0; i < len; ++i)
    ret += alpanum[rand() % alpanum.size()];
  return ret;
}

TEST_F(OAHMapTest, Basic) {
  EXPECT_TRUE(m_->AddOrUpdate("foo", "bar"));
  EXPECT_TRUE(m_->Contains("foo"));
  auto it = m_->Find("foo");
  EXPECT_EQ(it->Value(), "bar"sv);

  it = m_->begin();
  EXPECT_EQ(it->Key(), "foo"sv);
  EXPECT_EQ(it->Value(), "bar"sv);
  ++it;
  EXPECT_TRUE(it == m_->end());

  for (auto e : *m_) {
    EXPECT_EQ(e.Key(), "foo"sv);
    EXPECT_EQ(e.Value(), "bar"sv);
  }

  size_t sz = m_->ObjMallocUsed();
  EXPECT_FALSE(m_->AddOrUpdate("foo", "baraaaaaaaaaaaa2"));
  EXPECT_GT(m_->ObjMallocUsed(), sz);
  EXPECT_EQ(m_->Find("foo")->Value(), "baraaaaaaaaaaaa2"sv);

  EXPECT_FALSE(m_->AddOrSkip("foo", "bar2"));
  EXPECT_EQ(m_->Find("foo")->Value(), "baraaaaaaaaaaaa2"sv);
}

TEST_F(OAHMapTest, EmptyFind) {
  EXPECT_TRUE(m_->Find("bar") == m_->end());
}

TEST_F(OAHMapTest, Ttl) {
  EXPECT_TRUE(m_->AddOrUpdate("bla", "val1", 1));
  EXPECT_FALSE(m_->AddOrUpdate("bla", "val2", 1));
  m_->set_time(1);
  EXPECT_TRUE(m_->AddOrUpdate("bla", "val2", 1));
  EXPECT_EQ(1u, m_->UpperBoundSize());

  EXPECT_FALSE(m_->AddOrSkip("bla", "val3", 2));

  // set ttl to 2, meaning that the key will expire at time 3.
  EXPECT_TRUE(m_->AddOrSkip("bla2", "val3", 2));
  EXPECT_TRUE(m_->Contains("bla2"));

  m_->set_time(3);
  auto it = m_->begin();
  EXPECT_TRUE(it == m_->end());
}

TEST_F(OAHMapTest, IterateExpired) {
  EXPECT_TRUE(m_->AddOrUpdate("k1", "v1", 1));
  EXPECT_TRUE(m_->AddOrUpdate("k2", "v2", 1));
  m_->set_time(1);
  auto it = m_->begin();
  EXPECT_EQ(it, m_->end());
}

TEST_F(OAHMapTest, SetFieldExpireHasExpiry) {
  EXPECT_TRUE(m_->AddOrUpdate("k1", "v1", 5));
  auto k = m_->Find("k1");
  EXPECT_TRUE(k.HasExpiry());
  EXPECT_EQ(k.ExpiryTime(), 5);
  k.SetExpiryTime(1);
  EXPECT_TRUE(k.HasExpiry());
  EXPECT_EQ(k.ExpiryTime(), 1);
}

TEST_F(OAHMapTest, SetFieldExpireNoHasExpiry) {
  EXPECT_TRUE(m_->AddOrUpdate("k1", "v1"));
  auto k = m_->Find("k1");
  EXPECT_FALSE(k.HasExpiry());
  k.SetExpiryTime(1);
  EXPECT_TRUE(k.HasExpiry());
  EXPECT_EQ(k.ExpiryTime(), 1);
}

TEST_F(OAHMapTest, ManyFieldsSetExpiry) {
  for (unsigned i = 0; i < 8; i++)
    EXPECT_TRUE(m_->AddOrUpdate(to_string(i), "val"));
  for (unsigned i = 0; i < 8; i++) {
    auto k = m_->Find(to_string(i));
    ASSERT_FALSE(k.HasExpiry());
    k.SetExpiryTime(1);
    EXPECT_EQ(k.ExpiryTime(), 1);
  }
  for (unsigned i = 100; i < 1000; i++)
    EXPECT_TRUE(m_->AddOrUpdate(to_string(i), "val"));

  // make sure the first 8 keys have expiry set
  for (unsigned i = 0; i < 8; i++) {
    auto k = m_->Find(to_string(i));
    ASSERT_TRUE(k.HasExpiry());
    EXPECT_EQ(k.ExpiryTime(), 1);
  }
}

TEST_F(OAHMapTest, UpdateAfterSetExpiry) {
  for (unsigned i = 0; i < 6; i++)
    EXPECT_TRUE(m_->AddOrUpdate(to_string(i), "val"));
  for (unsigned i = 0; i < 6; i++) {
    auto k = m_->Find(to_string(i));
    ASSERT_FALSE(k.HasExpiry());
    k.SetExpiryTime(1);
    EXPECT_EQ(k.ExpiryTime(), 1);
  }
  for (unsigned i = 0; i < 6; i++)
    EXPECT_FALSE(m_->AddOrUpdate(to_string(i), "val"));
}

TEST_F(OAHMapTest, ExpiryChangesSize) {
  // Value sized so the +4 expiry bytes push the blob across a mimalloc size class (48 -> 64),
  // making the growth observable in ObjMallocUsed.
  const string value(35, 'x');
  m_->AddOrUpdate("field", value);
  const size_t old_size = m_->ObjMallocUsed();

  auto it = m_->Find("field");
  it.SetExpiryTime(1);

  const size_t new_size = m_->ObjMallocUsed();
  EXPECT_LT(old_size, new_size);

  m_->AddOrUpdate("field", value, 1);
  EXPECT_EQ(new_size, m_->ObjMallocUsed());
}

TEST_F(OAHMapTest, ExpiryWithMaxAndKeepTTL) {
  m_->AddOrUpdate("field", "value", 100);
  auto k = m_->Find("field");
  EXPECT_TRUE(k.HasExpiry());
  EXPECT_EQ(k.ExpiryTime(), 100);

  // ttl is copied from prev. if max value is supplied
  m_->AddOrUpdate("field", "value", UINT32_MAX, true);
  k = m_->Find("field");
  EXPECT_TRUE(k.HasExpiry());
  EXPECT_EQ(k.ExpiryTime(), 100);

  // max ttl value results in no expiry without keepttl
  m_->AddOrUpdate("field", "value", UINT32_MAX);
  EXPECT_FALSE(m_->Find("field").HasExpiry());

  // No prev. expiry, supplied ttl_sec value is used
  m_->AddOrUpdate("field", "value", 10, true);
  k = m_->Find("field");
  EXPECT_TRUE(k.HasExpiry());
  EXPECT_EQ(k.ExpiryTime(), 10);

  // object removed while adding due to expiry
  m_->set_time(11);
  m_->AddOrUpdate("field", "value", UINT32_MAX, true);
  k = m_->Find("field");
  EXPECT_FALSE(k.HasExpiry());
}

TEST_F(OAHMapTest, AddOrExchangeNew) {
  // Adding a new field returns an empty owner (no previous entry).
  auto prev = m_->AddOrExchange("f1", "v1");
  EXPECT_FALSE(prev);
  EXPECT_TRUE(m_->Contains("f1"));
  EXPECT_EQ(m_->Find("f1")->Value(), "v1"sv);
}

TEST_F(OAHMapTest, AddOrExchangeReplace) {
  m_->AddOrUpdate("f1", "old_value");
  EXPECT_EQ(m_->UpperBoundSize(), 1u);

  auto prev = m_->AddOrExchange("f1", "new_value");
  ASSERT_TRUE(prev);
  EXPECT_EQ(prev.pair().Value(), "old_value"sv);  // freed automatically on scope exit

  EXPECT_EQ(m_->Find("f1")->Value(), "new_value"sv);
  EXPECT_EQ(m_->UpperBoundSize(), 1u);
}

TEST_F(OAHMapTest, AddOrExchangeWithTtl) {
  m_->AddOrUpdate("f1", "v1", 100);

  auto prev = m_->AddOrExchange("f1", "v2", 200);
  ASSERT_TRUE(prev);
  EXPECT_EQ(prev.pair().Value(), "v1"sv);

  auto it = m_->Find("f1");
  EXPECT_EQ(it->Value(), "v2"sv);
  EXPECT_TRUE(it.HasExpiry());
  EXPECT_EQ(it.ExpiryTime(), 200u);
}

TEST_F(OAHMapTest, GetValue) {
  m_->AddOrUpdate("f1", "v1");
  auto v = m_->GetValue("f1");
  ASSERT_TRUE(v.has_value());
  EXPECT_EQ(*v, "v1"sv);
  EXPECT_FALSE(m_->GetValue("missing").has_value());
}

TEST_F(OAHMapTest, ExtractExisting) {
  m_->AddOrUpdate("f1", "v1");
  m_->AddOrUpdate("f2", "v2");
  EXPECT_EQ(m_->UpperBoundSize(), 2u);

  auto entry = m_->Extract("f1");
  ASSERT_TRUE(entry);
  EXPECT_EQ(entry.pair().Key(), "f1"sv);
  EXPECT_EQ(entry.pair().Value(), "v1"sv);

  EXPECT_EQ(m_->UpperBoundSize(), 1u);
  EXPECT_FALSE(m_->Contains("f1"));
  EXPECT_TRUE(m_->Contains("f2"));
}

TEST_F(OAHMapTest, ExtractNonExisting) {
  m_->AddOrUpdate("f1", "v1");
  auto entry = m_->Extract("no_such_key");
  EXPECT_FALSE(entry);
  EXPECT_EQ(m_->UpperBoundSize(), 1u);
}

TEST_F(OAHMapTest, ExtractMultiple) {
  for (unsigned i = 0; i < 20; i++)
    m_->AddOrUpdate(to_string(i), "val" + to_string(i));
  EXPECT_EQ(m_->UpperBoundSize(), 20u);

  for (unsigned i = 0; i < 20; i += 2) {
    auto entry = m_->Extract(to_string(i));
    ASSERT_TRUE(entry);
    EXPECT_EQ(entry.pair().Value(), ("val" + to_string(i)));
  }

  EXPECT_EQ(m_->UpperBoundSize(), 10u);
  for (unsigned i = 1; i < 20; i += 2)
    EXPECT_TRUE(m_->Contains(to_string(i)));
}

TEST_F(OAHMapTest, RandomPairsUniqueAfterSetExpiryTime) {
  m_->Reserve(1024);
  for (unsigned i = 0; i < 20; i++)
    EXPECT_TRUE(m_->AddOrUpdate(to_string(i), "v"));
  EXPECT_FALSE(m_->ExpirationUsed());

  for (unsigned i = 0; i < 10; i++) {
    auto it = m_->Find(to_string(i));
    ASSERT_FALSE(it.HasExpiry());
    it.SetExpiryTime(1);
  }
  EXPECT_TRUE(m_->ExpirationUsed());

  m_->set_time(2);

  vector<string> keys, vals;
  m_->RandomPairsUnique(20, keys, vals, false);
  EXPECT_EQ(keys.size(), 10u);
}

TEST_F(OAHMapTest, RandomPairsWithValues) {
  for (unsigned i = 0; i < 20; i++)
    m_->AddOrUpdate(to_string(i), "val" + to_string(i));

  vector<string> keys, vals;
  m_->RandomPairsUnique(5, keys, vals, true);
  ASSERT_EQ(keys.size(), 5u);
  ASSERT_EQ(vals.size(), 5u);
  for (size_t i = 0; i < keys.size(); ++i)
    EXPECT_EQ(vals[i], "val" + keys[i]);
}

TEST_F(OAHMapTest, ReallocIfNeeded) {
  auto build_str = [](size_t i) { return to_string(i) + string(131, 'a'); };

  static unsigned total_wasted_memory = 0;
  auto count_waste = [](const mi_heap_t* heap, const mi_heap_area_t* area, void* block,
                        size_t block_size, void* arg) {
    size_t used = block_size * area->used;
    total_wasted_memory += area->committed - used;
    return true;
  };

  for (size_t i = 0; i < 10'000; i++)
    m_->AddOrUpdate(build_str(i), build_str(i + 1), i * 10 + 1);

  for (size_t i = 0; i < 10'000; i++) {
    if (i % 10 == 0)
      continue;
    m_->Erase(build_str(i));
  }

  total_wasted_memory = 0;
  mi_heap_collect(mi_heap_get_backing(), true);
  mi_heap_visit_blocks(mi_heap_get_backing(), false, count_waste, nullptr);
  size_t wasted_before = total_wasted_memory;

  PageUsage page_usage{CollectPageStats::NO, 0.9};
  for (auto it = m_->begin(); it != m_->end(); ++it)
    it.ReallocIfNeeded(&page_usage);

  total_wasted_memory = 0;
  mi_heap_collect(mi_heap_get_backing(), true);
  mi_heap_visit_blocks(mi_heap_get_backing(), false, count_waste, nullptr);
  size_t wasted_after = total_wasted_memory;

  EXPECT_GT(wasted_before, wasted_after * 2);

  EXPECT_EQ(m_->UpperBoundSize(), 1000);
  for (size_t i = 0; i < 1000; i++)
    EXPECT_EQ(m_->Find(build_str(i * 10))->Value(), build_str(i * 10 + 1));
}

// Benchmarks, mirroring oah_set_test.cc adapted to key+value map operations.

static size_t MemUsed(OAHMap& obj) {
  return obj.ObjMallocUsed() + obj.SetMallocUsed();
}

void BM_Clone(benchmark::State& state) {
  mt19937 generator(0);
  OAHMap src, dst;
  unsigned elems = state.range(0);
  unsigned keySize = state.range(1);
  for (size_t i = 0; i < elems; ++i)
    src.AddOrUpdate(random_string(generator, keySize), random_string(generator, keySize));
  dst.Reserve(src.UpperBoundSize());
  while (state.KeepRunning()) {
    for (auto e : src)
      dst.AddOrUpdate(e.Key(), e.Value());
    state.PauseTiming();
    dst.Clear();
    dst.Reserve(src.UpperBoundSize());
    state.ResumeTiming();
  }
}
BENCHMARK(BM_Clone)->ArgNames({"elements", "KeySize"})->ArgsProduct({{32000}, {10, 100, 1000}});

void BM_Clear(benchmark::State& state) {
  unsigned elems = state.range(0);
  unsigned key_size = state.range(1);
  mt19937 generator(0);
  OAHMap m;
  while (state.KeepRunning()) {
    state.PauseTiming();
    for (size_t i = 0; i < elems; ++i)
      m.AddOrUpdate(random_string(generator, key_size), random_string(generator, key_size));
    state.ResumeTiming();
    m.Clear();
  }
}
BENCHMARK(BM_Clear)->ArgNames({"elements", "KeySize"})->ArgsProduct({{32000}, {10, 100, 1000}});

void BM_AddOrUpdate(benchmark::State& state) {
  vector<pair<string, string>> kv;
  mt19937 generator(0);
  OAHMap m;
  unsigned elems = state.range(0);
  unsigned keySize = state.range(1);
  for (size_t i = 0; i < elems; ++i)
    kv.emplace_back(random_string(generator, keySize), random_string(generator, keySize));
  m.Reserve(elems);
  size_t mem_used = 0;
  while (state.KeepRunning()) {
    for (auto& [k, v] : kv)
      m.AddOrUpdate(k, v);
    state.PauseTiming();
    mem_used += MemUsed(m);
    m.Clear();
    m.Reserve(elems);
    state.ResumeTiming();
  }
  state.counters["Memory_Used"] = mem_used / state.iterations();
}
BENCHMARK(BM_AddOrUpdate)
    ->ArgNames({"elements", "KeySize"})
    ->ArgsProduct({{1000, 10000, 100000}, {10, 100, 1000}});

void BM_Erase(benchmark::State& state) {
  vector<pair<string, string>> kv;
  mt19937 generator(0);
  OAHMap m;
  auto elems = state.range(0);
  auto keySize = state.range(1);
  for (long int i = 0; i < elems; ++i) {
    kv.emplace_back(random_string(generator, keySize), random_string(generator, keySize));
    m.AddOrUpdate(kv.back().first, kv.back().second);
  }
  while (state.KeepRunning()) {
    for (auto& [k, v] : kv)
      m.Erase(k);
    state.PauseTiming();
    for (auto& [k, v] : kv)
      m.AddOrUpdate(k, v);
    state.ResumeTiming();
  }
}
BENCHMARK(BM_Erase)
    ->ArgNames({"elements", "KeySize"})
    ->ArgsProduct({{1000, 10000, 100000}, {10, 100, 1000}});

void BM_Get(benchmark::State& state) {
  vector<string> keys;
  mt19937 generator(0);
  OAHMap m;
  auto elems = state.range(0);
  auto keySize = state.range(1);
  for (long int i = 0; i < elems; ++i) {
    string k = random_string(generator, keySize);
    keys.push_back(k);
    m.AddOrUpdate(k, random_string(generator, keySize));
  }
  while (state.KeepRunning()) {
    for (auto& k : keys)
      benchmark::DoNotOptimize(m.Find(k));
  }
}
BENCHMARK(BM_Get)
    ->ArgNames({"elements", "KeySize"})
    ->ArgsProduct({{1000, 10000, 100000}, {10, 100, 1000}});

void BM_GetRandomMember(benchmark::State& state) {
  mt19937 generator(0);
  OAHMap m;
  unsigned elems = state.range(0);
  unsigned keySize = state.range(1);
  for (size_t i = 0; i < elems; ++i)
    m.AddOrUpdate(random_string(generator, keySize), random_string(generator, keySize));

  while (state.KeepRunning())
    benchmark::DoNotOptimize(m.GetRandomMember());
}
BENCHMARK(BM_GetRandomMember)
    ->ArgNames({"elements", "KeySize"})
    ->ArgsProduct({{1000, 10000, 100000}, {10, 100, 1000}});

void BM_Scan(benchmark::State& state) {
  mt19937 generator(0);
  OAHMap m;
  unsigned elems = state.range(0);
  unsigned keySize = state.range(1);
  for (size_t i = 0; i < elems; ++i)
    m.AddOrUpdate(random_string(generator, keySize), random_string(generator, keySize));

  while (state.KeepRunning()) {
    uint32_t cursor = 0;
    size_t seen = 0;
    do {
      cursor = m.Scan(cursor, [&](auto key) {
        benchmark::DoNotOptimize(key.size());
        ++seen;
      });
    } while (cursor != 0);
    benchmark::DoNotOptimize(seen);
  }
}
BENCHMARK(BM_Scan)
    ->ArgNames({"elements", "KeySize"})
    ->ArgsProduct({{1000, 10000, 100000}, {10, 100, 1000}});

void BM_Shrink(benchmark::State& state) {
  mt19937 generator(0);
  unsigned elems = state.range(0);
  unsigned keySize = state.range(1);
  vector<pair<string, string>> kv;
  for (size_t i = 0; i < elems; ++i)
    kv.emplace_back(random_string(generator, keySize), random_string(generator, keySize));

  size_t kShrinkTo = absl::bit_ceil(size_t(elems));
  size_t kGrowTo = kShrinkTo * 4;
  OAHMap m;
  while (state.KeepRunning()) {
    state.PauseTiming();
    m.Clear();
    m.Reserve(kGrowTo);
    for (auto& [k, v] : kv)
      m.AddOrUpdate(k, v);
    CHECK_EQ(m.BucketCount(), kGrowTo);
    state.ResumeTiming();
    m.Shrink(kShrinkTo);
  }
}
BENCHMARK(BM_Shrink)
    ->ArgNames({"elements", "KeySize"})
    ->ArgsProduct({{1000, 10000, 100000}, {10, 100, 1000}});

}  // namespace dfly
