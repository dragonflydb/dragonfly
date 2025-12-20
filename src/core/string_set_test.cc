// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/string_set.h"

#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>
#include <mimalloc.h>

#include <algorithm>
#include <memory_resource>
#include <random>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

#include "base/gtest.h"
#include "base/logging.h"
#include "core/compact_object.h"
#include "core/page_usage/page_usage_stats.h"
#include "redis/sds.h"

extern "C" {
#include "redis/zmalloc.h"
}

namespace dfly {

using namespace std;
using absl::StrCat;

class DenseSetAllocator : public PMR_NS::memory_resource {
 public:
  bool all_freed() const {
    return alloced_ == 0;
  }

  void* do_allocate(size_t bytes, size_t alignment) override {
    alloced_ += bytes;
    void* p = PMR_NS::new_delete_resource()->allocate(bytes, alignment);
    return p;
  }

  void do_deallocate(void* p, size_t bytes, size_t alignment) override {
    alloced_ -= bytes;
    return PMR_NS::new_delete_resource()->deallocate(p, bytes, alignment);
  }

  bool do_is_equal(const PMR_NS::memory_resource& other) const noexcept override {
    return PMR_NS::new_delete_resource()->is_equal(other);
  }

 private:
  size_t alloced_ = 0;
};

class StringSetTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    auto* tlh = mi_heap_get_backing();
    init_zmalloc_threadlocal(tlh);
    InitTLStatelessAllocMR(PMR_NS::get_default_resource());
  }

  static void TearDownTestSuite() {
  }

  void SetUp() override {
    ss_ = new StringSet;
    generator_.seed(0);
  }

  void TearDown() override {
    delete ss_;

    // ensure there are no memory leaks after every test
    EXPECT_TRUE(alloc_.all_freed());
    EXPECT_EQ(zmalloc_used_memory_tl, 0);
  }

  StringSet* ss_;
  DenseSetAllocator alloc_;
  mt19937 generator_;
};

TEST_F(StringSetTest, Basic) {
  EXPECT_TRUE(ss_->Add("foo"sv));
  EXPECT_TRUE(ss_->Add("bar"sv));
  EXPECT_FALSE(ss_->Add("foo"sv));
  EXPECT_FALSE(ss_->Add("bar"sv));
  EXPECT_TRUE(ss_->Contains("foo"sv));
  EXPECT_TRUE(ss_->Contains("bar"sv));
  EXPECT_EQ(2, ss_->UpperBoundSize());
}

TEST_F(StringSetTest, StandardAddErase) {
  EXPECT_TRUE(ss_->Add("@@@@@@@@@@@@@@@@"));
  EXPECT_TRUE(ss_->Add("A@@@@@@@@@@@@@@@"));
  EXPECT_TRUE(ss_->Add("AA@@@@@@@@@@@@@@"));
  EXPECT_TRUE(ss_->Add("AAA@@@@@@@@@@@@@"));
  EXPECT_TRUE(ss_->Add("AAAAAAAAA@@@@@@@"));
  EXPECT_TRUE(ss_->Add("AAAAAAAAAA@@@@@@"));
  EXPECT_TRUE(ss_->Add("AAAAAAAAAAAAAAA@"));
  EXPECT_TRUE(ss_->Add("AAAAAAAAAAAAAAAA"));
  EXPECT_TRUE(ss_->Add("AAAAAAAAAAAAAAAD"));
  EXPECT_TRUE(ss_->Add("BBBBBAAAAAAAAAAA"));
  EXPECT_TRUE(ss_->Add("BBBBBBBBAAAAAAAA"));
  EXPECT_TRUE(ss_->Add("CCCCCBBBBBBBBBBB"));

  // Remove link in the middle of chain
  EXPECT_TRUE(ss_->Erase("BBBBBBBBAAAAAAAA"));
  // Remove start of a chain
  EXPECT_TRUE(ss_->Erase("CCCCCBBBBBBBBBBB"));
  // Remove end of link
  EXPECT_TRUE(ss_->Erase("AAA@@@@@@@@@@@@@"));
  // Remove only item in chain
  EXPECT_TRUE(ss_->Erase("AA@@@@@@@@@@@@@@"));
  EXPECT_TRUE(ss_->Erase("AAAAAAAAA@@@@@@@"));
  EXPECT_TRUE(ss_->Erase("AAAAAAAAAA@@@@@@"));
  EXPECT_TRUE(ss_->Erase("AAAAAAAAAAAAAAA@"));
}

TEST_F(StringSetTest, DisplacedBug) {
  string_view vals[] = {"imY", "OVl", "NhH", "BCe", "YDL", "lpb",
                        "nhF", "xod", "zYR", "PSa", "hce", "cTR"};
  ss_->AddMany(absl::MakeSpan(vals), UINT32_MAX, false);

  ss_->Add("fIc");
  ss_->Erase("YDL");
  ss_->Add("fYs");
  ss_->Erase("hce");
  ss_->Erase("nhF");
  ss_->Add("dye");
  ss_->Add("xZT");
  ss_->Add("LVK");
  ss_->Erase("zYR");
  ss_->Erase("fYs");
  ss_->Add("ueB");
  ss_->Erase("PSa");
  ss_->Erase("OVl");
  ss_->Add("cga");
  ss_->Add("too");
  ss_->Erase("ueB");
  ss_->Add("HZe");
  ss_->Add("oQn");
  ss_->Erase("too");
  ss_->Erase("HZe");
  ss_->Erase("xZT");
  ss_->Erase("cga");
  ss_->Erase("cTR");
  ss_->Erase("BCe");
  ss_->Add("eua");
  ss_->Erase("lpb");
  ss_->Add("OXK");
  ss_->Add("QmO");
  ss_->Add("SzV");
  ss_->Erase("QmO");
  ss_->Add("jbe");
  ss_->Add("BPN");
  ss_->Add("OfH");
  ss_->Add("Muf");
  ss_->Add("CwP");
  ss_->Erase("Muf");
  ss_->Erase("xod");
  ss_->Add("Cis");
  ss_->Add("Xvd");
  ss_->Erase("SzV");
  ss_->Erase("eua");
  ss_->Add("DGb");
  ss_->Add("leD");
  ss_->Add("MVX");
  ss_->Add("HPq");
}

static string random_string(mt19937& rand, unsigned len) {
  const string_view alpanum = "1234567890abcdefghijklmnopqrstuvwxyz";
  string ret;
  ret.reserve(len);

  for (size_t i = 0; i < len; ++i) {
    ret += alpanum[rand() % alpanum.size()];
  }

  return ret;
}

TEST_F(StringSetTest, Resizing) {
  constexpr size_t num_strs = 4096;
  unordered_set<string> strs;
  while (strs.size() != num_strs) {
    auto str = random_string(generator_, 10);
    strs.insert(str);
  }

  unsigned size = 0;
  for (auto it = strs.begin(); it != strs.end(); ++it) {
    const auto& str = *it;
    EXPECT_TRUE(ss_->Add(str, 1));
    EXPECT_EQ(ss_->UpperBoundSize(), size + 1);

    // make sure we haven't lost any items after a grow
    // which happens every power of 2
    if ((size & (size - 1)) == 0) {
      for (auto j = strs.begin(); j != it; ++j) {
        const auto& str = *j;
        auto it = ss_->Find(str);
        ASSERT_TRUE(it != ss_->end());
        EXPECT_TRUE(it.HasExpiry());
        EXPECT_EQ(it.ExpiryTime(), ss_->time_now() + 1);
      }
    }
    ++size;
  }
}

TEST_F(StringSetTest, SimpleScan) {
  unordered_set<string_view> info = {"foo", "bar"};
  unordered_set<string_view> seen;

  for (auto str : info) {
    EXPECT_TRUE(ss_->Add(str));
  }

  uint32_t cursor = 0;
  do {
    cursor = ss_->Scan(cursor, [&](const sds ptr) {
      sds s = (sds)ptr;
      string_view str{s, sdslen(s)};
      EXPECT_TRUE(info.count(str));
      seen.insert(str);
    });
  } while (cursor != 0);

  EXPECT_TRUE(seen.size() == info.size() && equal(seen.begin(), seen.end(), info.begin()));
}

// Ensure REDIS scan guarantees are met
TEST_F(StringSetTest, ScanGuarantees) {
  unordered_set<string_view> to_be_seen = {"foo", "bar"};
  unordered_set<string_view> not_be_seen = {"AAA", "BBB"};
  unordered_set<string_view> maybe_seen = {"AA@@@@@@@@@@@@@@", "AAA@@@@@@@@@@@@@",
                                           "AAAAAAAAA@@@@@@@", "AAAAAAAAAA@@@@@@"};
  unordered_set<string_view> seen;

  auto scan_callback = [&](const sds ptr) {
    sds s = (sds)ptr;
    string_view str{s, sdslen(s)};
    EXPECT_TRUE(to_be_seen.count(str) || maybe_seen.count(str));
    EXPECT_FALSE(not_be_seen.count(str));
    if (to_be_seen.count(str)) {
      seen.insert(str);
    }
  };

  EXPECT_EQ(ss_->Scan(0, scan_callback), 0);

  for (auto str : not_be_seen) {
    EXPECT_TRUE(ss_->Add(str));
  }

  for (auto str : not_be_seen) {
    EXPECT_TRUE(ss_->Erase(str));
  }

  for (auto str : to_be_seen) {
    EXPECT_TRUE(ss_->Add(str));
  }

  // should reach at least the first item in the set
  uint32_t cursor = ss_->Scan(0, scan_callback);

  for (auto str : maybe_seen) {
    EXPECT_TRUE(ss_->Add(str));
  }

  while (cursor != 0) {
    cursor = ss_->Scan(cursor, scan_callback);
  }

  EXPECT_TRUE(seen.size() == to_be_seen.size());
}

TEST_F(StringSetTest, IntOnly) {
  constexpr size_t num_ints = 8192;
  unordered_set<unsigned int> numbers;
  for (size_t i = 0; i < num_ints; ++i) {
    numbers.insert(i);
    EXPECT_TRUE(ss_->Add(to_string(i)));
  }

  for (size_t i = 0; i < num_ints; ++i) {
    ASSERT_FALSE(ss_->Add(to_string(i)));
  }

  size_t num_remove = generator_() % 4096;
  unordered_set<string> removed;

  for (size_t i = 0; i < num_remove; ++i) {
    auto remove_int = generator_() % num_ints;
    auto remove = to_string(remove_int);
    if (numbers.count(remove_int)) {
      ASSERT_TRUE(ss_->Contains(remove)) << remove_int;
      EXPECT_TRUE(ss_->Erase(remove));
      numbers.erase(remove_int);
    } else {
      EXPECT_FALSE(ss_->Erase(remove));
    }

    EXPECT_FALSE(ss_->Contains(remove));
    removed.insert(remove);
  }

  size_t expected_seen = 0;
  auto scan_callback = [&](const sds ptr) {
    string str{ptr, sdslen(ptr)};
    EXPECT_FALSE(removed.count(str));

    if (numbers.count(atoi(str.data()))) {
      ++expected_seen;
    }
  };

  uint32_t cursor = 0;
  do {
    cursor = ss_->Scan(cursor, scan_callback);
    // randomly throw in some new numbers
    uint32_t val = generator_();
    VLOG(1) << "Val " << val;
    ss_->Add(to_string(val));
  } while (cursor != 0);

  EXPECT_GE(expected_seen + removed.size(), num_ints);
}

TEST_F(StringSetTest, XtremeScanGrow) {
  unordered_set<string> to_see, force_grow, seen;

  while (to_see.size() != 8) {
    to_see.insert(random_string(generator_, 10));
  }

  while (force_grow.size() != 8192) {
    string str = random_string(generator_, 10);

    if (to_see.count(str)) {
      continue;
    }

    force_grow.insert(random_string(generator_, 10));
  }

  for (auto& str : to_see) {
    EXPECT_TRUE(ss_->Add(str));
  }

  auto scan_callback = [&](const sds ptr) {
    sds s = (sds)ptr;
    string_view str{s, sdslen(s)};
    if (to_see.count(string(str))) {
      seen.insert(string(str));
    }
  };

  uint32_t cursor = ss_->Scan(0, scan_callback);

  // force approx 10 grows
  for (auto& s : force_grow) {
    EXPECT_TRUE(ss_->Add(s));
  }

  while (cursor != 0) {
    cursor = ss_->Scan(cursor, scan_callback);
  }

  EXPECT_EQ(seen.size(), to_see.size());
}

TEST_F(StringSetTest, Pop) {
  constexpr size_t num_items = 8;
  unordered_set<string> to_insert;

  while (to_insert.size() != num_items) {
    auto str = random_string(generator_, 10);
    if (to_insert.count(str)) {
      continue;
    }

    to_insert.insert(str);
    EXPECT_TRUE(ss_->Add(str));
  }

  while (!ss_->Empty()) {
    size_t size = ss_->UpperBoundSize();
    auto str = ss_->Pop();
    DCHECK(ss_->UpperBoundSize() == to_insert.size() - 1);
    DCHECK(str.has_value());
    DCHECK(to_insert.count(str.value()));
    DCHECK_EQ(ss_->UpperBoundSize(), size - 1);
    to_insert.erase(str.value());
  }

  DCHECK(ss_->Empty());
  DCHECK(to_insert.empty());
}

TEST_F(StringSetTest, Iteration) {
  ss_->Add("foo");
  for (const sds ptr : *ss_) {
    LOG(INFO) << ptr;
  }
  ss_->Clear();
  constexpr size_t num_items = 8192;
  unordered_set<string> to_insert;

  while (to_insert.size() != num_items) {
    auto str = random_string(generator_, 10);
    if (to_insert.count(str)) {
      continue;
    }

    to_insert.insert(str);
    EXPECT_TRUE(ss_->Add(str));
  }

  for (const sds ptr : *ss_) {
    string str{ptr, sdslen(ptr)};
    EXPECT_TRUE(to_insert.count(str));
    to_insert.erase(str);
  }

  EXPECT_EQ(to_insert.size(), 0);
}

TEST_F(StringSetTest, SetFieldExpireHasExpiry) {
  EXPECT_TRUE(ss_->Add("k1", 100));
  auto k = ss_->Find("k1");
  EXPECT_TRUE(k.HasExpiry());
  EXPECT_EQ(k.ExpiryTime(), 100);
  k.SetExpiryTime(1);
  EXPECT_TRUE(k.HasExpiry());
  EXPECT_EQ(k.ExpiryTime(), 1);
}

TEST_F(StringSetTest, SetFieldExpireNoHasExpiry) {
  EXPECT_TRUE(ss_->Add("k1"));
  auto k = ss_->Find("k1");
  EXPECT_FALSE(k.HasExpiry());
  k.SetExpiryTime(10);
  EXPECT_TRUE(k.HasExpiry());
  EXPECT_EQ(k.ExpiryTime(), 10);
}

TEST_F(StringSetTest, Ttl) {
  EXPECT_TRUE(ss_->Add("bla"sv, 1));
  EXPECT_FALSE(ss_->Add("bla"sv, 1));
  auto it = ss_->Find("bla"sv);
  EXPECT_EQ(1u, it.ExpiryTime());

  ss_->set_time(1);
  EXPECT_TRUE(ss_->Add("bla"sv, 1));
  EXPECT_EQ(1u, ss_->UpperBoundSize());

  for (unsigned i = 0; i < 100; ++i) {
    EXPECT_TRUE(ss_->Add(StrCat("foo", i), 1));
  }
  EXPECT_EQ(101u, ss_->UpperBoundSize());
  it = ss_->Find("foo50");
  EXPECT_STREQ("foo50", *it);
  EXPECT_EQ(2u, it.ExpiryTime());

  ss_->set_time(2);
  for (unsigned i = 0; i < 100; ++i) {
    EXPECT_TRUE(ss_->Add(StrCat("bar", i)));
  }
  it = ss_->Find("bar50");
  EXPECT_FALSE(it.HasExpiry());

  for (auto it = ss_->begin(); it != ss_->end(); ++it) {
    ASSERT_TRUE(absl::StartsWith(*it, "bar")) << *it;
    string str = *it;
    VLOG(1) << *it;
  }
}

TEST_F(StringSetTest, Grow) {
  for (size_t j = 0; j < 10; ++j) {
    for (size_t i = 0; i < 4098; ++i) {
      ss_->Reserve(generator_() % 256);
      auto str = random_string(generator_, 3);
      ss_->Add(str);
    }
    ss_->Clear();
  }
}

TEST_F(StringSetTest, Reserve) {
  vector<string> strs;

  for (size_t i = 0; i < 10; ++i) {
    strs.push_back(random_string(generator_, 10));
    ss_->Add(strs.back());
  }

  for (size_t j = 2; j < 20; j += 3) {
    ss_->Reserve(j * 20);
    for (size_t i = 0; i < 10; ++i) {
      ASSERT_TRUE(ss_->Contains(strs[i]));
    }
  }
}

TEST_F(StringSetTest, Fill) {
  for (size_t i = 0; i < 100; ++i) {
    ss_->Add(random_string(generator_, 10));
  }
  StringSet s2;
  ss_->Fill(&s2);
  EXPECT_EQ(s2.UpperBoundSize(), ss_->UpperBoundSize());
  for (sds str : *ss_) {
    EXPECT_TRUE(s2.Contains(str));
  }
}

TEST_F(StringSetTest, ClearResetsObjMallocUsed) {
  // Add some items
  for (size_t i = 0; i < 100; ++i) {
    ss_->Add(random_string(generator_, 10));
  }

  // Verify ObjMallocUsed() > 0 after adding items
  EXPECT_GT(ss_->ObjMallocUsed(), 0u);
  EXPECT_GT(ss_->UpperBoundSize(), 0u);

  // Clear the set
  ss_->Clear();

  // Verify ObjMallocUsed() is reset to 0 after Clear
  EXPECT_EQ(ss_->ObjMallocUsed(), 0u);
  EXPECT_EQ(ss_->UpperBoundSize(), 0u);
}

TEST_F(StringSetTest, IterateEmpty) {
  for (const auto& s : *ss_) {
    // We're iterating to make sure there is no crash. However, if we got here, it's a bug
    CHECK(false) << "Found entry " << s << " in empty set";
  }
}

size_t memUsed(StringSet& obj) {
  return obj.ObjMallocUsed() + obj.SetMallocUsed();
}

void BM_Clone(benchmark::State& state) {
  vector<string> strs;
  mt19937 generator(0);
  StringSet ss1, ss2;
  unsigned elems = state.range(0);
  for (size_t i = 0; i < elems; ++i) {
    string str = random_string(generator, 10);
    ss1.Add(str);
  }
  ss2.Reserve(ss1.UpperBoundSize());
  while (state.KeepRunning()) {
    for (auto src : ss1) {
      ss2.Add(src);
    }
    state.PauseTiming();
    ss2.Clear();
    ss2.Reserve(ss1.UpperBoundSize());
    state.ResumeTiming();
  }
}
BENCHMARK(BM_Clone)->ArgName("elements")->Arg(32000);

void BM_Fill(benchmark::State& state) {
  unsigned elems = state.range(0);
  vector<string> strs;
  mt19937 generator(0);
  StringSet ss1, ss2;
  for (size_t i = 0; i < elems; ++i) {
    string str = random_string(generator, 10);
    ss1.Add(str);
  }

  while (state.KeepRunning()) {
    ss1.Fill(&ss2);
    state.PauseTiming();
    ss2.Clear();
    state.ResumeTiming();
  }
}
BENCHMARK(BM_Fill)->ArgName("elements")->Arg(32000);

void BM_Clear(benchmark::State& state) {
  unsigned elems = state.range(0);
  mt19937 generator(0);
  StringSet ss;
  while (state.KeepRunning()) {
    state.PauseTiming();
    for (size_t i = 0; i < elems; ++i) {
      string str = random_string(generator, 16);
      ss.Add(str);
    }
    state.ResumeTiming();
    ss.Clear();
  }
}
BENCHMARK(BM_Clear)->ArgName("elements")->Arg(32000);

void BM_Add(benchmark::State& state) {
  vector<string> strs;
  mt19937 generator(0);
  StringSet ss;
  unsigned elems = state.range(0);
  unsigned keySize = state.range(1);
  for (size_t i = 0; i < elems; ++i) {
    string str = random_string(generator, keySize);
    strs.push_back(str);
  }
  ss.Reserve(elems);
  while (state.KeepRunning()) {
    for (auto& str : strs)
      ss.Add(str);
    state.PauseTiming();
    state.counters["Memory_Used"] = memUsed(ss);
    ss.Clear();
    ss.Reserve(elems);
    state.ResumeTiming();
  }
}
BENCHMARK(BM_Add)
    ->ArgNames({"elements", "Key Size"})
    ->ArgsProduct({{1000, 10000, 100000}, {10, 100, 1000}});

void BM_AddMany(benchmark::State& state) {
  vector<string> strs;
  mt19937 generator(0);
  StringSet ss;
  unsigned elems = state.range(0);
  unsigned keySize = state.range(1);
  for (size_t i = 0; i < elems; ++i) {
    string str = random_string(generator, keySize);
    strs.push_back(str);
  }
  ss.Reserve(elems);
  vector<string_view> svs;
  for (const auto& str : strs) {
    svs.push_back(str);
  }
  while (state.KeepRunning()) {
    ss.AddMany(absl::MakeSpan(svs), UINT32_MAX, false);
    state.PauseTiming();
    CHECK_EQ(ss.UpperBoundSize(), elems);
    state.counters["Memory_Used"] = memUsed(ss);
    ss.Clear();
    ss.Reserve(elems);
    state.ResumeTiming();
  }
}
BENCHMARK(BM_AddMany)
    ->ArgNames({"elements", "Key Size"})
    ->ArgsProduct({{1000, 10000, 100000}, {10, 100, 1000}});

void BM_Erase(benchmark::State& state) {
  std::vector<std::string> strs;
  mt19937 generator(0);
  StringSet ss;
  auto elems = state.range(0);
  auto keySize = state.range(1);
  for (long int i = 0; i < elems; ++i) {
    std::string str = random_string(generator, keySize);
    strs.push_back(str);
    ss.Add(str);
  }
  state.counters["Memory_Before_Erase"] = memUsed(ss);
  while (state.KeepRunning()) {
    for (auto& str : strs) {
      ss.Erase(str);
    }
    state.PauseTiming();
    state.counters["Memory_After_Erase"] = memUsed(ss);
    for (auto& str : strs) {
      ss.Add(str);
    }
    state.ResumeTiming();
  }
}
BENCHMARK(BM_Erase)
    ->ArgNames({"elements", "Key Size"})
    ->ArgsProduct({{1000, 10000, 100000}, {10, 100, 1000}});

void BM_Get(benchmark::State& state) {
  std::vector<std::string> strs;
  mt19937 generator(0);
  StringSet ss;
  auto elems = state.range(0);
  auto keySize = state.range(1);
  for (long int i = 0; i < elems; ++i) {
    std::string str = random_string(generator, keySize);
    strs.push_back(str);
    ss.Add(str);
  }
  while (state.KeepRunning()) {
    for (auto& str : strs) {
      ss.Find(str);
    }
  }
}
BENCHMARK(BM_Get)
    ->ArgNames({"elements", "Key Size"})
    ->ArgsProduct({{1000, 10000, 100000}, {10, 100, 1000}});

void BM_Grow(benchmark::State& state) {
  vector<string> strs;
  mt19937 generator(0);
  StringSet src;
  unsigned elems = 1 << 18;
  for (size_t i = 0; i < elems; ++i) {
    src.Add(random_string(generator, 16), UINT32_MAX);
    strs.push_back(random_string(generator, 16));
  }

  while (state.KeepRunning()) {
    state.PauseTiming();
    StringSet tmp;
    src.Fill(&tmp);
    CHECK_EQ(tmp.BucketCount(), elems);
    state.ResumeTiming();
    for (const auto& str : strs) {
      tmp.Add(str);
      if (tmp.BucketCount() > elems) {
        break;  // we grew
      }
    }

    CHECK_GT(tmp.BucketCount(), elems);
  }
}
BENCHMARK(BM_Grow);

void BM_Spop1000(benchmark::State& state) {
  mt19937 generator(0);
  StringSet src;
  unsigned elems = 1 << 14;
  for (size_t i = 0; i < elems; ++i) {
    src.Add(random_string(generator, 16), UINT32_MAX);
  }

  auto sparseness = state.range(0);
  while (state.KeepRunning()) {
    state.PauseTiming();
    StringSet tmp;
    src.Fill(&tmp);
    tmp.Reserve(elems * sparseness);
    state.ResumeTiming();
    for (int i = 0; i < 1000; ++i) {
      tmp.Pop();
    }
  }
}
BENCHMARK(BM_Spop1000)->ArgName("sparseness")->ArgsProduct({{1, 4, 10}});

unsigned total_wasted_memory = 0;

TEST_F(StringSetTest, ReallocIfNeeded) {
  auto build_str = [](size_t i) { return to_string(i) + string(131, 'a'); };

  auto count_waste = [](const mi_heap_t* heap, const mi_heap_area_t* area, void* block,
                        size_t block_size, void* arg) {
    size_t used = block_size * area->used;
    total_wasted_memory += area->committed - used;
    return true;
  };

  for (size_t i = 0; i < 10'000; i++)
    ss_->Add(build_str(i));

  for (size_t i = 0; i < 10'000; i++) {
    if (i % 10 == 0)
      continue;
    ss_->Erase(build_str(i));
  }

  mi_heap_collect(mi_heap_get_backing(), true);
  mi_heap_visit_blocks(mi_heap_get_backing(), false, count_waste, nullptr);
  size_t wasted_before = total_wasted_memory;

  size_t underutilized = 0;
  PageUsage page_usage{CollectPageStats::NO, 0.9};
  for (auto it = ss_->begin(); it != ss_->end(); ++it) {
    underutilized += page_usage.IsPageForObjectUnderUtilized(*it);
    it.ReallocIfNeeded(&page_usage);
  }
  // Check there are underutilized pages
  CHECK_GT(underutilized, 0u);

  total_wasted_memory = 0;
  mi_heap_collect(mi_heap_get_backing(), true);
  mi_heap_visit_blocks(mi_heap_get_backing(), false, count_waste, nullptr);
  size_t wasted_after = total_wasted_memory;

  // Check we waste significanlty less now
  EXPECT_GT(wasted_before, wasted_after * 2);

  EXPECT_EQ(ss_->UpperBoundSize(), 1000);
  for (size_t i = 0; i < 1000; i++)
    EXPECT_EQ(*ss_->Find(build_str(i * 10)), build_str(i * 10));
}

TEST_F(StringSetTest, TransferTTLFlagLinkToObjectOnDelete) {
  for (size_t i = 0; i < 10; i++) {
    EXPECT_TRUE(ss_->Add(absl::StrCat(i), 1));
  }
  for (size_t i = 0; i < 9; i++) {
    EXPECT_TRUE(ss_->Erase(absl::StrCat(i)));
  }
  auto it = ss_->Find("9"sv);
  EXPECT_TRUE(it.HasExpiry());
  EXPECT_EQ(1u, it.ExpiryTime());
}

class ShrinkTest : public StringSetTest, public ::testing::WithParamInterface<size_t> {};

TEST_P(ShrinkTest, BasicShrink) {
  constexpr size_t num_strs = 1000000;
  size_t shrink_to = GetParam();

  vector<string> strs;
  for (size_t i = 0; i < num_strs; ++i) {
    strs.push_back(random_string(generator_, 10));
    EXPECT_TRUE(ss_->Add(strs.back()));
  }

  // Grow to a larger size
  ss_->Reserve(1 << 22);
  size_t original_bucket_count = ss_->BucketCount();
  EXPECT_EQ(original_bucket_count, 1u << 22);

  // Shrink to the parameterized size
  ss_->Shrink(shrink_to);

  EXPECT_EQ(ss_->BucketCount(), shrink_to);
  EXPECT_EQ(ss_->UpperBoundSize(), num_strs);

  // Verify all elements are still accessible
  for (const auto& str : strs) {
    EXPECT_TRUE(ss_->Contains(str)) << "Missing: " << str;
  }
}

INSTANTIATE_TEST_SUITE_P(ShrinkSizes, ShrinkTest,
                         ::testing::Values(1u << 21,   // 2M buckets (sparse)
                                           1u << 20,   // 1M buckets (~1 per bucket)
                                           1u << 19),  // 512K buckets (~2 per bucket)
                         [](const auto& info) { return absl::StrCat("buckets_", info.param); });

TEST_F(StringSetTest, ShrinkWithTTL) {
  constexpr size_t num_strs = 1000000;

  // Track elements by their TTL category
  vector<string> expired_strs;    // TTL 1-50, will expire
  vector<string> surviving_strs;  // TTL 51-100, will survive
  vector<string> no_ttl_strs;     // No TTL, will survive

  for (size_t i = 0; i < num_strs; ++i) {
    string str = random_string(generator_, 10);
    if (i % 3 == 0) {
      // No TTL
      EXPECT_TRUE(ss_->Add(str));
      no_ttl_strs.push_back(str);
    } else if (i % 3 == 1) {
      // TTL 1-50 (will expire when time=50)
      uint32_t ttl = (i % 50) + 1;
      EXPECT_TRUE(ss_->Add(str, ttl));
      expired_strs.push_back(str);
    } else {
      // TTL 51-100 (will survive when time=50)
      uint32_t ttl = (i % 50) + 51;
      EXPECT_TRUE(ss_->Add(str, ttl));
      surviving_strs.push_back(str);
    }
  }

  // Grow to larger size
  ss_->Reserve(1 << 22);

  // Set time to 50 - this will expire elements with TTL <= 50
  ss_->set_time(50);

  // Shrink
  ss_->Shrink(1 << 21);
  EXPECT_EQ(ss_->BucketCount(), 1u << 21);

  // Verify expired elements are gone
  for (const auto& str : expired_strs) {
    EXPECT_EQ(ss_->Find(str), ss_->end()) << "Should be expired: " << str;
  }

  // Verify surviving TTL elements are still accessible with correct TTL
  for (const auto& str : surviving_strs) {
    auto it = ss_->Find(str);
    ASSERT_NE(it, ss_->end()) << "Missing surviving TTL element: " << str;
    EXPECT_TRUE(it.HasExpiry());
    EXPECT_GT(it.ExpiryTime(), 50u);
  }

  // Verify no-TTL elements are still accessible
  for (const auto& str : no_ttl_strs) {
    auto it = ss_->Find(str);
    ASSERT_NE(it, ss_->end()) << "Missing no-TTL element: " << str;
    EXPECT_FALSE(it.HasExpiry());
  }
}

TEST_F(StringSetTest, ScanWithShrinkBetweenCalls) {
  // Test that cursor-based scanning works correctly when Grow and Shrink happen between Scan calls
  // This verifies SCAN guarantees: elements present at start and end of scan must be seen
  constexpr size_t num_strs = 1000000;
  vector<string> strs;
  unordered_set<string> must_see;

  // Add elements and track them
  for (size_t i = 0; i < num_strs; ++i) {
    strs.push_back(random_string(generator_, 10));
    EXPECT_TRUE(ss_->Add(strs.back()));
    must_see.insert(strs.back());
  }

  // Note initial bucket count (will be ~1M after adding 1M elements)
  size_t initial_bucket_count = ss_->BucketCount();

  unordered_set<string> seen;
  auto scan_callback = [&](const sds ptr) {
    string str{ptr, sdslen(ptr)};
    seen.insert(str);
  };

  // Start scanning BEFORE Grow
  uint32_t cursor = ss_->Scan(0, scan_callback);
  EXPECT_NE(cursor, 0u) << "Should not finish in one iteration";

  // Grow to large size in the middle of scanning
  ss_->Reserve(1 << 22);
  EXPECT_EQ(ss_->BucketCount(), 1u << 22);
  EXPECT_GT(ss_->BucketCount(), initial_bucket_count);

  // Continue scanning a bit after Grow
  cursor = ss_->Scan(cursor, scan_callback);

  // Now Shrink in the middle of scanning - this is the key test
  // Elements that existed at scan start must still be visible
  ss_->Shrink(1 << 21);
  EXPECT_EQ(ss_->BucketCount(), 1u << 21);

  // Continue scanning with the same cursor
  constexpr int max_iterations = 1 << 22;
  int iterations = 0;
  while (cursor != 0 && iterations < max_iterations) {
    cursor = ss_->Scan(cursor, scan_callback);
    iterations++;
  }
  EXPECT_LT(iterations, max_iterations) << "Hit iteration limit";
  EXPECT_EQ(cursor, 0u) << "Scan should complete";

  // Verify all original elements were seen
  for (const auto& str : must_see) {
    EXPECT_TRUE(seen.count(str)) << "Missing element after shrink: " << str;
  }
  EXPECT_EQ(seen.size(), must_see.size()) << "Should see exactly all original elements";
}

}  // namespace dfly
