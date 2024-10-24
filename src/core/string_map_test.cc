// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/string_map.h"

#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>
#include <gtest/gtest.h>
#include <mimalloc.h>

#include <algorithm>
#include <cstddef>
#include <memory_resource>
#include <random>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

#include "core/compact_object.h"
#include "core/mi_memory_resource.h"
#include "glog/logging.h"
#include "redis/sds.h"

extern "C" {
#include "redis/zmalloc.h"
}

namespace dfly {

using namespace std;
using absl::StrCat;

class StringMapTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    auto* tlh = mi_heap_get_backing();
    init_zmalloc_threadlocal(tlh);
  }

  static void TearDownTestSuite() {
    mi_heap_collect(mi_heap_get_backing(), true);

    auto cb_visit = [](const mi_heap_t* heap, const mi_heap_area_t* area, void* block,
                       size_t block_size, void* arg) {
      LOG(ERROR) << "Unfreed allocations: block_size " << block_size
                 << ", allocated: " << area->used * block_size;
      return true;
    };

    mi_heap_visit_blocks(mi_heap_get_backing(), false /* do not visit all blocks*/, cb_visit,
                         nullptr);
  }

  StringMapTest() : mi_alloc_(mi_heap_get_backing()) {
  }

  void SetUp() override {
    sm_.reset(new StringMap(&mi_alloc_));
  }

  void TearDown() override {
    sm_.reset();
    EXPECT_EQ(zmalloc_used_memory_tl, 0);
  }

  MiMemoryResource mi_alloc_;
  std::unique_ptr<StringMap> sm_;
};

TEST_F(StringMapTest, Basic) {
  EXPECT_TRUE(sm_->AddOrUpdate("foo", "bar"));
  EXPECT_TRUE(sm_->Contains("foo"));
  auto it = sm_->Find("foo");
  EXPECT_STREQ("bar", it->second);

  it = sm_->begin();
  EXPECT_STREQ("foo", it->first);
  EXPECT_STREQ("bar", it->second);
  ++it;
  EXPECT_TRUE(it == sm_->end());

  for (const auto& k_v : *sm_) {
    EXPECT_STREQ("foo", k_v.first);
    EXPECT_STREQ("bar", k_v.second);
  }

  size_t sz = sm_->ObjMallocUsed();
  EXPECT_FALSE(sm_->AddOrUpdate("foo", "baraaaaaaaaaaaa2"));
  EXPECT_GT(sm_->ObjMallocUsed(), sz);
  it = sm_->begin();
  EXPECT_STREQ("baraaaaaaaaaaaa2", it->second);

  EXPECT_FALSE(sm_->AddOrSkip("foo", "bar2"));
  EXPECT_STREQ("baraaaaaaaaaaaa2", it->second);
}

TEST_F(StringMapTest, EmptyFind) {
  sm_->Find("bar");
}

TEST_F(StringMapTest, Ttl) {
  EXPECT_TRUE(sm_->AddOrUpdate("bla", "val1", 1));
  EXPECT_FALSE(sm_->AddOrUpdate("bla", "val2", 1));
  sm_->set_time(1);
  EXPECT_TRUE(sm_->AddOrUpdate("bla", "val2", 1));
  EXPECT_EQ(1u, sm_->UpperBoundSize());

  EXPECT_FALSE(sm_->AddOrSkip("bla", "val3", 2));

  // set ttl to 2, meaning that the key will expire at time 3.
  EXPECT_TRUE(sm_->AddOrSkip("bla2", "val3", 2));
  EXPECT_TRUE(sm_->Contains("bla2"));

  sm_->set_time(3);
  auto it = sm_->begin();
  EXPECT_TRUE(it == sm_->end());
}

TEST_F(StringMapTest, IterateExpired) {
  EXPECT_TRUE(sm_->AddOrUpdate("k1", "v1", 1));
  EXPECT_TRUE(sm_->AddOrUpdate("k2", "v2", 1));
  sm_->set_time(1);
  auto it = sm_->begin();
  it += 1;
  EXPECT_EQ(it, sm_->end());
}

TEST_F(StringMapTest, SetFieldExpireHasExpiry) {
  EXPECT_TRUE(sm_->AddOrUpdate("k1", "v1", 5));
  auto k = sm_->Find("k1");
  EXPECT_TRUE(k.HasExpiry());
  EXPECT_EQ(k.ExpiryTime(), 5);
  k.SetExpiryTime(1);
  EXPECT_TRUE(k.HasExpiry());
  EXPECT_EQ(k.ExpiryTime(), 1);
}

TEST_F(StringMapTest, SetFieldExpireNoHasExpiry) {
  EXPECT_TRUE(sm_->AddOrUpdate("k1", "v1"));
  auto k = sm_->Find("k1");
  EXPECT_FALSE(k.HasExpiry());
  k.SetExpiryTime(1);
  EXPECT_TRUE(k.HasExpiry());
  EXPECT_EQ(k.ExpiryTime(), 1);
}

TEST_F(StringMapTest, Bug3973) {
  for (unsigned i = 0; i < 8; i++) {
    EXPECT_TRUE(sm_->AddOrUpdate(to_string(i), "val"));
  }
  for (unsigned i = 0; i < 8; i++) {
    auto k = sm_->Find(to_string(i));
    ASSERT_FALSE(k.HasExpiry());
    k.SetExpiryTime(1);
    EXPECT_EQ(k.ExpiryTime(), 1);
  }
  for (unsigned i = 100; i < 1000; i++) {
    EXPECT_TRUE(sm_->AddOrUpdate(to_string(i), "val"));
  }

  // make sure the first 8 keys have expiry set
  for (unsigned i = 0; i < 8; i++) {
    auto k = sm_->Find(to_string(i));
    ASSERT_TRUE(k.HasExpiry());
    EXPECT_EQ(k.ExpiryTime(), 1);
  }
}

TEST_F(StringMapTest, Bug3984) {
  for (unsigned i = 0; i < 6; i++) {
    EXPECT_TRUE(sm_->AddOrUpdate(to_string(i), "val"));
  }
  for (unsigned i = 0; i < 6; i++) {
    auto k = sm_->Find(to_string(i));
    ASSERT_FALSE(k.HasExpiry());
    k.SetExpiryTime(1);
    EXPECT_EQ(k.ExpiryTime(), 1);
  }

  for (unsigned i = 0; i < 6; i++) {
    EXPECT_FALSE(sm_->AddOrUpdate(to_string(i), "val"));
  }
}

unsigned total_wasted_memory = 0;

TEST_F(StringMapTest, ReallocIfNeeded) {
  auto build_str = [](size_t i) { return to_string(i) + string(131, 'a'); };

  auto count_waste = [](const mi_heap_t* heap, const mi_heap_area_t* area, void* block,
                        size_t block_size, void* arg) {
    size_t used = block_size * area->used;
    total_wasted_memory += area->committed - used;
    return true;
  };

  for (size_t i = 0; i < 10'000; i++)
    sm_->AddOrUpdate(build_str(i), build_str(i + 1), i * 10 + 1);

  for (size_t i = 0; i < 10'000; i++) {
    if (i % 10 == 0)
      continue;
    sm_->Erase(build_str(i));
  }

  mi_heap_collect(mi_heap_get_backing(), true);
  mi_heap_visit_blocks(mi_heap_get_backing(), false, count_waste, nullptr);
  size_t wasted_before = total_wasted_memory;

  size_t underutilized = 0;
  for (auto it = sm_->begin(); it != sm_->end(); ++it) {
    underutilized += zmalloc_page_is_underutilized(it->first, 0.9);
    it.ReallocIfNeeded(0.9);
  }
  // Check there are underutilized pages
  CHECK_GT(underutilized, 0u);

  total_wasted_memory = 0;
  mi_heap_collect(mi_heap_get_backing(), true);
  mi_heap_visit_blocks(mi_heap_get_backing(), false, count_waste, nullptr);
  size_t wasted_after = total_wasted_memory;

  // Check we waste significanlty less now
  EXPECT_GT(wasted_before, wasted_after * 2);

  EXPECT_EQ(sm_->UpperBoundSize(), 1000);
  for (size_t i = 0; i < 1000; i++)
    EXPECT_EQ(sm_->Find(build_str(i * 10))->second, build_str(i * 10 + 1));
}

}  // namespace dfly
