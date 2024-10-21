// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/score_map.h"

#include <mimalloc.h>

#include "base/gtest.h"
#include "base/logging.h"
#include "core/mi_memory_resource.h"

extern "C" {
#include "redis/zmalloc.h"
}

using namespace std;

namespace dfly {

class ScoreMapTest : public ::testing::Test {
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

  ScoreMapTest() : mi_alloc_(mi_heap_get_backing()) {
  }

  void SetUp() override {
    sm_.reset(new ScoreMap(&mi_alloc_));
  }

  void TearDown() override {
    sm_.reset();
    EXPECT_EQ(zmalloc_used_memory_tl, 0);
  }

  MiMemoryResource mi_alloc_;
  std::unique_ptr<ScoreMap> sm_;
};

TEST_F(ScoreMapTest, Basic) {
  EXPECT_TRUE(sm_->AddOrUpdate("foo", 5).second);
  EXPECT_EQ(5, sm_->Find("foo"));

  auto it = sm_->begin();
  EXPECT_STREQ("foo", it->first);
  EXPECT_EQ(5, it->second);
  ++it;

  EXPECT_TRUE(it == sm_->end());

  for (const auto& k_v : *sm_) {
    EXPECT_STREQ("foo", k_v.first);
    EXPECT_EQ(5, k_v.second);
  }

  size_t sz = sm_->ObjMallocUsed();
  EXPECT_FALSE(sm_->AddOrUpdate("foo", 17).second);
  EXPECT_EQ(sm_->ObjMallocUsed(), sz);

  it = sm_->begin();
  EXPECT_EQ(17, it->second);

  EXPECT_FALSE(sm_->AddOrSkip("foo", 31).second);
  EXPECT_EQ(17, it->second);
}

TEST_F(ScoreMapTest, EmptyFind) {
  EXPECT_EQ(nullopt, sm_->Find("bar"));
}

uint64_t total_wasted_memory = 0;

TEST_F(ScoreMapTest, ReallocIfNeeded) {
  auto build_str = [](size_t i) { return to_string(i) + string(131, 'a'); };

  auto count_waste = [](const mi_heap_t* heap, const mi_heap_area_t* area, void* block,
                        size_t block_size, void* arg) {
    size_t used = block_size * area->used;
    total_wasted_memory += area->committed - used;
    return true;
  };

  for (size_t i = 0; i < 10'000; i++) {
    sm_->AddOrUpdate(build_str(i), i);
  }

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

  ASSERT_EQ(sm_->UpperBoundSize(), 1000);
  for (size_t i = 0; i < 1000; i++) {
    auto res = sm_->Find(build_str(i * 10));
    ASSERT_EQ(res.has_value(), true);
    ASSERT_EQ((size_t)*res, i * 10);
  }
}

}  // namespace dfly
