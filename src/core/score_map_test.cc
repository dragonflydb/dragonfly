// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/score_map.h"

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

}  // namespace dfly
