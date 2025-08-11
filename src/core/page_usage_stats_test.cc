// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/page_usage_stats.h"

#include <gmock/gmock-matchers.h>

#include "base/gtest.h"
#include "base/logging.h"
#include "core/compact_object.h"
#include "core/mi_memory_resource.h"
#include "core/score_map.h"
#include "core/small_string.h"
#include "core/sorted_map.h"
#include "core/string_map.h"
#include "core/string_set.h"

extern "C" {
#include "redis/zmalloc.h"
}

using namespace dfly;

class PageUsageStatsTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    init_zmalloc_threadlocal(mi_heap_get_backing());
  }

  static void TearDownTestSuite() {
    mi_heap_collect(mi_heap_get_backing(), true);
    mi_heap_visit_blocks(
        mi_heap_get_backing(), false,
        [](auto*, auto* a, void*, size_t block_sz, void*) {
          LOG(ERROR) << "Unfreed allocations: block_size " << block_sz
                     << ", allocated: " << a->used * block_sz;
          return true;
        },
        nullptr);
  }

  PageUsageStatsTest() : m_(mi_heap_get_backing()) {
  }

  void SetUp() override {
    score_map_ = std::make_unique<ScoreMap>(&m_);
    sorted_map_ = std::make_unique<detail::SortedMap>(&m_);
    string_set_ = std::make_unique<StringSet>(&m_);
    string_map_ = std::make_unique<StringMap>(&m_);
    SmallString::InitThreadLocal(m_.heap());
  }

  void TearDown() override {
    score_map_.reset();
    sorted_map_.reset();
    string_set_.reset();
    string_map_.reset();
    small_string_.Free();
    EXPECT_EQ(zmalloc_used_memory_tl, 0);
  }

  MiMemoryResource m_;
  std::unique_ptr<ScoreMap> score_map_;
  std::unique_ptr<detail::SortedMap> sorted_map_;
  std::unique_ptr<StringSet> string_set_;
  std::unique_ptr<StringMap> string_map_;
  SmallString small_string_{};
  CompactObj c_obj_{};
};

TEST_F(PageUsageStatsTest, Defrag) {
  score_map_->AddOrUpdate("test", 0.1);
  sorted_map_->InsertNew(0.1, "x");
  string_set_->Add("a");
  string_map_->AddOrUpdate("key", "value");
  small_string_.Assign("small-string");

  // INT_TAG, defrag will be skipped
  c_obj_.SetString("1");

  {
    PageUsage p{CollectPageStats::YES, 0.1};
    score_map_->begin().ReallocIfNeeded(&p);
    sorted_map_->DefragIfNeeded(&p);
    string_set_->begin().ReallocIfNeeded(&p);
    string_map_->begin().ReallocIfNeeded(&p);
    small_string_.DefragIfNeeded(&p);
    c_obj_.DefragIfNeeded(&p);

    const auto stats = p.CollectedStats();
    EXPECT_GT(stats.pages_scanned, 0);
    EXPECT_EQ(stats.objects_skipped_not_required, 1);
  }

  {
    PageUsage p{CollectPageStats::NO, 0.1};
    score_map_->begin().ReallocIfNeeded(&p);
    sorted_map_->DefragIfNeeded(&p);
    string_set_->begin().ReallocIfNeeded(&p);
    string_map_->begin().ReallocIfNeeded(&p);
    small_string_.DefragIfNeeded(&p);
    EXPECT_EQ(p.CollectedStats().pages_scanned, 0);
  }
}

TEST_F(PageUsageStatsTest, StatCollection) {
  constexpr auto threshold = 0.5;
  PageUsage p{CollectPageStats::YES, threshold};
  for (size_t i = 0; i < 10000; ++i) {
    p.ConsumePageStats({.page_address = uintptr_t{100000 + i},
                        .block_size = 1,
                        .capacity = 100,
                        .reserved = 100,
                        .used = 65,
                        .flags = 0});
  }

  for (size_t i = 0; i < 2000; ++i) {
    p.ConsumePageStats({.page_address = uintptr_t{200000 + i},
                        .block_size = 1,
                        .capacity = 100,
                        .reserved = 100,
                        .used = 85,
                        .flags = 0});
  }

  for (size_t i = 0; i < 1000; ++i) {
    p.ConsumePageStats({.page_address = uintptr_t{300000 + i},
                        .block_size = 1,
                        .capacity = 100,
                        .reserved = 100,
                        .used = 89,
                        .flags = 0});
  }

  constexpr auto page_count_per_flag = 150;
  // allow for collisions in filter
  constexpr auto expected_min_count = 140;

  auto start = 0;
  for (const uint8_t flag : {MI_DFLY_PAGE_FULL, MI_DFLY_PAGE_USED_FOR_MALLOC, MI_DFLY_HEAP_MISMATCH,
                             MI_DFLY_PAGE_BELOW_THRESHOLD}) {
    for (size_t i = 0; i < page_count_per_flag; ++i) {
      p.ConsumePageStats({.page_address = uintptr_t{start + i},
                          .block_size = 1,
                          .capacity = 100,
                          .reserved = 100,
                          .used = 100,
                          .flags = flag});
    }
    start += page_count_per_flag;
  }

  CollectedPageStats st;
  st.Merge(p.CollectedStats(), 1);

  EXPECT_GT(st.pages_scanned, 12000);
  EXPECT_GT(st.pages_full, expected_min_count);
  EXPECT_GT(st.pages_reserved_for_malloc, expected_min_count);
  EXPECT_GT(st.pages_marked_for_realloc, expected_min_count);

  const auto usage = st.shard_wide_summary;

  EXPECT_EQ(usage.size(), 1);
  EXPECT_TRUE(usage.contains(1));

  const CollectedPageStats::ShardUsageSummary expected{{50, 65}, {90, 85}, {99, 89}};
  EXPECT_EQ(usage.at(1), expected);
}
