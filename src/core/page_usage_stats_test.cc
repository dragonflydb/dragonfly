// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/page_usage_stats.h"

#include <gmock/gmock-matchers.h>

#include "base/gtest.h"
#include "base/logging.h"
#include "core/mi_memory_resource.h"
#include "core/score_map.h"
#include "core/small_string.h"
#include "core/sorted_map.h"
#include "core/string_map.h"
#include "core/string_set.h"

extern "C" {
#include "redis/zmalloc.h"
}

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
    score_map_ = std::make_unique<dfly::ScoreMap>(&m_);
    sorted_map_ = std::make_unique<dfly::detail::SortedMap>(&m_);
    string_set_ = std::make_unique<dfly::StringSet>(&m_);
    string_map_ = std::make_unique<dfly::StringMap>(&m_);
    dfly::SmallString::InitThreadLocal(m_.heap());
  }

  void TearDown() override {
    score_map_.reset();
    sorted_map_.reset();
    string_set_.reset();
    string_map_.reset();
    small_string_.Free();
    EXPECT_EQ(zmalloc_used_memory_tl, 0);
  }

  dfly::MiMemoryResource m_;
  std::unique_ptr<dfly::ScoreMap> score_map_;
  std::unique_ptr<dfly::detail::SortedMap> sorted_map_;
  std::unique_ptr<dfly::StringSet> string_set_;
  std::unique_ptr<dfly::StringMap> string_map_;
  dfly::SmallString small_string_{};
};

bool PageCannotRealloc(mi_page_usage_stats_t s) {
  const bool page_full = s.flags & MI_DFLY_PAGE_FULL;
  const bool heap_mismatch = s.flags & MI_DFLY_HEAP_MISMATCH;
  const bool malloc_page = s.flags & MI_DFLY_PAGE_USED_FOR_MALLOC;
  return page_full || heap_mismatch || malloc_page;
}

TEST_F(PageUsageStatsTest, Defrag) {
  score_map_->AddOrUpdate("test", 0.1);
  sorted_map_->InsertNew(0.1, "x");
  string_set_->Add("a");
  string_map_->AddOrUpdate("key", "value");
  small_string_.Assign("small-string");

  {
    dfly::PageUsage p{dfly::CollectPageStats::YES, 0.1};
    score_map_->begin().ReallocIfNeeded(&p);
    sorted_map_->DefragIfNeeded(&p);
    string_set_->begin().ReallocIfNeeded(&p);
    string_map_->begin().ReallocIfNeeded(&p);
    small_string_.DefragIfNeeded(&p);

    const auto stats = p.CollectedStats();
    EXPECT_GT(stats.pages_scanned, 0);
  }

  {
    dfly::PageUsage p{dfly::CollectPageStats::NO, 0.1};
    score_map_->begin().ReallocIfNeeded(&p);
    sorted_map_->DefragIfNeeded(&p);
    string_set_->begin().ReallocIfNeeded(&p);
    string_map_->begin().ReallocIfNeeded(&p);
    small_string_.DefragIfNeeded(&p);
    EXPECT_EQ(p.CollectedStats().pages_scanned, 0);
  }
}
