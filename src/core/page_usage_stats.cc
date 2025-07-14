// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/page_usage_stats.h"

extern "C" {
#include <unistd.h>

#include "redis/zmalloc.h"
mi_page_usage_stats_t mi_heap_page_is_underutilized(mi_heap_t* heap, void* p, float ratio);
}

namespace dfly {

PageUsage::PageUsage(CollectPageStats collect_stats, float ratio)
    : collect_stats_{collect_stats}, ratio_{ratio} {
}

bool PageUsage::IsPageForObjectUnderUtilized(void* object) {
  mi_page_usage_stats_t stats;
  zmalloc_page_is_underutilized(object, ratio_, &stats);
  return ConsumePageStats(stats);
}

bool PageUsage::IsPageForObjectUnderUtilized(mi_heap_t* heap, void* object) {
  return ConsumePageStats(mi_heap_page_is_underutilized(heap, object, ratio_));
}

std::vector<mi_page_usage_stats_t> PageUsage::Stats() {
  return std::move(page_stats_);
}

const PageScanStats& PageUsage::Summary() const {
  return summary_;
}

bool PageUsage::ConsumePageStats(mi_page_usage_stats_t page_stat) {
  summary_.pages_scanned++;

  const bool should_realloc = page_stat.should_realloc;
  // The page is not full, not head of page queue and not heap mismatched, but still not
  // considered for reallocation
  const bool will_not_realloc = !should_realloc && !page_stat.is_malloc_page &&
                                !page_stat.heap_mismatch && !page_stat.is_full;
  if (will_not_realloc) {
    summary_.pages_not_considered_for_realloc++;
  }

  if (collect_stats_ == CollectPageStats::YES) {
    page_stats_.push_back(page_stat);
    summary_.page_sizes.insert(page_stat.block_size);
    if (will_not_realloc) {
      summary_.data_for_page_not_realloc[page_stat.block_size] = page_stat;
    }
  }
  return should_realloc;
}

}  // namespace dfly
