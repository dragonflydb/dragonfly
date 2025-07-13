// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/page_usage_stats.h"

extern "C" {
#include "redis/zmalloc.h"
mi_page_usage_stats_t mi_heap_page_is_underutilized(mi_heap_t* heap, void* p, float ratio);
}

namespace dfly {

PageUsage::PageUsage(CollectStats collect_stats, float ratio)
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

bool PageUsage::ConsumePageStats(mi_page_usage_stats_t stats) {
  const bool should_reallocate = stats.should_realloc;
  if (collect_stats_ == CollectStats::YES) {
    page_stats_.push_back(stats);
  }
  return should_reallocate;
}

}  // namespace dfly
