// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/page_usage_stats.h"

extern "C" {
#include <unistd.h>

#include "redis/zmalloc.h"
mi_page_usage_stats_t mi_heap_page_is_underutilized(mi_heap_t* heap, void* p, float ratio,
                                                    bool collect_stats);
}

namespace dfly {

PageUsage::PageUsage(CollectPageStats collect_stats, float threshold)
    : collect_stats_{collect_stats}, threshold_{threshold} {
}

bool PageUsage::IsPageForObjectUnderUtilized(void* object) {
  mi_page_usage_stats_t stat =
      zmalloc_page_is_underutilized(object, threshold_, collect_stats_ == CollectPageStats::YES);
  return ConsumePageStats(stat);
}

bool PageUsage::IsPageForObjectUnderUtilized(mi_heap_t* heap, void* object) {
  return ConsumePageStats(mi_heap_page_is_underutilized(heap, object, threshold_,
                                                        collect_stats_ == CollectPageStats::YES));
}

bool PageUsage::ConsumePageStats(mi_page_usage_stats_t stat) {
  const bool should_reallocate = stat.flags == MI_DFLY_PAGE_BELOW_THRESHOLD;
  if (collect_stats_ == CollectPageStats::YES) {
    stats_.push_back(stat);
  }
  return should_reallocate;
}

}  // namespace dfly
