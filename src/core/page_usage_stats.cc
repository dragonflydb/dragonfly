// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/page_usage_stats.h"

namespace dfly {

PageUsage::PageUsage(CollectPageStats collect_stats, float threshold)
    : collect_stats_{collect_stats}, threshold_{threshold} {
}
bool PageUsage::ConsumePageStats(mi_page_usage_stats_t page_stat) {
  const bool should_reallocate = page_stat.should_realloc;
  if (collect_stats_ == CollectPageStats::YES) {
    stats_.push_back(page_stat);
  }
  return should_reallocate;
}

}  // namespace dfly
