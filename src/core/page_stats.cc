// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/page_stats.h"

namespace dfly {

PageStatsCollector::PageStatsCollector(bool collect_detailed_stats)
    : collect_detailed_stats_{collect_detailed_stats} {
}

bool PageStatsCollector::ConsumePageStats(mi_page_usage_stats_t stats) {
  const bool should_reallocate = stats.should_realloc;
  if (collect_detailed_stats_) {
    page_stats_.push_back(stats);
  }
  return should_reallocate;
}

}  // namespace dfly
