// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once
#include <mimalloc/types.h>

#include <vector>

namespace dfly {

class PageStatsCollector {
 public:
  explicit PageStatsCollector(bool collect_detailed_stats);

  bool ConsumePageStats(mi_page_usage_stats_t stats);

 private:
  bool collect_detailed_stats_{false};
  std::vector<mi_page_usage_stats_t> page_stats_;
};

}  // namespace dfly
