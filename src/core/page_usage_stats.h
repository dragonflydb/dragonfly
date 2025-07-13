// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once
#include <mimalloc/types.h>

#include <vector>

namespace dfly {

enum class CollectPageStats : uint8_t { YES, NO };

class PageUsage {
 public:
  PageUsage(CollectPageStats collect_stats, float threshold);

  bool ConsumePageStats(mi_page_usage_stats_t page_stat);

 private:
  CollectPageStats collect_stats_{CollectPageStats::NO};
  float threshold_;

  std::vector<mi_page_usage_stats_t> stats_;
};

}  // namespace dfly
