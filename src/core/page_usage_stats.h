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

  bool IsPageForObjectUnderUtilized(void* object);

  bool IsPageForObjectUnderUtilized(mi_heap_t* heap, void* object);

  const std::vector<mi_page_usage_stats_t>& Stats() const {
    return stats_;
  }

 private:
  bool ConsumePageStats(mi_page_usage_stats_t stats);

  CollectPageStats collect_stats_{CollectPageStats::NO};
  float threshold_;

  std::vector<mi_page_usage_stats_t> stats_;
};

}  // namespace dfly
