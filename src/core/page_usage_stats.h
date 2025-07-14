// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once
#include <mimalloc/types.h>

#include <vector>

namespace dfly {

enum class CollectPageStats { YES, NO };

class PageUsage {
 public:
  PageUsage(CollectPageStats collect_stats, float ratio);

  bool IsPageForObjectUnderUtilized(void* object);

  bool IsPageForObjectUnderUtilized(mi_heap_t* heap, void* object);

  std::vector<mi_page_usage_stats_t> Stats();

 private:
  bool ConsumePageStats(mi_page_usage_stats_t stats);

  CollectPageStats collect_stats_{CollectPageStats::NO};
  float ratio_;
  std::vector<mi_page_usage_stats_t> page_stats_;
};

}  // namespace dfly
