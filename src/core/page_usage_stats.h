// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once
#include <mimalloc/types.h>

#include <vector>

namespace dfly {

class PageUsage {
 public:
  enum class CollectStats { YES, NO };
  PageUsage(CollectStats collect_stats, float ratio);

  bool IsPageForObjectUnderUtilized(void* object);

  bool IsPageForObjectUnderUtilized(mi_heap_t* heap, void* object);

 private:
  bool ConsumePageStats(mi_page_usage_stats_t stats);

  CollectStats collect_stats_{CollectStats::NO};
  float ratio_;
  std::vector<mi_page_usage_stats_t> page_stats_;
};

}  // namespace dfly
