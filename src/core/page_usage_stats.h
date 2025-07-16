// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <mimalloc/types.h>

#include <vector>

namespace dfly {

enum class CollectPageStats : uint8_t { YES, NO };

struct PageScanStats {
  size_t pages_scanned{0};
  size_t pages_not_considered_for_realloc{0};
  absl::flat_hash_set<size_t> page_sizes;
  absl::flat_hash_map<uintptr_t, mi_page_usage_stats_t> data_for_page_not_realloc;
};

class PageUsage {
 public:
  PageUsage(CollectPageStats collect_stats, float ratio);

  bool IsPageForObjectUnderUtilized(void* object);

  bool IsPageForObjectUnderUtilized(mi_heap_t* heap, void* object);

  std::vector<mi_page_usage_stats_t> Stats();

  const PageScanStats& Summary() const;

 private:
  bool ConsumePageStats(mi_page_usage_stats_t page_stat);

  CollectPageStats collect_stats_{CollectPageStats::NO};
  float ratio_;
  std::vector<mi_page_usage_stats_t> page_stats_;
  PageScanStats summary_;
};

}  // namespace dfly
