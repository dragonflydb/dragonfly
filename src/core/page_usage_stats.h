// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/btree_map.h>
#include <mimalloc/types.h>

#include "core/bloom.h"
#include "server/tx_base.h"

struct hdr_histogram;

namespace dfly {

enum class CollectPageStats : uint8_t { YES, NO };

struct FilterWithSize {
  SBF sbf;
  size_t size;
  void Add(uintptr_t);
};

struct CollectedPageStats {
  double threshold{0.0};
  uint64_t pages_scanned{0};
  uint64_t pages_marked_for_realloc{0};
  uint64_t pages_full{0};
  uint64_t pages_reserved_for_malloc{0};
  uint64_t pages_with_heap_mismatch{0};
  uint64_t pages_above_threshold{0};

  using ShardUsageSummary = absl::btree_map<uint8_t, uint64_t>;
  ShardUsageSummary page_usage_hist;
  absl::btree_map<ShardId, ShardUsageSummary> shard_wide_summary;

  void Merge(CollectedPageStats&& other, ShardId shard_id);
  static CollectedPageStats Merge(CollectedPageStats* stats, size_t size, float threshold);

  std::string ToString() const;
};

struct UniquePages {
  FilterWithSize pages_scanned;
  FilterWithSize pages_marked_for_realloc;
  FilterWithSize pages_full;
  FilterWithSize pages_reserved_for_malloc;
  FilterWithSize pages_with_heap_mismatch;
  FilterWithSize pages_above_threshold;
  hdr_histogram* page_usage_hist;

  explicit UniquePages();
  ~UniquePages();

  void AddStat(mi_page_usage_stats_t stat);
  CollectedPageStats CollectedStats() const;
};

class PageUsage {
 public:
  PageUsage(CollectPageStats collect_stats, float threshold);

  bool IsPageForObjectUnderUtilized(void* object);

  bool IsPageForObjectUnderUtilized(mi_heap_t* heap, void* object);

  CollectedPageStats CollectedStats() const {
    return unique_pages_.CollectedStats();
  }

 private:
  bool ConsumePageStats(mi_page_usage_stats_t stats);

  CollectPageStats collect_stats_{CollectPageStats::NO};
  float threshold_;

  UniquePages unique_pages_;
};

}  // namespace dfly
