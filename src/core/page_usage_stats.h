// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/btree_map.h>

#define MI_BUILD_RELEASE 1
#include <mimalloc/types.h>

extern "C" {
#include "redis/hyperloglog.h"
}

struct hdr_histogram;

namespace dfly {

enum class CollectPageStats : uint8_t { YES, NO };

struct CollectedPageStats {
  double threshold{0.0};
  uint64_t pages_scanned{0};
  uint64_t pages_marked_for_realloc{0};
  uint64_t pages_full{0};
  uint64_t pages_reserved_for_malloc{0};
  uint64_t pages_with_heap_mismatch{0};
  uint64_t pages_above_threshold{0};
  uint64_t objects_skipped_not_required{0};
  uint64_t objects_skipped_not_supported{0};

  using ShardUsageSummary = absl::btree_map<uint8_t, uint64_t>;
  ShardUsageSummary page_usage_hist;
  absl::btree_map<uint16_t, ShardUsageSummary> shard_wide_summary;

  void Merge(CollectedPageStats&& other, uint16_t shard_id);
  static CollectedPageStats Merge(std::vector<CollectedPageStats>&& stats, float threshold);

  std::string ToString() const;
};

class PageUsage {
 public:
  PageUsage(CollectPageStats collect_stats, float threshold);

  bool IsPageForObjectUnderUtilized(void* object);

  bool IsPageForObjectUnderUtilized(mi_heap_t* heap, void* object);

  CollectedPageStats CollectedStats() const {
    return unique_pages_.CollectedStats();
  }

  bool ConsumePageStats(mi_page_usage_stats_t stats);

  void RecordNotRequired() {
    unique_pages_.objects_skipped_not_required += 1;
  }

  void RecordNotSupported() {
    unique_pages_.objects_skipped_not_supported += 1;
  }

 private:
  CollectPageStats collect_stats_{CollectPageStats::NO};
  float threshold_;

  struct UniquePages {
    HllBufferPtr pages_scanned;
    HllBufferPtr pages_marked_for_realloc;
    HllBufferPtr pages_full;
    HllBufferPtr pages_reserved_for_malloc;
    HllBufferPtr pages_with_heap_mismatch;
    HllBufferPtr pages_above_threshold;
    hdr_histogram* page_usage_hist{};

    uint64_t objects_skipped_not_required{0};
    uint64_t objects_skipped_not_supported{0};

    explicit UniquePages();
    ~UniquePages();

    void AddStat(mi_page_usage_stats_t stat);
    CollectedPageStats CollectedStats() const;
  };

  UniquePages unique_pages_;
};

}  // namespace dfly
