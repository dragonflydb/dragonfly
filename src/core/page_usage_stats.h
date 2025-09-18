// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/btree_map.h>
#include <mimalloc/types.h>

#include "core/bloom.h"

struct hdr_histogram;

namespace dfly {

enum class CollectPageStats : uint8_t { YES, NO };

struct FilterWithSize {
  FilterWithSize();
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

  void SetForceReallocate(bool force_reallocate) {
    force_reallocate_ = force_reallocate;
  }

 private:
  CollectPageStats collect_stats_{CollectPageStats::NO};
  float threshold_;

  struct UniquePages {
    FilterWithSize pages_scanned{};
    FilterWithSize pages_marked_for_realloc{};
    FilterWithSize pages_full{};
    FilterWithSize pages_reserved_for_malloc{};
    FilterWithSize pages_with_heap_mismatch{};
    FilterWithSize pages_above_threshold{};
    hdr_histogram* page_usage_hist{};

    uint64_t objects_skipped_not_required{0};
    uint64_t objects_skipped_not_supported{0};

    explicit UniquePages();
    ~UniquePages();

    void AddStat(mi_page_usage_stats_t stat);
    CollectedPageStats CollectedStats() const;
  };

  UniquePages unique_pages_;

  // For use in testing, forces reallocate check to always return true
  bool force_reallocate_{false};
};

}  // namespace dfly
