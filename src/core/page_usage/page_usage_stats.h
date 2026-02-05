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

class CycleQuota {
 public:
  static constexpr uint64_t kMaxQuota = std::numeric_limits<uint64_t>::max();
  static constexpr uint64_t kDefaultDefragQuota = 150;

  explicit CycleQuota(uint64_t quota_usec);

  // Sets the starting point for the quota to be counted from. Can be called multiple times to reset
  // the quota counter.
  void Arm();

  bool Depleted() const;

  uint64_t UsedCycles() const;

  static CycleQuota Unlimited();

  // Extends the quota by the given amount. If any quota was already left over, it is also retained
  // on top of the newly added quota. For example, if 80 usec was left, and we extend by 50 usec,
  // the task now has 130 usec before the quota will be depleted.
  void Extend(uint64_t quota_usec);

 private:
  explicit CycleQuota(uint64_t quota_cycles, bool /*tag*/);

  uint64_t quota_cycles_;
  uint64_t start_cycles_{0};
};

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
  PageUsage(CollectPageStats collect_stats, float threshold,
            CycleQuota quota = CycleQuota::Unlimited());

  virtual ~PageUsage() = default;

  // Resets the quota timer to split defragmentation into different groups with separate quotas.
  // For example, first defragment objects with a quota and then defragment search indices with the
  // same quota independently.
  void ArmQuotaTimer();

  uint64_t UsedQuotaCycles() const;

  virtual bool IsPageForObjectUnderUtilized(void* object);

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

  bool QuotaDepleted() const;

  void ExtendQuota(uint64_t quota_usec);

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

  CycleQuota quota_;

  // For use in testing, forces reallocate check to always return true
  bool force_reallocate_{false};
};

}  // namespace dfly
