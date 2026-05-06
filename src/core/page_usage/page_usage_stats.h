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
  // 40000 here is ~10ms of real time because helio's CycleClock mixes raw rdtsc
  // with abseil's shifted frequency, making FromUsec/ToUsec ~4x off on x86.
  // Once the helio bug is fixed, drop this to 10000.
  static constexpr uint64_t kDefaultDefragQuota = 40'000;

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
  // Tag for tagged-dispatch on the IsPageForObjectUnderUtilized hot path.
  // Subclasses set this in their constructor; the dispatch in
  // page_usage_dispatch.h switches on it and inlines into the concrete
  // subclass's *Impl method. Avoids virtual-call + cross-TU-inlining cost.
  //
  // kCustom is a test/benchmark-only escape hatch: the dispatch forwards to
  // the virtual IsPageForObjectUnderUtilizedHook, letting test classes
  // override behavior without a Kind of their own. Production hot paths
  // (kBase, kEvacuator, kCensus) never pay vtable cost.
  enum class Kind : uint8_t { kBase, kEvacuator, kCensus, kCustom };

  PageUsage(CollectPageStats collect_stats, float threshold,
            CycleQuota quota = CycleQuota::Unlimited());

  virtual ~PageUsage() = default;

  // Resets the quota timer to split defragmentation into different groups with separate quotas.
  // For example, first defragment objects with a quota and then defragment search indices with the
  // same quota independently.
  void ArmQuotaTimer();

  uint64_t UsedQuotaCycles() const;

  Kind kind() const {
    return kind_;
  }

  // Non-virtual entry point. Definition lives in page_usage_dispatch.h as an
  // inline switch on kind_ — callers that want this method must include that
  // header. Callers that include only page_usage_stats.h get a link error if
  // they try to use it (intentional: catches missed callers at link time).
  bool IsPageForObjectUnderUtilized(void* object);

  bool IsPageForObjectUnderUtilized(mi_heap_t* heap, void* object);

  // Default-kind (Kind::kBase) implementations used by the dispatch header
  // when no subclass override is selected. Non-virtual; out-of-line in
  // page_usage_stats.cc.
  bool BaseIsPageForObjectUnderUtilized(void* object);
  bool BaseIsPageForObjectUnderUtilized(mi_heap_t* heap, void* object);

  // Optional fast-path filter for the legacy DefragIfNeeded walker. When set,
  // BaseIsPageForObjectUnderUtilized consults the predicate before calling
  // mi_heap_page_is_underutilized: if the predicate returns false, the page
  // is treated as "not underutil" without the mimalloc syscall. Conservative-
  // positive filter — see defrag_underutil::IsPageMaybeUnderutil for semantics.
  // Phased Evacuator/CensusTaker bypass the Base impl and don't consult this.
  using UnderutilFilter = bool (*)(uintptr_t page_addr);
  void SetUnderutilFilter(UnderutilFilter f) {
    underutil_filter_ = f;
  }

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

  virtual bool ShouldStop() const {
    return false;
  }

  // Read-only walkers (e.g. CENSUS) never reallocate, so callers can skip
  // pre/post sizing work that only matters when an object may move.
  virtual bool IsReadOnly() const {
    return false;
  }

  // Walkers may stash the bucket cursor about to be visited so that downstream
  // Observe() calls can attribute candidates back to a bucket. Default no-op.
  virtual void SetCurrentBucketCursor(uint64_t /*cursor*/) {
  }

  float threshold() const {
    return threshold_;
  }

  void ExtendQuota(uint64_t quota_usec);

 protected:
  // Hook invoked from the dispatch switch when kind_ == kCustom. Default
  // returns false. Test/benchmark subclasses (e.g. SelectiveDefragment)
  // override to inject probabilistic or custom decisions without paying
  // virtual-dispatch cost on the production paths.
  virtual bool IsPageForObjectUnderUtilizedHook(void* /*object*/) {
    return false;
  }
  virtual bool IsPageForObjectUnderUtilizedHook(mi_heap_t* /*heap*/, void* /*object*/) {
    return false;
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

  CycleQuota quota_;

  UnderutilFilter underutil_filter_ = nullptr;

 protected:
  // Set to non-kBase by subclass constructors. Read by the inline dispatch
  // in page_usage_dispatch.h to forward to the concrete subclass impl.
  Kind kind_ = Kind::kBase;

  // For use in testing, forces reallocate check to always return true
  bool force_reallocate_{false};
};

}  // namespace dfly
