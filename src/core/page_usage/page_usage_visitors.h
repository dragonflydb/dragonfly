// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>

#include <cstdint>
#include <limits>
#include <queue>
#include <vector>

#include "core/page_usage/page_usage_stats.h"

extern "C" {
#include "redis/zmalloc.h"
mi_page_usage_stats_t mi_heap_page_is_underutilized(mi_heap_t* heap, void* p, float ratio,
                                                    bool collect_stats);
}

namespace dfly {

struct PageAgg {
  uintptr_t page_address = 0;

  uint32_t block_size = 0;
  float retention_score = 0.0f;

  uint16_t capacity_blocks = 0;
  uint16_t used_blocks = 0;
  uint16_t observed_movable_blocks = 0;
  uint16_t generation = 0;

  uint8_t flags = 0;
};

struct CensusStats {
  uint64_t allocations_seen = 0;
  uint64_t allocations_recorded = 0;
  uint64_t skipped_above_threshold = 0;
  uint64_t skipped_full_page = 0;
  uint64_t skipped_wrong_heap = 0;
  uint64_t skipped_active_malloc_page = 0;
  uint64_t skipped_low_score = 0;

  // Top-K bookkeeping
  uint64_t pages_evicted_from_retained = 0;
  uint64_t heap_rebuilds = 0;

  void Merge(const CensusStats& other);
};

class PageCensus {
 public:
  static constexpr size_t kDefaultMaxRetainedPages = 100'000;

  // When false, PageCensus skips the top-k heap entirely and just inserts every
  // observed page into pages_. Cheap (no priority_queue work per Observe), but
  // loses the cap-and-evict-worst guard: the map hard-crashes on a new entry
  // once it reaches max_retained_pages_. Flip to true to restore heap-based
  // eviction so workloads exceeding the cap stay bounded.
  static constexpr bool kEnableTopK = false;

  explicit PageCensus(CensusStats* stats, size_t max_retained_pages = kDefaultMaxRetainedPages,
                      uint64_t per_block_move_cost_bytes = 256);

  // bucket_cursor is the DashTable cursor of the bucket the observed object
  // currently lives in. Recorded so EVACUATE can restrict its walk to buckets
  // known to contain at least one candidate object. Pass 0 if unknown
  // (callers outside the hot defrag path may not have a cursor).
  void Observe(const mi_page_usage_stats_t& stat, uint64_t bucket_cursor = 0);

  // Page-level observe used by the underutil-set fast path: caller has already
  // verified the page is in-heap, non-full, and below threshold. We don't have
  // per-object visibility, so we assume every used block is movable and let
  // EVACUATE's per-page revalidation correct any wrongly-classified entries.
  // No bucket cursor is recorded (cursor_hints stays empty in this path).
  void ObservePage(const mi_page_usage_stats_t& stat);

  const CensusStats& stats() const {
    return *stats_;
  }

  const absl::flat_hash_map<uintptr_t, PageAgg>& pages() const {
    return pages_;
  }

  const absl::flat_hash_set<uint64_t>& cursor_hints() const {
    return cursor_hints_;
  }

  // Move-out accessor: lets SELECT_TARGETS hand the hint set off to
  // DefragTaskState before the census itself is released. Returns a sorted
  // vector so the EVACUATE walker can iterate deterministically and resume
  // from a saved cursor index across DoDefrag invocations.
  std::vector<uint64_t> TakeCursorHints();

 private:
  void RebuildHeap();

  struct HeapEntry {
    uintptr_t page_address;
    float score;
    uint16_t generation;
  };

  struct WorseFirst {
    bool operator()(const HeapEntry& a, const HeapEntry& b) const {
      return a.score > b.score;
    }
  };

  absl::flat_hash_map<uintptr_t, PageAgg> pages_;
  std::priority_queue<HeapEntry, std::vector<HeapEntry>, WorseFirst> worst_retained_;
  // Buckets that observed at least one object on a candidate (under-threshold)
  // page. Consumed by EVACUATE to skip buckets with no targets.
  absl::flat_hash_set<uint64_t> cursor_hints_;
  CensusStats* stats_;
  size_t max_retained_pages_;
  uint64_t per_block_move_cost_bytes_;
};

enum class TargetStatus : uint8_t {
  kPending,
  kSuccess,
  kPartial,
  kFailed,
};

enum class TargetFilterReason : uint8_t {
  kKeep,
  kNoObservedBlocks,  // observed_movable_blocks == 0 (defensive)
  kStaleObservation,  // observed_movable_blocks > used_blocks
  kHasImmovableData,  // observed_movable_blocks < used_blocks (non-movables pin the page)
  kAlreadyEmpty,      // used_blocks == 0
};

enum class RevalidationFailureReason : uint8_t {
  kNone = 0,
  kHeapMismatch,
  kActiveMallocPage,
  kFullPage,
  kAboveThreshold,
};

struct TargetPage {
  // Snapshot from census (immutable after BuildFrom).
  uintptr_t page_address = 0;
  uint32_t block_size = 0;
  uint16_t capacity_blocks = 0;
  uint16_t blocks_at_census = 0;  // used_blocks at census time
  float retention_score_at_census = 0.0f;

  // Mutated during EVACUATE.
  uint16_t blocks_evacuated = 0;
  uint16_t evacuation_failures = 0;
  TargetStatus status = TargetStatus::kPending;
  bool revalidation_failed = false;
  // Set on the first revalidation failure; consulted on sticky-skip branches
  // to attribute subsequent block/byte skips to the originating reason.
  RevalidationFailureReason failure_reason = RevalidationFailureReason::kNone;
};

struct PlanStats {
  uint64_t targets_kept = 0;
  uint64_t filtered_no_observed_blocks = 0;
  uint64_t filtered_stale = 0;
  uint64_t filtered_has_immovable_data = 0;
  uint64_t filtered_already_empty = 0;
  uint64_t truncated_by_cap = 0;  // pages dropped because over max_targets

  uint64_t selected_capacity_bytes_at_census = 0;
  uint64_t selected_used_bytes_at_census = 0;
  uint64_t selected_reclaimable_bytes_at_census = 0;

  uint64_t truncated_reclaimable_bytes = 0;
  uint64_t filtered_immovable_reclaimable_bytes = 0;

  void Merge(const PlanStats& other);
};

enum class EvacOutcome : uint8_t {
  kNotATarget,          // page is not in the plan
  kTargetAlreadyDone,   // target's blocks_evacuated already at blocks_at_census
  kRevalidationFailed,  // page state shifted (full / above threshold / heap mismatch / etc.)
  kCommitMove,          // caller should perform the move; counter pre-bumped
};

struct EvacStats {
  uint64_t blocks_skipped_not_target = 0;
  uint64_t blocks_skipped_target_done = 0;
  uint64_t blocks_skipped_revalidation_failed = 0;

  uint64_t blocks_move_committed = 0;

  uint64_t bytes_skipped_target_done = 0;
  uint64_t bytes_skipped_revalidation_failed = 0;
  uint64_t bytes_move_committed = 0;

  uint64_t targets_revalidation_heap_mismatch = 0;
  uint64_t targets_revalidation_active_malloc_page = 0;
  uint64_t targets_revalidation_full_page = 0;
  uint64_t targets_revalidation_above_threshold = 0;

  // Block/byte breakdown of revalidation skips by originating reason. Sums to
  // blocks_skipped_revalidation_failed / bytes_skipped_revalidation_failed.
  uint64_t blocks_revalidation_heap_mismatch = 0;
  uint64_t blocks_revalidation_active_malloc_page = 0;
  uint64_t blocks_revalidation_full_page = 0;
  uint64_t blocks_revalidation_above_threshold = 0;
  uint64_t bytes_revalidation_heap_mismatch = 0;
  uint64_t bytes_revalidation_active_malloc_page = 0;
  uint64_t bytes_revalidation_full_page = 0;
  uint64_t bytes_revalidation_above_threshold = 0;

  uint64_t targets_abandoned_revalidation = 0;
  uint64_t targets_completed_during_evac = 0;

  void Merge(const EvacStats& other);
};

class TargetPlan {
 public:
  explicit TargetPlan(PlanStats* stats);
  ~TargetPlan();

  // Non-copyable, non-movable: destructor clears mimalloc defrag_skip bits on
  // active targets, so move-from would double-clear (harmless) but copies
  // would set bits this object doesn't own.
  TargetPlan(const TargetPlan&) = delete;
  TargetPlan& operator=(const TargetPlan&) = delete;
  TargetPlan(TargetPlan&&) = delete;
  TargetPlan& operator=(TargetPlan&&) = delete;

  // Default `max_targets` is effectively unlimited; selective skip-bit (top
  // skip_pct fraction) bounds lockout pressure. Tests pass explicit small
  // values to exercise the cap path.
  void BuildFrom(const PageCensus& census, size_t max_targets = std::numeric_limits<size_t>::max());

  const std::vector<TargetPage>& targets() const {
    return targets_;
  }

  const PlanStats& stats() const {
    return *stats_;
  }

  bool Contains(uintptr_t addr) const;

  const TargetPage* Find(uintptr_t addr) const;
  TargetPage* FindMut(uintptr_t addr);

  size_t size() const {
    return targets_.size();
  }

  bool empty() const {
    return targets_.empty();
  }

  bool AllTargetsDone() const {
    return pending_targets_ == 0;
  }

  void NotifyTargetDone() {
    --pending_targets_;
  }

 private:
  std::vector<TargetPage> targets_;  // sorted by retention_score desc
  absl::flat_hash_map<uintptr_t, size_t> address_to_index_;
  PlanStats* stats_;
  size_t pending_targets_ = 0;
};

// Hot-path variant: caller has already resolved the target (e.g. via
// plan.FindMut). Must be non-null. Skips the redundant lookup that the
// 3-arg variant otherwise performs.
EvacOutcome EvacDecide(TargetPlan& plan, TargetPage* target, const mi_page_usage_stats_t& stat,
                       EvacStats& stats);

// Convenience variant: looks up the target from stat.page_address. Returns
// kNotATarget on miss. Kept for test ergonomics; production callers should
// prefer the 4-arg form so they can fold the lookup with their own
// fast-path checks.
EvacOutcome EvacDecide(TargetPlan& plan, const mi_page_usage_stats_t& stat, EvacStats& stats);

class CensusTaker final : public PageUsage {
 public:
  CensusTaker(PageCensus* census, float threshold, CycleQuota quota = CycleQuota::Unlimited());

  // Override to call the mimalloc syscall and feed the resulting stats into
  // the census; CensusTaker never reallocates so the return is always false.
  bool IsPageForObjectUnderUtilized(void* object) override;
  bool IsPageForObjectUnderUtilized(mi_heap_t* heap, void* object) override;

  bool IsReadOnly() const final {
    return true;
  }

  bool ShouldDefragKeys() const final;

  void SetCurrentBucketCursor(uint64_t cursor) final {
    current_cursor_ = cursor;
  }

 private:
  PageCensus* census_;
  float threshold_;
  uint64_t current_cursor_ = 0;
};

class Evacuator final : public PageUsage {
 public:
  Evacuator(TargetPlan* plan, float threshold, EvacStats* evac_stats,
            CycleQuota quota = CycleQuota::Unlimited());

  // Override to filter through the plan: a per-object hashmap lookup short-
  // circuits the expensive mi_heap_page_is_underutilized syscall when the
  // object isn't on a target page. On hit, calls the syscall + EvacDecide.
  bool IsPageForObjectUnderUtilized(void* object) override;
  bool IsPageForObjectUnderUtilized(mi_heap_t* heap, void* object) override;

  bool ShouldDefragKeys() const final;

  bool ShouldStop() const final {
    return plan_->AllTargetsDone();
  }

 private:
  TargetPlan* plan_;
  float threshold_;
  EvacStats* evac_stats_;
};

}  // namespace dfly
