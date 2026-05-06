// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>

#define MI_BUILD_RELEASE 1
#include <mimalloc/types.h>

#include <functional>
#include <limits>
#include <optional>
#include <queue>
#include <vector>

#include "core/page_usage/page_usage_stats.h"
#include "server/table.h"

namespace dfly {

// Tracks pages that mimalloc has signalled (via the dragonfly underutil
// callback) as having dropped below the defrag utilization threshold during a
// free. Storage is per-thread (via thread_local) so each shard observes only
// its own heap. Replaces the dashtable-walking CENSUS as the source of
// candidate target pages.
namespace defrag_underutil {

// Register the mimalloc callback once per process. Safe to call from any
// thread; only the first call performs the registration. Subsequent calls are
// no-ops.
void InitOnce();

// Set the threshold (in percent, 0-100) used by mimalloc to decide when a page
// has crossed below the underutil watermark during a free. Must match the
// dragonfly-side `mem_defrag_page_utilization_threshold` so census and EVAC
// use the same definition of "underutilized".
void SetThresholdPct(uint8_t pct);

// Number of pages currently tracked on this thread's set.
size_t Size();

// Returns a copy of the page-address set as a vector. Does not modify the
// set; callers drop entries explicitly via Remove. Returned order is
// unspecified.
std::vector<uintptr_t> Snapshot();

// Drop a page from this thread's set. Used by VERIFY to retire targets that
// were successfully drained, and by the new CENSUS to drop entries that have
// since recovered above threshold.
void Remove(uintptr_t page_addr);

// Clear the entire set on this thread.
void Clear();

}  // namespace defrag_underutil

enum class DefragPhase : uint8_t {
  IDLE,
  CENSUS,
  SELECT_TARGETS,
  EVACUATE,
  VERIFY,
};

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
  std::vector<uint64_t> TakeCursorHints() {
    std::vector out(cursor_hints_.begin(), cursor_hints_.end());
    cursor_hints_.clear();
    std::ranges::sort(out);
    return out;
  }

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

struct CycleProgress {
  uint64_t targets_complete = 0;     // blocks_evacuated >= blocks_at_census
  uint64_t targets_partial = 0;      // 0 < blocks_evacuated < blocks_at_census
  uint64_t targets_no_progress = 0;  // blocks_evacuated == 0
  uint64_t targets_abandoned = 0;    // revalidation_failed; orthogonal to the trio above

  uint64_t blocks_total_at_census = 0;
  uint64_t blocks_evacuated = 0;
  uint64_t blocks_remaining = 0;

  uint64_t bytes_total_at_census = 0;
  uint64_t bytes_evacuated = 0;
  uint64_t bytes_remaining = 0;

  // Bytes mimalloc can return once fully-drained source pages are reclaimed:
  // sum over completed targets of (capacity - used)_at_census * block_size.
  // Compares directly to PlanStats::selected_reclaimable_bytes_at_census.
  uint64_t bytes_freed = 0;

  void Merge(const CycleProgress& other);
};

CycleProgress RunVerify(const TargetPlan& plan);

struct DefragCycleStats {
  CensusStats census;
  PlanStats plan;
  EvacStats evac;
  CycleProgress verify;

  uint64_t census_db_objects_scanned = 0;
  uint64_t evac_db_objects_scanned = 0;
  uint64_t evac_reallocations = 0;

  size_t census_retained_pages = 0;
  size_t plan_target_pages = 0;

  uint64_t census_potential_reclaim_bytes = 0;
  uint64_t census_movable_bytes_observed = 0;

  bool cycle_finished = false;

  void Merge(const DefragCycleStats& other);
};

struct DefragShardSummary {
  DefragPhase phase_start = DefragPhase::IDLE;  // phase on entry to DoDefrag
  DefragPhase phase_end = DefragPhase::IDLE;    // phase on exit from DoDefrag
  uint64_t duration_us = 0;                     // wall-clock time spent in DoDefrag
  bool quota_depleted = false;                  // visitor exhausted its CycleQuota
  bool finished_all_dbs = false;                // legacy: walked all dbs; phased: cycle complete
};

struct DefragShardReport {
  DefragShardSummary summary;           // per-shard exit info
  DefragCycleStats cycle_stats;         // empty on the legacy path
  CollectedPageStats page_usage_stats;  // search-index defrag stats from PageUsage
  bool work_pending = false;            // bg-task scheduler hint: true = high priority
};

struct DefragMergedReport {
  std::vector<DefragShardSummary> shard_summaries;  // index = shard_id
  DefragCycleStats cycle_stats;                     // summed across shards
  CollectedPageStats page_usage_stats;              // merged via CollectedPageStats::Merge

  static DefragMergedReport Merge(std::vector<DefragShardReport>&& shards);

  std::string ToString() const;
};

const char* PhaseName(DefragPhase phase);

class CensusTaker : public PageUsage {
 public:
  CensusTaker(PageCensus* census, float threshold, CycleQuota quota = CycleQuota::Unlimited());

  bool IsPageForObjectUnderUtilized(void* object) override;
  bool IsPageForObjectUnderUtilized(mi_heap_t* heap, void* object) override;

  bool IsReadOnly() const override {
    return true;
  }

  void SetCurrentBucketCursor(uint64_t cursor) override {
    current_cursor_ = cursor;
  }

 private:
  PageCensus* census_;
  float threshold_;
  uint64_t current_cursor_ = 0;
};

class Evacuator : public PageUsage {
 public:
  Evacuator(TargetPlan* plan, float threshold, EvacStats* evac_stats,
            CycleQuota quota = CycleQuota::Unlimited());

  bool IsPageForObjectUnderUtilized(void* object) override;
  bool IsPageForObjectUnderUtilized(mi_heap_t* heap, void* object) override;

  bool ShouldStop() const override {
    return plan_->AllTargetsDone();
  }

 private:
  TargetPlan* plan_;
  float threshold_;
  EvacStats* evac_stats_;
};

struct DefragTaskState {
  // Cycle position, used by both legacy and phased paths.
  size_t dbid = 0;
  uint64_t cursor = 0;

  // Threshold-gate state, consulted before starting a new cycle.
  time_t last_check_time = 0;
  float page_utilization_threshold = 0.8f;

  // Per-block move-cost weight in the page retention score:
  //   score = reclaim / (move_bytes + used_blocks * per_block_move_cost + slot_overhead)
  // Higher values penalize many-entry pages more strongly, pushing pages with
  // small block sizes (more entries per page) toward the back of the candidate
  // ordering. Useful for wide/mixed workloads where evacuating small-block
  // pages is expensive per byte reclaimed.
  uint64_t per_block_move_cost_bytes = 256;

  // Phased-only state, untouched in legacy mode.
  DefragPhase phase = DefragPhase::IDLE;
  std::optional<PageCensus> census;
  std::optional<TargetPlan> plan;
  // Bucket cursors observed during CENSUS that contained at least one object
  // on a candidate page. Moved here from PageCensus before SELECT_TARGETS
  // releases the census, then consumed by EVACUATE. Sorted vector + cursor
  // index lets the hinted walker resume mid-iteration across DoDefrag calls
  // when one EVAC quota slice can't drain the full hint set.
  std::vector<uint64_t> cursor_hints;
  size_t hint_cursor_idx = 0;
  DefragCycleStats cycle_stats;

  uint16_t shard_id = 0;
  uint64_t cycle_id = 0;
  uint64_t cycle_start_ns = 0;
  uint64_t phase_start_ns = 0;
  // CPU time spent doing actual work in the current phase, summed across
  // DoDefrag invocations. Resets at each phase transition. Distinguishes
  // CPU-only effort from wall-clock (phase_start_ns -> now), which includes
  // idle gaps between invocations.
  uint64_t phase_active_ns = 0;

  void UpdateScanState(uint64_t cursor_val);

  void ResetScanState();

  void FinishCycle();
};

struct DbSliceResult {
  uint64_t attempts = 0;       // # of (key, value) pairs visited
  uint64_t reallocations = 0;  // # of values where DefragIfNeeded returned true
  bool finished_all_dbs = false;
};

// Walker callable. If `hints` is non-null and non-empty, the walker should
// visit only the buckets listed in the hint vector starting at *hint_cursor
// (used by EVACUATE to skip buckets without candidate objects); the walker
// updates *hint_cursor to where it stopped so the next call can resume. If
// `hints` is null, the walker performs a full slice walk (used by CENSUS).
using DbSliceWalker = std::function<DbSliceResult(PageUsage*, const std::vector<uint64_t>* hints,
                                                  size_t* hint_cursor)>;

void DefragIdleStep(DefragTaskState* state, float threshold);
void DefragCensusStep(DefragTaskState* state, float threshold, CycleQuota quota,
                      const DbSliceWalker& walk);
void DefragSelectTargetsStep(DefragTaskState* state);
void DefragEvacuateStep(DefragTaskState* state, float threshold, CycleQuota quota,
                        const DbSliceWalker& walk);
void DefragVerifyStep(DefragTaskState* state);

void RunPhaseDefrag(DefragTaskState* state, float threshold, CycleQuota quota,
                    const DbSliceWalker& walk);

}  // namespace dfly
