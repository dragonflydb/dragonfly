// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>
#include <ctime>
#include <functional>
#include <optional>
#include <string>
#include <vector>

#include "core/page_usage/page_usage_stats.h"
#include "core/page_usage/page_usage_visitors.h"

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

// Filter predicate for the legacy DefragIfNeeded fast path. Returns true when
// the page is "interesting" (a candidate for the expensive
// mi_heap_page_is_underutilized check):
//   - empty set ⇒ bootstrap, no info available, return true so callers
//     fall through to the original behavior;
//   - non-empty set ⇒ return true iff the page appears in the set.
// Conservative-positive: pages that crossed threshold without a recent free
// will be missed until the next free on them lands them in the set.
bool IsPageMaybeUnderutil(uintptr_t page_addr);

}  // namespace defrag_underutil

enum class DefragPhase : uint8_t {
  IDLE,
  CENSUS,
  SELECT_TARGETS,
  EVACUATE,
  VERIFY,
};

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
