// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/defrag.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>
#include <absl/strings/strip.h>
#include <absl/time/clock.h>
#include <mimalloc/internal.h>
#include <mimalloc/types.h>

#include <algorithm>
#include <mutex>

#include "base/flags.h"
#include "base/logging.h"

ABSL_FLAG(bool, defrag_use_skip_bit, false,
          "If true, mark target pages with mimalloc's defrag_skip bit so EVAC moves don't "
          "refill them. Disable to A/B compare against an unmarked baseline.");

ABSL_FLAG(double, defrag_skip_percentile, 0.5,
          "Fraction of the target plan (sorted by retention_score, most-fragmented first) "
          "to apply the mimalloc defrag_skip bit to. 0.5 (default) marks the top half "
          "and lets the bottom half stay refillable, which empirically gives the best "
          "floor-vs-bulge tradeoff. 1.0 marks every target (max reclaim, biggest bulge). "
          "Lower values shrink the lockout footprint: only the most-fragmented top-K "
          "pages are protected from refill while higher-utilization targets stay "
          "refillable.");

ABSL_FLAG(uint64_t, defrag_min_plan_reclaimable_bytes, 64u << 20,
          "Minimum bytes-reclaimable threshold the SELECT_TARGETS plan must hit to "
          "justify running EVACUATE. Below this, the cycle is skipped (PLAN_SKIPPED) "
          "and we return to IDLE without walking the dashtable. The underutil set is "
          "left intact so the next cycle picks it up if churn refills the pages above "
          "threshold (or new fragmentation appears). Default 64 MiB.");

extern "C" {
#include "redis/zmalloc.h"
mi_page_usage_stats_t mi_heap_page_is_underutilized(mi_heap_t* heap, void* p, float ratio,
                                                    bool collect_stats);
// Dragonfly mimalloc patch: tell mi_malloc to skip a page during defrag so
// EVACUATE moves don't refill pages we're trying to drain.
void mi_page_set_defrag_skip(uintptr_t page_addr, bool skip);

// Dragonfly mimalloc patch: per-process callback fired by mi_free_block_local
// when a page's used count crosses below the configured underutil threshold.
typedef void (*mi_dfly_underutil_callback_t)(uintptr_t page_addr);
void mi_dfly_set_underutil_callback(mi_dfly_underutil_callback_t cb);
void mi_dfly_set_underutil_threshold_pct(uint8_t pct);
}

namespace dfly {

namespace {

constexpr uint64_t kPerTargetSlotCostBytes = 16 * 1024;

uint64_t ReclaimableBytes(uint16_t capacity_blocks, uint16_t used_blocks, uint32_t block_size) {
  if (used_blocks >= capacity_blocks)
    return 0;
  return uint64_t(capacity_blocks - used_blocks) * block_size;
}

uint64_t MoveBytes(uint16_t used_blocks, uint32_t block_size) {
  return uint64_t(used_blocks) * block_size;
}

uint64_t ReclaimableBytes(const TargetPage& target) {
  return ReclaimableBytes(target.capacity_blocks, target.blocks_at_census, target.block_size);
}

uint64_t MoveBytes(const TargetPage& target) {
  return MoveBytes(target.blocks_at_census, target.block_size);
}

float ComputeRetentionScore(uint16_t capacity_blocks, uint16_t used_blocks, uint32_t block_size,
                            uint64_t per_block_move_cost_bytes) {
  const uint64_t reclaim = ReclaimableBytes(capacity_blocks, used_blocks, block_size);
  const uint64_t move = MoveBytes(used_blocks, block_size);
  const uint64_t cost =
      move + uint64_t(used_blocks) * per_block_move_cost_bytes + kPerTargetSlotCostBytes;
  return static_cast<float>(static_cast<double>(reclaim) / std::max<uint64_t>(1, cost));
}

void PopulateAgg(PageAgg& agg, const mi_page_usage_stats_t& stat, float score) {
  agg.page_address = stat.page_address;
  agg.block_size = static_cast<uint32_t>(stat.block_size);
  agg.capacity_blocks = stat.capacity;
  agg.used_blocks = stat.used;
  agg.flags = stat.flags;
  ++agg.observed_movable_blocks;
  ++agg.generation;
  agg.retention_score = score;
}

TargetFilterReason ClassifyForTarget(const PageAgg& agg) {
  if (agg.observed_movable_blocks == 0)
    return TargetFilterReason::kNoObservedBlocks;
  if (agg.used_blocks == 0)
    return TargetFilterReason::kAlreadyEmpty;
  if (agg.observed_movable_blocks > agg.used_blocks)
    return TargetFilterReason::kStaleObservation;
  return TargetFilterReason::kKeep;
}

TargetPage MakeTargetPage(const PageAgg& agg) {
  TargetPage tp;
  tp.page_address = agg.page_address;
  tp.block_size = agg.block_size;
  tp.capacity_blocks = agg.capacity_blocks;
  tp.blocks_at_census = agg.used_blocks;
  tp.retention_score_at_census = agg.retention_score;
  return tp;
}

#define DEFRAG_STEP_LOG LOG(INFO)

uint64_t NowNs() {
  return absl::GetCurrentTimeNanos();
}

double NsToMs(uint64_t ns) {
  return static_cast<double>(ns) / 1e6;
}

std::string FormatMiB(uint64_t bytes) {
  return absl::StrFormat("%.2fMiB", static_cast<double>(bytes) / (1024.0 * 1024.0));
}

// Wrapper around mimalloc's defrag_skip setter. Gated by a runtime flag so
// experiments can disable the skip-bit logic and observe whether refills on
// drained pages re-emerge.
//
// SHARP EDGE: mi_page_set_defrag_skip writes through the page address as
// mi_page_t*. If that memory has been unmapped (page retired -> segment
// freed -> OS reclaim, particularly under Dragonfly's aggressive purge
// settings) or reused for something else, the write segfaults or silently
// corrupts unrelated state. The window opens between SELECT_TARGETS adding
// the page to the plan and ~TargetPlan clearing the bit at end-of-cycle;
// any external drain-to-empty plus retire-and-unmap during that window
// makes the page address stale.
//
// The EvacDecide success / revalidation paths clear the bit *before* the
// triggering move runs (so the page is still mapped). The destructor sweep
// is the riskier site — it touches every plan target unconditionally. To
// harden, options are: (a) move the skip flag to a per-shard side table
// keyed by page address, (b) add a mimalloc refcount keeping target pages
// mapped for the cycle, (c) validate the page is still in a known segment
// before writing. (a) is the cleanest if it ever bites in production.
void SetDefragSkipIfEnabled(uintptr_t page_addr, bool skip) {
  if (absl::GetFlag(FLAGS_defrag_use_skip_bit)) {
    mi_page_set_defrag_skip(page_addr, skip);
  }
}

}  // namespace

namespace defrag_underutil {

namespace {

thread_local absl::flat_hash_set<uintptr_t> tl_underutil_pages;

void OnPageUnderutil(uintptr_t page_addr) {
  tl_underutil_pages.insert(page_addr);
}

}  // namespace

void InitOnce() {
  static std::once_flag once;
  std::call_once(once, []() {
    mi_dfly_set_underutil_callback(&OnPageUnderutil);
    LOG(INFO) << "defrag[underutil_cb] registered with mimalloc";
  });
}

void SetThresholdPct(uint8_t pct) {
  mi_dfly_set_underutil_threshold_pct(pct);
}

size_t Size() {
  return tl_underutil_pages.size();
}

std::vector<uintptr_t> Snapshot() {
  return {tl_underutil_pages.begin(), tl_underutil_pages.end()};
}

void Remove(uintptr_t page_addr) {
  tl_underutil_pages.erase(page_addr);
}

void Clear() {
  tl_underutil_pages.clear();
}

}  // namespace defrag_underutil

void DefragTaskState::UpdateScanState(uint64_t cursor_val) {
  cursor = cursor_val;
  if (cursor == 0u) {
    ++dbid;
  }
}

void DefragTaskState::ResetScanState() {
  dbid = 0;
  cursor = 0;
}

void DefragTaskState::FinishCycle() {
  phase = DefragPhase::IDLE;
  census.reset();
  plan.reset();
  cursor_hints.clear();
  hint_cursor_idx = 0;
  ResetScanState();
}

void CensusStats::Merge(const CensusStats& other) {
  allocations_seen += other.allocations_seen;
  allocations_recorded += other.allocations_recorded;
  skipped_above_threshold += other.skipped_above_threshold;
  skipped_full_page += other.skipped_full_page;
  skipped_wrong_heap += other.skipped_wrong_heap;
  skipped_active_malloc_page += other.skipped_active_malloc_page;
  skipped_low_score += other.skipped_low_score;
  pages_evicted_from_retained += other.pages_evicted_from_retained;
  heap_rebuilds += other.heap_rebuilds;
}

void PlanStats::Merge(const PlanStats& other) {
  targets_kept += other.targets_kept;
  filtered_no_observed_blocks += other.filtered_no_observed_blocks;
  filtered_stale += other.filtered_stale;
  filtered_has_immovable_data += other.filtered_has_immovable_data;
  filtered_already_empty += other.filtered_already_empty;
  truncated_by_cap += other.truncated_by_cap;
  selected_capacity_bytes_at_census += other.selected_capacity_bytes_at_census;
  selected_used_bytes_at_census += other.selected_used_bytes_at_census;
  selected_reclaimable_bytes_at_census += other.selected_reclaimable_bytes_at_census;
  truncated_reclaimable_bytes += other.truncated_reclaimable_bytes;
  filtered_immovable_reclaimable_bytes += other.filtered_immovable_reclaimable_bytes;
}

void EvacStats::Merge(const EvacStats& other) {
  blocks_skipped_not_target += other.blocks_skipped_not_target;
  blocks_skipped_target_done += other.blocks_skipped_target_done;
  blocks_skipped_revalidation_failed += other.blocks_skipped_revalidation_failed;
  blocks_move_committed += other.blocks_move_committed;
  bytes_skipped_target_done += other.bytes_skipped_target_done;
  bytes_skipped_revalidation_failed += other.bytes_skipped_revalidation_failed;
  bytes_move_committed += other.bytes_move_committed;
  targets_revalidation_heap_mismatch += other.targets_revalidation_heap_mismatch;
  targets_revalidation_active_malloc_page += other.targets_revalidation_active_malloc_page;
  targets_revalidation_full_page += other.targets_revalidation_full_page;
  targets_revalidation_above_threshold += other.targets_revalidation_above_threshold;
  blocks_revalidation_heap_mismatch += other.blocks_revalidation_heap_mismatch;
  blocks_revalidation_active_malloc_page += other.blocks_revalidation_active_malloc_page;
  blocks_revalidation_full_page += other.blocks_revalidation_full_page;
  blocks_revalidation_above_threshold += other.blocks_revalidation_above_threshold;
  bytes_revalidation_heap_mismatch += other.bytes_revalidation_heap_mismatch;
  bytes_revalidation_active_malloc_page += other.bytes_revalidation_active_malloc_page;
  bytes_revalidation_full_page += other.bytes_revalidation_full_page;
  bytes_revalidation_above_threshold += other.bytes_revalidation_above_threshold;
  targets_abandoned_revalidation += other.targets_abandoned_revalidation;
  targets_completed_during_evac += other.targets_completed_during_evac;
}

void CycleProgress::Merge(const CycleProgress& other) {
  targets_complete += other.targets_complete;
  targets_partial += other.targets_partial;
  targets_no_progress += other.targets_no_progress;
  targets_abandoned += other.targets_abandoned;
  blocks_total_at_census += other.blocks_total_at_census;
  blocks_evacuated += other.blocks_evacuated;
  blocks_remaining += other.blocks_remaining;
  bytes_total_at_census += other.bytes_total_at_census;
  bytes_evacuated += other.bytes_evacuated;
  bytes_remaining += other.bytes_remaining;
  bytes_freed += other.bytes_freed;
}

void DefragCycleStats::Merge(const DefragCycleStats& other) {
  census.Merge(other.census);
  plan.Merge(other.plan);
  evac.Merge(other.evac);
  verify.Merge(other.verify);
  census_db_objects_scanned += other.census_db_objects_scanned;
  evac_db_objects_scanned += other.evac_db_objects_scanned;
  evac_reallocations += other.evac_reallocations;
  census_retained_pages += other.census_retained_pages;
  plan_target_pages += other.plan_target_pages;
  census_potential_reclaim_bytes += other.census_potential_reclaim_bytes;
  census_movable_bytes_observed += other.census_movable_bytes_observed;
  // cycle_finished is per-shard semantics; not meaningfully mergeable.
  // On the merged report, callers should use phase_per_shard to answer
  // "is every shard done?" — cycle_finished stays at its default (false).
}

DefragMergedReport DefragMergedReport::Merge(std::vector<DefragShardReport>&& shards) {
  DefragMergedReport result;
  result.shard_summaries.reserve(shards.size());
  std::vector<CollectedPageStats> page_usage_list;
  page_usage_list.reserve(shards.size());

  for (DefragShardReport& shard : shards) {
    result.shard_summaries.push_back(shard.summary);
    result.cycle_stats.Merge(shard.cycle_stats);
    page_usage_list.push_back(std::move(shard.page_usage_stats));
  }
  // CollectedPageStats::Merge takes a threshold; carry the first-shard value
  // forward (all shards are configured with the same threshold).
  const float threshold =
      page_usage_list.empty() ? 0.0f : static_cast<float>(page_usage_list.front().threshold);
  result.page_usage_stats = CollectedPageStats::Merge(std::move(page_usage_list), threshold);
  return result;
}

const char* PhaseName(DefragPhase phase) {
  switch (phase) {
    case DefragPhase::IDLE:
      return "IDLE";
    case DefragPhase::CENSUS:
      return "CENSUS";
    case DefragPhase::SELECT_TARGETS:
      return "SELECT_TARGETS";
    case DefragPhase::EVACUATE:
      return "EVACUATE";
    case DefragPhase::VERIFY:
      return "VERIFY";
  }
  return "UNKNOWN";
}

std::string DefragMergedReport::ToString() const {
  std::string out;
  const auto& cs = cycle_stats;

  absl::StrAppend(&out, "Per-shard summary:\n");
  absl::StrAppend(&out, "  shard | phase_start    -> phase_end      | duration_us | exit_reason\n");
  for (size_t i = 0; i < shard_summaries.size(); ++i) {
    const DefragShardSummary& s = shard_summaries[i];
    const char* exit_reason = s.finished_all_dbs ? "finished" : (s.quota_depleted ? "quota" : "-");
    absl::StrAppend(
        &out, "  ", absl::Dec(i, absl::PadSpec::kSpacePad5), " | ",
        absl::StrFormat("%-14s -> %-14s", PhaseName(s.phase_start), PhaseName(s.phase_end)), " | ",
        absl::Dec(s.duration_us, absl::PadSpec::kSpacePad11), " | ", exit_reason, "\n");
  }

  absl::StrAppend(&out, "\n[CENSUS]\n");
  absl::StrAppend(&out, "Allocations seen: ", cs.census.allocations_seen, "\n");
  absl::StrAppend(&out, "Allocations recorded: ", cs.census.allocations_recorded, "\n");
  absl::StrAppend(&out, "Skipped (above threshold): ", cs.census.skipped_above_threshold, "\n");
  absl::StrAppend(&out, "Skipped (full page): ", cs.census.skipped_full_page, "\n");
  absl::StrAppend(&out, "Skipped (wrong heap): ", cs.census.skipped_wrong_heap, "\n");
  absl::StrAppend(&out, "Skipped (active malloc page): ", cs.census.skipped_active_malloc_page,
                  "\n");
  absl::StrAppend(&out, "Skipped (low score): ", cs.census.skipped_low_score, "\n");
  absl::StrAppend(&out, "Pages evicted from retained: ", cs.census.pages_evicted_from_retained,
                  "\n");
  absl::StrAppend(&out, "Heap rebuilds: ", cs.census.heap_rebuilds, "\n");
  absl::StrAppend(&out, "DB objects scanned: ", cs.census_db_objects_scanned, "\n");
  absl::StrAppend(&out, "Retained pages (total): ", cs.census_retained_pages, "\n");
  absl::StrAppend(
      &out, "Potential reclaimable bytes (observed): ", cs.census_potential_reclaim_bytes, "\n");
  absl::StrAppend(&out, "Movable bytes (observed): ", cs.census_movable_bytes_observed, "\n");

  absl::StrAppend(&out, "\n[SELECT]\n");
  absl::StrAppend(&out, "Targets kept: ", cs.plan.targets_kept, "\n");
  absl::StrAppend(&out, "Filtered (no observed blocks): ", cs.plan.filtered_no_observed_blocks,
                  "\n");
  absl::StrAppend(&out, "Filtered (stale): ", cs.plan.filtered_stale, "\n");
  absl::StrAppend(&out, "Filtered (has immovable data): ", cs.plan.filtered_has_immovable_data,
                  "\n");
  absl::StrAppend(&out, "Filtered (already empty): ", cs.plan.filtered_already_empty, "\n");
  absl::StrAppend(&out, "Truncated by cap: ", cs.plan.truncated_by_cap, "\n");
  absl::StrAppend(&out, "Target pages (total): ", cs.plan_target_pages, "\n");
  absl::StrAppend(&out, "Selected capacity bytes (at census): ",
                  cs.plan.selected_capacity_bytes_at_census, "\n");
  absl::StrAppend(&out, "Selected used bytes (at census): ", cs.plan.selected_used_bytes_at_census,
                  "\n");
  absl::StrAppend(&out, "Selected reclaimable bytes (at census): ",
                  cs.plan.selected_reclaimable_bytes_at_census, "\n");
  absl::StrAppend(&out, "Truncated reclaimable bytes: ", cs.plan.truncated_reclaimable_bytes, "\n");
  absl::StrAppend(&out, "Filtered immovable reclaimable bytes: ",
                  cs.plan.filtered_immovable_reclaimable_bytes, "\n");

  absl::StrAppend(&out, "\n[EVACUATE]\n");
  absl::StrAppend(&out, "DB objects scanned: ", cs.evac_db_objects_scanned, "\n");
  absl::StrAppend(&out, "Reallocations: ", cs.evac_reallocations, "\n");
  absl::StrAppend(&out, "Blocks moved (committed): ", cs.evac.blocks_move_committed, "\n");
  absl::StrAppend(&out, "Bytes moved (committed): ", cs.evac.bytes_move_committed, "\n");
  absl::StrAppend(&out, "Blocks skipped (not target): ", cs.evac.blocks_skipped_not_target, "\n");
  absl::StrAppend(&out, "Blocks skipped (target done): ", cs.evac.blocks_skipped_target_done, "\n");
  absl::StrAppend(&out, "Blocks skipped (revalidation failed): ",
                  cs.evac.blocks_skipped_revalidation_failed, "\n");
  absl::StrAppend(&out, "Bytes skipped (target done): ", cs.evac.bytes_skipped_target_done, "\n");
  absl::StrAppend(&out, "Bytes skipped (revalidation failed): ",
                  cs.evac.bytes_skipped_revalidation_failed, "\n");
  absl::StrAppend(&out, "Targets revalidation (heap mismatch): ",
                  cs.evac.targets_revalidation_heap_mismatch, "\n");
  absl::StrAppend(&out, "Targets revalidation (active malloc page): ",
                  cs.evac.targets_revalidation_active_malloc_page, "\n");
  absl::StrAppend(
      &out, "Targets revalidation (full page): ", cs.evac.targets_revalidation_full_page, "\n");
  absl::StrAppend(&out, "Targets revalidation (above threshold): ",
                  cs.evac.targets_revalidation_above_threshold, "\n");
  absl::StrAppend(&out, "Blocks revalidation (heap mismatch): ",
                  cs.evac.blocks_revalidation_heap_mismatch, "\n");
  absl::StrAppend(&out, "Blocks revalidation (active malloc page): ",
                  cs.evac.blocks_revalidation_active_malloc_page, "\n");
  absl::StrAppend(&out, "Blocks revalidation (full page): ", cs.evac.blocks_revalidation_full_page,
                  "\n");
  absl::StrAppend(&out, "Blocks revalidation (above threshold): ",
                  cs.evac.blocks_revalidation_above_threshold, "\n");
  absl::StrAppend(
      &out, "Bytes revalidation (heap mismatch): ", cs.evac.bytes_revalidation_heap_mismatch, "\n");
  absl::StrAppend(&out, "Bytes revalidation (active malloc page): ",
                  cs.evac.bytes_revalidation_active_malloc_page, "\n");
  absl::StrAppend(&out, "Bytes revalidation (full page): ", cs.evac.bytes_revalidation_full_page,
                  "\n");
  absl::StrAppend(&out, "Bytes revalidation (above threshold): ",
                  cs.evac.bytes_revalidation_above_threshold, "\n");
  absl::StrAppend(
      &out, "Targets abandoned (revalidation): ", cs.evac.targets_abandoned_revalidation, "\n");
  absl::StrAppend(&out, "Targets completed during evac: ", cs.evac.targets_completed_during_evac,
                  "\n");

  absl::StrAppend(&out, "\n[VERIFY]\n");
  absl::StrAppend(&out, "Targets complete: ", cs.verify.targets_complete, "\n");
  absl::StrAppend(&out, "Targets partial: ", cs.verify.targets_partial, "\n");
  absl::StrAppend(&out, "Targets no progress: ", cs.verify.targets_no_progress, "\n");
  absl::StrAppend(&out, "Targets abandoned: ", cs.verify.targets_abandoned, "\n");
  absl::StrAppend(&out, "Blocks total (at census): ", cs.verify.blocks_total_at_census, "\n");
  absl::StrAppend(&out, "Blocks evacuated: ", cs.verify.blocks_evacuated, "\n");
  absl::StrAppend(&out, "Blocks remaining: ", cs.verify.blocks_remaining, "\n");
  absl::StrAppend(&out, "Bytes total (at census): ", cs.verify.bytes_total_at_census, "\n");
  absl::StrAppend(&out, "Bytes evacuated: ", cs.verify.bytes_evacuated, "\n");
  absl::StrAppend(&out, "Bytes remaining: ", cs.verify.bytes_remaining, "\n");

  absl::StrAppend(&out, "\n[PAGE USAGE]\n", page_usage_stats.ToString());

  absl::StripTrailingAsciiWhitespace(&out);
  return out;
}

// Build a usage-stat struct directly from a page address, validating that the
// page still belongs to `heap`, isn't empty, isn't full, and is still below
// threshold. Returns true on success; on false the caller should drop the
// address from the underutil set (page recovered, was reclaimed, or never was
// ours). Threshold is fractional in [0, 1] matching the dragonfly setting.
static bool BuildPageStatFromAddress(uintptr_t page_addr, mi_heap_t* heap, float threshold,
                                     mi_page_usage_stats_t* out) {
  if (page_addr == 0)
    return false;
  mi_page_t* page = reinterpret_cast<mi_page_t*>(page_addr);

  if (mi_page_heap(page) != heap)
    return false;

  const uint16_t used = page->used;
  const uint16_t cap = page->capacity;
  if (used == 0 || cap == 0)
    return false;
  if (used >= cap)
    return false;  // full; mimalloc may have re-filled it

  const float used_ratio = static_cast<float>(used) / static_cast<float>(cap);
  if (used_ratio > threshold)
    return false;  // recovered above threshold

  out->page_address = page_addr;
  out->block_size = mi_page_block_size(page);
  out->capacity = cap;
  out->reserved = page->reserved;
  out->used = used;
  out->flags = MI_DFLY_PAGE_BELOW_THRESHOLD;
  return true;
}

PageCensus::PageCensus(CensusStats* stats, size_t max_retained_pages,
                       uint64_t per_block_move_cost_bytes)
    : stats_(stats),
      max_retained_pages_(max_retained_pages),
      per_block_move_cost_bytes_(per_block_move_cost_bytes) {
}

void PageCensus::Observe(const mi_page_usage_stats_t& stat, uint64_t bucket_cursor) {
  ++stats_->allocations_seen;

  if (stat.flags & MI_DFLY_HEAP_MISMATCH) {
    ++stats_->skipped_wrong_heap;
    return;
  }
  if (stat.flags & MI_DFLY_PAGE_USED_FOR_MALLOC) {
    ++stats_->skipped_active_malloc_page;
    return;
  }
  if (stat.flags & MI_DFLY_PAGE_FULL) {
    ++stats_->skipped_full_page;
    return;
  }
  if ((stat.flags & MI_DFLY_PAGE_BELOW_THRESHOLD) == 0) {
    ++stats_->skipped_above_threshold;
    return;
  }

  // Object lives on a candidate page; remember its bucket so EVACUATE can
  // skip buckets that contain no candidates at all.
  cursor_hints_.insert(bucket_cursor);

  const float new_score = ComputeRetentionScore(
      stat.capacity, stat.used, static_cast<uint32_t>(stat.block_size), per_block_move_cost_bytes_);

  if constexpr (kEnableTopK) {
    auto add_entry = [&] {
      PageAgg& agg = pages_[stat.page_address];
      PopulateAgg(agg, stat, new_score);
      worst_retained_.push({stat.page_address, new_score, agg.generation});
      ++stats_->allocations_recorded;
    };

    if (auto it = pages_.find(stat.page_address); it != pages_.end()) {
      PopulateAgg(it->second, stat, new_score);
      worst_retained_.push({stat.page_address, new_score, it->second.generation});
      ++stats_->allocations_recorded;
    } else if (pages_.size() < max_retained_pages_) {
      add_entry();
    } else {
      while (!worst_retained_.empty()) {
        const HeapEntry top = worst_retained_.top();
        auto evict_it = pages_.find(top.page_address);
        if (evict_it == pages_.end() || evict_it->second.generation != top.generation) {
          worst_retained_.pop();
          continue;
        }
        if (new_score > top.score) {
          worst_retained_.pop();
          pages_.erase(evict_it);
          ++stats_->pages_evicted_from_retained;
          add_entry();
        } else {
          ++stats_->skipped_low_score;
        }
        break;
      }
    }

    if (worst_retained_.size() > 2 * max_retained_pages_) {
      RebuildHeap();
      ++stats_->heap_rebuilds;
    }
  } else {
    if (auto it = pages_.find(stat.page_address); it != pages_.end()) {
      PopulateAgg(it->second, stat, new_score);
    } else {
      CHECK_LT(pages_.size(), max_retained_pages_)
          << "PageCensus exceeded max_retained_pages_=" << max_retained_pages_
          << " with kEnableTopK=false";
      PageAgg& agg = pages_[stat.page_address];
      PopulateAgg(agg, stat, new_score);
    }
    ++stats_->allocations_recorded;
  }
}

void PageCensus::ObservePage(const mi_page_usage_stats_t& stat) {
  ++stats_->allocations_seen;

  if ((stat.flags & MI_DFLY_PAGE_BELOW_THRESHOLD) == 0) {
    ++stats_->skipped_above_threshold;
    return;
  }

  const float new_score = ComputeRetentionScore(
      stat.capacity, stat.used, static_cast<uint32_t>(stat.block_size), per_block_move_cost_bytes_);

  if constexpr (!kEnableTopK) {
    if (!pages_.contains(stat.page_address)) {
      CHECK_LT(pages_.size(), max_retained_pages_)
          << "PageCensus exceeded max_retained_pages_=" << max_retained_pages_
          << " with kEnableTopK=false";
    }
  }

  PageAgg& agg = pages_[stat.page_address];
  agg.page_address = stat.page_address;
  agg.block_size = static_cast<uint32_t>(stat.block_size);
  agg.capacity_blocks = stat.capacity;
  agg.used_blocks = stat.used;
  agg.flags = stat.flags;
  // No per-object visibility on this path; assume every used block is movable.
  // EVAC's per-page revalidation drops pages whose blocks turn out immovable.
  agg.observed_movable_blocks = stat.used;
  agg.generation = 1;
  agg.retention_score = new_score;

  ++stats_->allocations_recorded;

  if constexpr (kEnableTopK) {
    worst_retained_.push({stat.page_address, new_score, agg.generation});
    if (worst_retained_.size() > 2 * max_retained_pages_) {
      RebuildHeap();
      ++stats_->heap_rebuilds;
    }
  }
}

void PageCensus::RebuildHeap() {
  std::vector<HeapEntry> entries;
  entries.reserve(pages_.size());
  for (const auto& [addr, agg] : pages_) {
    entries.push_back({addr, agg.retention_score, agg.generation});
  }
  worst_retained_ = std::priority_queue(WorseFirst{}, std::move(entries));
}

TargetPlan::TargetPlan(PlanStats* stats) : stats_(stats) {
}

TargetPlan::~TargetPlan() {
  // Clear the mimalloc defrag_skip bit on every active target so the page
  // becomes eligible for new allocations again. Tail entries are not marked
  // (only active plan entries are), so they need no clear.
  for (const TargetPage& tp : targets_) {
    SetDefragSkipIfEnabled(tp.page_address, false);
  }
}

void TargetPlan::BuildFrom(const PageCensus& census, size_t max_targets) {
  targets_.clear();
  address_to_index_.clear();
  *stats_ = PlanStats{};

  std::vector<TargetPage> candidates;
  candidates.reserve(census.pages().size());

  for (const auto& agg : census.pages() | std::views::values) {
    switch (ClassifyForTarget(agg)) {
      case TargetFilterReason::kKeep:
        candidates.push_back(MakeTargetPage(agg));
        break;
      case TargetFilterReason::kNoObservedBlocks:
        ++stats_->filtered_no_observed_blocks;
        break;
      case TargetFilterReason::kAlreadyEmpty:
        ++stats_->filtered_already_empty;
        break;
      case TargetFilterReason::kStaleObservation:
        ++stats_->filtered_stale;
        break;
      case TargetFilterReason::kHasImmovableData:
        ++stats_->filtered_has_immovable_data;
        stats_->filtered_immovable_reclaimable_bytes +=
            uint64_t(agg.capacity_blocks - agg.used_blocks) * agg.block_size;
        break;
    }
  }

  std::ranges::sort(candidates, [](const TargetPage& a, const TargetPage& b) {
    if (a.retention_score_at_census != b.retention_score_at_census)
      return a.retention_score_at_census > b.retention_score_at_census;
    const uint64_t a_reclaim = ReclaimableBytes(a);
    const uint64_t b_reclaim = ReclaimableBytes(b);
    if (a_reclaim != b_reclaim)
      return a_reclaim > b_reclaim;
    const uint64_t a_move = MoveBytes(a);
    const uint64_t b_move = MoveBytes(b);
    if (a_move != b_move)
      return a_move < b_move;
    return a.page_address < b.page_address;
  });

  max_targets = std::min(max_targets, candidates.size());
  if (candidates.size() > max_targets) {
    stats_->truncated_by_cap = candidates.size() - max_targets;
    for (size_t i = max_targets; i < candidates.size(); ++i) {
      const TargetPage& tp = candidates[i];
      stats_->truncated_reclaimable_bytes +=
          uint64_t(tp.capacity_blocks - tp.blocks_at_census) * tp.block_size;
    }
    candidates.resize(max_targets);
  }

  targets_ = std::move(candidates);
  address_to_index_.reserve(targets_.size());
  // Selective skip: targets_ is sorted descending by retention_score, which
  // correlates inversely with used/capacity. Head of the vector = most
  // fragmented. Apply skip_bit only to the top fraction so high-utilization
  // targets stay refillable, shrinking lockout pressure on the workload.
  const double skip_pct = std::clamp(absl::GetFlag(FLAGS_defrag_skip_percentile), 0.0, 1.0);
  const size_t skip_count = static_cast<size_t>(static_cast<double>(targets_.size()) * skip_pct);
  for (size_t i = 0; i < targets_.size(); ++i) {
    address_to_index_[targets_[i].page_address] = i;
    if (i < skip_count) {
      // Tell mimalloc to skip this page in alloc paths; EVACUATE moves should
      // not refill pages we are about to drain.
      SetDefragSkipIfEnabled(targets_[i].page_address, true);
    }
  }
  stats_->targets_kept = targets_.size();
  pending_targets_ = targets_.size();

  for (const TargetPage& tp : targets_) {
    stats_->selected_capacity_bytes_at_census += uint64_t(tp.capacity_blocks) * tp.block_size;
    stats_->selected_used_bytes_at_census += uint64_t(tp.blocks_at_census) * tp.block_size;
    stats_->selected_reclaimable_bytes_at_census +=
        uint64_t(tp.capacity_blocks - tp.blocks_at_census) * tp.block_size;
  }
}

bool TargetPlan::Contains(uintptr_t addr) const {
  return address_to_index_.contains(addr);
}

const TargetPage* TargetPlan::Find(uintptr_t addr) const {
  const auto it = address_to_index_.find(addr);
  return it == address_to_index_.end() ? nullptr : &targets_[it->second];
}

TargetPage* TargetPlan::FindMut(uintptr_t addr) {
  const auto it = address_to_index_.find(addr);
  return it == address_to_index_.end() ? nullptr : &targets_[it->second];
}

namespace {

void AttributeBlockSkip(EvacStats& stats, RevalidationFailureReason reason, uint32_t block_size) {
  switch (reason) {
    case RevalidationFailureReason::kHeapMismatch:
      ++stats.blocks_revalidation_heap_mismatch;
      stats.bytes_revalidation_heap_mismatch += block_size;
      break;
    case RevalidationFailureReason::kActiveMallocPage:
      ++stats.blocks_revalidation_active_malloc_page;
      stats.bytes_revalidation_active_malloc_page += block_size;
      break;
    case RevalidationFailureReason::kFullPage:
      ++stats.blocks_revalidation_full_page;
      stats.bytes_revalidation_full_page += block_size;
      break;
    case RevalidationFailureReason::kAboveThreshold:
      ++stats.blocks_revalidation_above_threshold;
      stats.bytes_revalidation_above_threshold += block_size;
      break;
    case RevalidationFailureReason::kNone:
      break;
  }
}

void RecordFirstFailure(TargetPage* target, EvacStats& stats, RevalidationFailureReason reason,
                        uint32_t block_size) {
  target->revalidation_failed = true;
  target->failure_reason = reason;
  ++stats.blocks_skipped_revalidation_failed;
  stats.bytes_skipped_revalidation_failed += block_size;
  AttributeBlockSkip(stats, reason, block_size);
  switch (reason) {
    case RevalidationFailureReason::kHeapMismatch:
      ++stats.targets_revalidation_heap_mismatch;
      break;
    case RevalidationFailureReason::kActiveMallocPage:
      ++stats.targets_revalidation_active_malloc_page;
      break;
    case RevalidationFailureReason::kFullPage:
      ++stats.targets_revalidation_full_page;
      break;
    case RevalidationFailureReason::kAboveThreshold:
      ++stats.targets_revalidation_above_threshold;
      break;
    case RevalidationFailureReason::kNone:
      break;
  }
  ++stats.targets_abandoned_revalidation;
}

}  // namespace

EvacOutcome EvacDecide(TargetPlan& plan, TargetPage* target, const mi_page_usage_stats_t& stat,
                       EvacStats& stats) {
  if (target->revalidation_failed) {
    ++stats.blocks_skipped_revalidation_failed;
    stats.bytes_skipped_revalidation_failed += stat.block_size;
    AttributeBlockSkip(stats, target->failure_reason, stat.block_size);
    return EvacOutcome::kRevalidationFailed;
  }
  // Order matches the precedence in CENSUS skip-checks. The first matching
  // flag is attributed; targets_revalidation_* sums to targets_abandoned_revalidation.
  if (stat.flags & MI_DFLY_HEAP_MISMATCH) {
    RecordFirstFailure(target, stats, RevalidationFailureReason::kHeapMismatch, stat.block_size);
    SetDefragSkipIfEnabled(target->page_address, false);
    plan.NotifyTargetDone();
    return EvacOutcome::kRevalidationFailed;
  }
  if (stat.flags & MI_DFLY_PAGE_USED_FOR_MALLOC) {
    RecordFirstFailure(target, stats, RevalidationFailureReason::kActiveMallocPage,
                       stat.block_size);
    SetDefragSkipIfEnabled(target->page_address, false);
    plan.NotifyTargetDone();
    return EvacOutcome::kRevalidationFailed;
  }
  if (stat.flags & MI_DFLY_PAGE_FULL) {
    RecordFirstFailure(target, stats, RevalidationFailureReason::kFullPage, stat.block_size);
    SetDefragSkipIfEnabled(target->page_address, false);
    plan.NotifyTargetDone();
    return EvacOutcome::kRevalidationFailed;
  }
  if ((stat.flags & MI_DFLY_PAGE_BELOW_THRESHOLD) == 0) {
    RecordFirstFailure(target, stats, RevalidationFailureReason::kAboveThreshold, stat.block_size);
    SetDefragSkipIfEnabled(target->page_address, false);
    plan.NotifyTargetDone();
    return EvacOutcome::kRevalidationFailed;
  }
  if (target->blocks_evacuated >= target->blocks_at_census) {
    ++stats.blocks_skipped_target_done;
    stats.bytes_skipped_target_done += stat.block_size;
    return EvacOutcome::kTargetAlreadyDone;
  }
  ++target->blocks_evacuated;
  ++stats.blocks_move_committed;
  stats.bytes_move_committed += stat.block_size;
  if (target->blocks_evacuated == target->blocks_at_census) {
    // First-time completion: drop the skip bit so the now-drained page can be
    // reused by mi_malloc immediately if needed (no need to wait for plan
    // teardown).
    SetDefragSkipIfEnabled(target->page_address, false);
    plan.NotifyTargetDone();
    ++stats.targets_completed_during_evac;
  }
  return EvacOutcome::kCommitMove;
}

EvacOutcome EvacDecide(TargetPlan& plan, const mi_page_usage_stats_t& stat, EvacStats& stats) {
  TargetPage* target = plan.FindMut(stat.page_address);
  if (target == nullptr) {
    ++stats.blocks_skipped_not_target;
    return EvacOutcome::kNotATarget;
  }
  return EvacDecide(plan, target, stat, stats);
}

CycleProgress RunVerify(const TargetPlan& plan) {
  CycleProgress p;
  for (const TargetPage& target : plan.targets()) {
    const bool is_complete = target.blocks_evacuated >= target.blocks_at_census;
    if (target.blocks_evacuated == 0) {
      ++p.targets_no_progress;
    } else if (is_complete) {
      ++p.targets_complete;
    } else {
      ++p.targets_partial;
    }
    if (target.revalidation_failed) {
      ++p.targets_abandoned;
    }

    const uint16_t evac_clamped = std::min(target.blocks_evacuated, target.blocks_at_census);
    p.blocks_total_at_census += target.blocks_at_census;
    p.blocks_evacuated += evac_clamped;
    p.bytes_total_at_census += uint64_t(target.blocks_at_census) * target.block_size;
    p.bytes_evacuated += uint64_t(evac_clamped) * target.block_size;

    if (is_complete) {
      p.bytes_freed +=
          uint64_t(target.capacity_blocks - target.blocks_at_census) * target.block_size;
    }
  }
  p.blocks_remaining = p.blocks_total_at_census - p.blocks_evacuated;
  p.bytes_remaining = p.bytes_total_at_census - p.bytes_evacuated;
  return p;
}

CensusTaker::CensusTaker(PageCensus* census, float threshold, CycleQuota quota)
    : PageUsage(CollectPageStats::NO, threshold, quota), census_(census), threshold_(threshold) {
}

bool CensusTaker::IsPageForObjectUnderUtilized(void* object) {
  mi_page_usage_stats_t stat = mi_heap_page_is_underutilized(
      static_cast<mi_heap_t*>(zmalloc_heap), object, threshold_, /*collect_stats=*/true);
  census_->Observe(stat, current_cursor_);
  return false;
}

bool CensusTaker::IsPageForObjectUnderUtilized(mi_heap_t* heap, void* object) {
  mi_page_usage_stats_t stat =
      mi_heap_page_is_underutilized(heap, object, threshold_, /*collect_stats=*/true);
  census_->Observe(stat, current_cursor_);
  return false;
}

Evacuator::Evacuator(TargetPlan* plan, float threshold, EvacStats* evac_stats, CycleQuota quota)
    : PageUsage(CollectPageStats::NO, threshold, quota),
      plan_(plan),
      threshold_(threshold),
      evac_stats_(evac_stats) {
}

bool Evacuator::IsPageForObjectUnderUtilized(void* object) {
  const uintptr_t addr = reinterpret_cast<uintptr_t>(_mi_ptr_page(object));
  TargetPage* target = plan_->FindMut(addr);
  if (target == nullptr) {
    ++evac_stats_->blocks_skipped_not_target;
    return false;
  }
  const mi_page_usage_stats_t stat = mi_heap_page_is_underutilized(
      static_cast<mi_heap_t*>(zmalloc_heap), object, threshold_, /*collect_stats=*/true);
  return EvacDecide(*plan_, target, stat, *evac_stats_) == EvacOutcome::kCommitMove;
}

bool Evacuator::IsPageForObjectUnderUtilized(mi_heap_t* heap, void* object) {
  const uintptr_t addr = reinterpret_cast<uintptr_t>(_mi_ptr_page(object));
  TargetPage* target = plan_->FindMut(addr);
  if (target == nullptr) {
    ++evac_stats_->blocks_skipped_not_target;
    return false;
  }
  const mi_page_usage_stats_t stat =
      mi_heap_page_is_underutilized(heap, object, threshold_, /*collect_stats=*/true);
  return EvacDecide(*plan_, target, stat, *evac_stats_) == EvacOutcome::kCommitMove;
}

void DefragIdleStep(DefragTaskState* state, float threshold) {
  state->cycle_stats = {};
  state->ResetScanState();
  ++state->cycle_id;
  const uint64_t now = NowNs();
  state->cycle_start_ns = now;
  state->phase_start_ns = now;
  state->phase_active_ns = 0;

  state->census.emplace(&state->cycle_stats.census, PageCensus::kDefaultMaxRetainedPages,
                        state->per_block_move_cost_bytes);
  LOG(INFO) << absl::StrFormat("defrag[CYCLE_START] shard=%u cycle=%llu threshold=%.2f",
                               state->shard_id, state->cycle_id, threshold);
  state->phase = DefragPhase::CENSUS;
}

void DefragCensusStep(DefragTaskState* state, float threshold, CycleQuota quota,
                      const DbSliceWalker& walk) {
  const uint64_t step_start_ns = NowNs();

  // Reactive fast path: if mimalloc has flagged any pages via the underutil
  // callback, hydrate the census from that set and skip the dashtable walk.
  // Falls back to the legacy walk if the set is empty (bootstrap, or workload
  // with no recent threshold-crossing frees).
  const size_t underutil_set_size = defrag_underutil::Size();
  LOG_FIRST_N(INFO, 8) << absl::StrFormat("defrag[CENSUS_ENTRY] shard=%u cycle=%llu set_size=%zu",
                                          state->shard_id, state->cycle_id, underutil_set_size);
  if (underutil_set_size > 0) {
    auto* heap = static_cast<mi_heap_t*>(zmalloc_heap);
    const std::vector<uintptr_t> snapshot = defrag_underutil::Snapshot();
    size_t recovered = 0;
    for (uintptr_t addr : snapshot) {
      mi_page_usage_stats_t stat;
      if (!BuildPageStatFromAddress(addr, heap, threshold, &stat)) {
        defrag_underutil::Remove(addr);
        ++recovered;
        continue;
      }
      state->census->ObservePage(stat);
    }

    state->cycle_stats.census_retained_pages = state->census->pages().size();
    for (const auto& agg : state->census->pages() | std::views::values) {
      state->cycle_stats.census_potential_reclaim_bytes +=
          uint64_t(agg.capacity_blocks - agg.used_blocks) * agg.block_size;
      state->cycle_stats.census_movable_bytes_observed +=
          uint64_t(agg.observed_movable_blocks) * agg.block_size;
    }

    const uint64_t now = NowNs();
    state->phase_active_ns += now - step_start_ns;
    DEFRAG_STEP_LOG << absl::StrFormat(
        "defrag[CENSUS_REACTIVE] shard=%u cycle=%llu set_in=%zu retained=%zu recovered=%zu "
        "potential_reclaim=%s movable_observed=%s took=%.1fms cpu=%.1fms",
        state->shard_id, state->cycle_id, underutil_set_size,
        state->cycle_stats.census_retained_pages, recovered,
        FormatMiB(state->cycle_stats.census_potential_reclaim_bytes),
        FormatMiB(state->cycle_stats.census_movable_bytes_observed),
        NsToMs(now - state->phase_start_ns), NsToMs(state->phase_active_ns));

    state->phase_start_ns = now;
    state->phase_active_ns = 0;
    state->phase = DefragPhase::SELECT_TARGETS;
    return;
  }

  // Fallback: full dashtable walk.
  CensusTaker visitor(&*state->census, threshold, quota);
  const DbSliceResult result = walk(&visitor, /*hints=*/nullptr, /*hint_cursor=*/nullptr);
  state->cycle_stats.census_db_objects_scanned += result.attempts;
  if (!result.finished_all_dbs) {
    state->phase_active_ns += NowNs() - step_start_ns;
    return;
  }

  // Aggregate page-level totals here so the [CENSUS] log can report them
  // before SELECT_TARGETS runs.
  state->cycle_stats.census_retained_pages = state->census->pages().size();
  for (const auto& agg : state->census->pages() | std::views::values) {
    state->cycle_stats.census_potential_reclaim_bytes +=
        uint64_t(agg.capacity_blocks - agg.used_blocks) * agg.block_size;
    state->cycle_stats.census_movable_bytes_observed +=
        uint64_t(agg.observed_movable_blocks) * agg.block_size;
  }

  const CensusStats& c = state->cycle_stats.census;
  const uint64_t now = NowNs();
  state->phase_active_ns += now - step_start_ns;
  DEFRAG_STEP_LOG << absl::StrFormat(
      "defrag[CENSUS] shard=%u cycle=%llu db_objects=%llu retained=%zu/%zu "
      "recorded/seen=%llu/%llu cursor_hints=%zu potential_reclaim=%s movable_observed=%s "
      "skipped{above_thr=%llu full=%llu wrong_heap=%llu active=%llu low_score=%llu} "
      "topk{evicted=%llu rebuilds=%llu} took=%.1fms cpu=%.1fms",
      state->shard_id, state->cycle_id, state->cycle_stats.census_db_objects_scanned,
      state->cycle_stats.census_retained_pages, PageCensus::kDefaultMaxRetainedPages,
      c.allocations_recorded, c.allocations_seen, state->census->cursor_hints().size(),
      FormatMiB(state->cycle_stats.census_potential_reclaim_bytes),
      FormatMiB(state->cycle_stats.census_movable_bytes_observed), c.skipped_above_threshold,
      c.skipped_full_page, c.skipped_wrong_heap, c.skipped_active_malloc_page, c.skipped_low_score,
      c.pages_evicted_from_retained, c.heap_rebuilds, NsToMs(now - state->phase_start_ns),
      NsToMs(state->phase_active_ns));

  state->phase_start_ns = now;
  state->phase_active_ns = 0;
  state->phase = DefragPhase::SELECT_TARGETS;
}

void DefragSelectTargetsStep(DefragTaskState* state) {
  const uint64_t step_start_ns = NowNs();
  state->plan.emplace(&state->cycle_stats.plan);
  state->plan->BuildFrom(*state->census);
  state->cycle_stats.plan_target_pages = state->plan->size();
  // Hand the bucket-cursor hints off to the task state so EVACUATE can use
  // them after we release the census itself (the page map is large).
  state->cursor_hints = state->census->TakeCursorHints();
  state->hint_cursor_idx = 0;
  state->census.reset();
  // EVACUATE walks the prime table again from the start.
  state->ResetScanState();

  const PlanStats& p = state->cycle_stats.plan;
  const uint64_t now = NowNs();
  state->phase_active_ns += now - step_start_ns;
  DEFRAG_STEP_LOG << absl::StrFormat(
      "defrag[PLAN] shard=%u cycle=%llu targets=%zu/%zu kept=%llu reclaimable=%s "
      "filtered{no_obs=%llu stale=%llu immovable=%llu empty=%llu} truncated_by_cap=%llu "
      "filtered_immovable=%s truncated=%s took=%.1fms cpu=%.1fms",
      state->shard_id, state->cycle_id, state->cycle_stats.plan_target_pages,
      state->cycle_stats.census_retained_pages, p.targets_kept,
      FormatMiB(p.selected_reclaimable_bytes_at_census), p.filtered_no_observed_blocks,
      p.filtered_stale, p.filtered_has_immovable_data, p.filtered_already_empty, p.truncated_by_cap,
      FormatMiB(p.filtered_immovable_reclaimable_bytes), FormatMiB(p.truncated_reclaimable_bytes),
      NsToMs(now - state->phase_start_ns), NsToMs(state->phase_active_ns));

  // Skip EVAC when the prize is too small to justify the dashtable walk. The
  // underutil set is left intact: future cycles re-enter via reactive CENSUS
  // and re-plan; if churn pushes more pages below threshold, the plan grows
  // back above the bar naturally.
  const uint64_t min_reclaimable = absl::GetFlag(FLAGS_defrag_min_plan_reclaimable_bytes);
  if (p.selected_reclaimable_bytes_at_census < min_reclaimable) {
    LOG(INFO) << absl::StrFormat(
        "defrag[PLAN_SKIPPED] shard=%u cycle=%llu reclaimable=%s threshold=%s targets=%zu",
        state->shard_id, state->cycle_id, FormatMiB(p.selected_reclaimable_bytes_at_census),
        FormatMiB(min_reclaimable), state->cycle_stats.plan_target_pages);
    state->cycle_stats.cycle_finished = true;
    state->FinishCycle();
    return;
  }

  state->phase_start_ns = now;
  state->phase_active_ns = 0;
  state->phase = DefragPhase::EVACUATE;
}

void DefragEvacuateStep(DefragTaskState* state, float threshold, CycleQuota quota,
                        const DbSliceWalker& walk) {
  const uint64_t step_start_ns = NowNs();
  Evacuator visitor(&*state->plan, threshold, &state->cycle_stats.evac, quota);
  const bool use_hints = !state->cursor_hints.empty();
  const DbSliceResult result = walk(&visitor, use_hints ? &state->cursor_hints : nullptr,
                                    use_hints ? &state->hint_cursor_idx : nullptr);
  state->cycle_stats.evac_db_objects_scanned += result.attempts;
  state->cycle_stats.evac_reallocations += result.reallocations;
  if (!result.finished_all_dbs && !state->plan->AllTargetsDone()) {
    state->phase_active_ns += NowNs() - step_start_ns;
    return;
  }

  const EvacStats& e = state->cycle_stats.evac;
  const uint64_t attempted = e.blocks_move_committed + e.blocks_skipped_revalidation_failed;
  const double commit_pct =
      attempted == 0 ? 0.0 : 100.0 * static_cast<double>(e.blocks_move_committed) / attempted;
  const uint64_t now = NowNs();
  state->phase_active_ns += now - step_start_ns;
  DEFRAG_STEP_LOG << absl::StrFormat(
      "defrag[EVACUATE] shard=%u cycle=%llu db_objects=%llu reallocs=%llu "
      "commit=%llu/%llu (%.1f%%) bytes_committed=%s "
      "skipped_blocks{not_target=%llu target_done=%llu revalid=%llu} "
      "reval_fail{heap=%llu active=%llu full=%llu above_thr=%llu} "
      "abandoned=%llu completed_during_evac=%llu took=%.1fms cpu=%.1fms",
      state->shard_id, state->cycle_id, state->cycle_stats.evac_db_objects_scanned,
      state->cycle_stats.evac_reallocations, e.blocks_move_committed, attempted, commit_pct,
      FormatMiB(e.bytes_move_committed), e.blocks_skipped_not_target, e.blocks_skipped_target_done,
      e.blocks_skipped_revalidation_failed, e.targets_revalidation_heap_mismatch,
      e.targets_revalidation_active_malloc_page, e.targets_revalidation_full_page,
      e.targets_revalidation_above_threshold, e.targets_abandoned_revalidation,
      e.targets_completed_during_evac, NsToMs(now - state->phase_start_ns),
      NsToMs(state->phase_active_ns));

  state->phase_start_ns = now;
  state->phase_active_ns = 0;
  state->phase = DefragPhase::VERIFY;
}

void DefragVerifyStep(DefragTaskState* state) {
  const uint64_t step_start_ns = NowNs();
  state->cycle_stats.verify = RunVerify(*state->plan);
  state->cycle_stats.cycle_finished = true;

  const CycleProgress& v = state->cycle_stats.verify;
  // complete/partial/no_progress are mutually exclusive and cover every target;
  // abandoned is a parallel dimension (revalidation_failed) that overlaps with
  // those three, so it is not part of the denominator.
  const uint64_t total_targets = v.targets_complete + v.targets_partial + v.targets_no_progress;
  const double done_pct =
      total_targets == 0 ? 0.0 : 100.0 * static_cast<double>(v.targets_complete) / total_targets;
  const double bytes_pct =
      v.bytes_total_at_census == 0
          ? 0.0
          : 100.0 * static_cast<double>(v.bytes_evacuated) / v.bytes_total_at_census;
  const uint64_t planned_reclaim = state->cycle_stats.plan.selected_reclaimable_bytes_at_census;
  const double freed_pct =
      planned_reclaim == 0 ? 0.0 : 100.0 * static_cast<double>(v.bytes_freed) / planned_reclaim;
  const uint64_t now = NowNs();
  state->phase_active_ns += now - step_start_ns;
  DEFRAG_STEP_LOG << absl::StrFormat(
      "defrag[VERIFY] shard=%u cycle=%llu targets{done=%llu/%llu (%.1f%%) "
      "partial=%llu none=%llu abandoned=%llu} "
      "bytes{moved=%s/%s (%.1f%%) freed=%s/%s (%.1f%%) remaining=%s} took=%.1fms cpu=%.1fms",
      state->shard_id, state->cycle_id, v.targets_complete, total_targets, done_pct,
      v.targets_partial, v.targets_no_progress, v.targets_abandoned, FormatMiB(v.bytes_evacuated),
      FormatMiB(v.bytes_total_at_census), bytes_pct, FormatMiB(v.bytes_freed),
      FormatMiB(planned_reclaim), freed_pct, FormatMiB(v.bytes_remaining),
      NsToMs(now - state->phase_start_ns), NsToMs(state->phase_active_ns));

  const double cycle_ms = NsToMs(now - state->cycle_start_ns);
  const double freed_mib_per_s =
      cycle_ms <= 0.0
          ? 0.0
          : (static_cast<double>(v.bytes_freed) / (1024.0 * 1024.0)) / (cycle_ms / 1000.0);
  LOG(INFO) << absl::StrFormat(
      "defrag[CYCLE_DONE] shard=%u cycle=%llu targets_done=%llu/%llu (%.1f%%) "
      "bytes_freed=%s/%s (%.1f%%) bytes_moved=%s cycle_took=%.1fms freed_rate=%.1fMiB/s",
      state->shard_id, state->cycle_id, v.targets_complete, total_targets, done_pct,
      FormatMiB(v.bytes_freed), FormatMiB(planned_reclaim), freed_pct, FormatMiB(v.bytes_evacuated),
      cycle_ms, freed_mib_per_s);

  state->FinishCycle();
}

namespace {

struct StepTransition {
  DefragPhase before;
  DefragPhase after;
};

StepTransition RunPhaseStep(DefragTaskState* state, float threshold, CycleQuota quota,
                            const DbSliceWalker& walk) {
  const DefragPhase before = state->phase;
  switch (state->phase) {
    case DefragPhase::IDLE:
      DefragIdleStep(state, threshold);
      break;
    case DefragPhase::CENSUS:
      DefragCensusStep(state, threshold, quota, walk);
      break;
    case DefragPhase::SELECT_TARGETS:
      DefragSelectTargetsStep(state);
      break;
    case DefragPhase::EVACUATE:
      DefragEvacuateStep(state, threshold, quota, walk);
      break;
    case DefragPhase::VERIFY:
      DefragVerifyStep(state);
      break;
  }
  return {before, state->phase};
}

bool CycleEnded(StepTransition t) {
  if (t.after != DefragPhase::IDLE)
    return false;
  // Normal end (VERIFY -> IDLE) and PLAN_SKIPPED bail-out (SELECT_TARGETS ->
  // IDLE) both terminate the cycle.
  return t.before == DefragPhase::VERIFY || t.before == DefragPhase::SELECT_TARGETS;
}

}  // namespace

void RunPhaseDefrag(DefragTaskState* state, float threshold, CycleQuota quota,
                    const DbSliceWalker& walk) {
  StepTransition t;
  do {
    t = RunPhaseStep(state, threshold, quota, walk);
  } while (!quota.Depleted() && !CycleEnded(t));
}

}  // namespace dfly
