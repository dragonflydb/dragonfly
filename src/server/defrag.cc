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
#include <ranges>

#include "base/flags.h"
#include "base/logging.h"
#include "core/page_usage/page_usage_visitors.h"

ABSL_FLAG(uint64_t, defrag_min_plan_reclaimable_bytes, 64u << 20,
          "Minimum bytes-reclaimable threshold the SELECT_TARGETS plan must hit to "
          "justify running EVACUATE. Below this, the cycle is skipped (PLAN_SKIPPED) "
          "and we return to IDLE without walking the dashtable. The underutil set is "
          "left intact so the next cycle picks it up if churn refills the pages above "
          "threshold (or new fragmentation appears). Default 64 MiB.");

extern "C" {
#include "redis/zmalloc.h"
// Dragonfly mimalloc patch: per-process callback fired by mi_free_block_local
// when a page's used count crosses below the configured underutil threshold.
typedef void (*mi_dfly_underutil_callback_t)(uintptr_t page_addr);
void mi_dfly_set_underutil_callback(mi_dfly_underutil_callback_t cb);
void mi_dfly_set_underutil_threshold_pct(uint8_t pct);
}

namespace dfly {

namespace {

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

bool IsPageMaybeUnderutil(uintptr_t page_addr) {
  if (tl_underutil_pages.empty()) {
    return true;  // bootstrap: no info, fall through to original mimalloc check
  }
  return tl_underutil_pages.contains(page_addr);
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
