// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/page_usage/page_usage_visitors.h"

#include <absl/flags/flag.h>

#define MI_BUILD_RELEASE 1
#include <mimalloc/internal.h>

#include <algorithm>
#include <ranges>
#include <utility>

#include "base/flags.h"
#include "base/logging.h"

extern "C" {
#include "redis/zmalloc.h"
}

ABSL_FLAG(bool, defrag_use_skip_bit, false,
          "If true, mark target pages with mimalloc's defrag_skip bit so EVAC moves don't "
          "refill them. Disable to A/B compare against an unmarked baseline.");

ABSL_FLAG(bool, defrag_keys, false,
          "If true, the phased defragmenter also defragments key allocations "
          "(it->first) in addition to values. Set to false to measure the "
          "incremental benefit of key defrag.");

ABSL_FLAG(double, defrag_skip_percentile, 0.5,
          "Fraction of the target plan (sorted by retention_score, most-fragmented first) "
          "to apply the mimalloc defrag_skip bit to. 0.5 (default) marks the top half "
          "and lets the bottom half stay refillable, which empirically gives the best "
          "floor-vs-bulge tradeoff. 1.0 marks every target (max reclaim, biggest bulge). "
          "Lower values shrink the lockout footprint: only the most-fragmented top-K "
          "pages are protected from refill while higher-utilization targets stay "
          "refillable.");

extern "C" {
// Dragonfly mimalloc patch: tell mi_malloc to skip a page during defrag so
// EVACUATE moves don't refill pages we're trying to drain.
void mi_page_set_defrag_skip(uintptr_t page_addr, bool skip);
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
  if (agg.observed_movable_blocks < agg.used_blocks)
    return TargetFilterReason::kHasImmovableData;
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

std::vector<uint64_t> PageCensus::TakeCursorHints() {
  std::vector out(cursor_hints_.begin(), cursor_hints_.end());
  cursor_hints_.clear();
  std::ranges::sort(out);
  return out;
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

bool CensusTaker::ShouldDefragKeys() const {
  return ::absl::GetFlag(FLAGS_defrag_keys);
}

Evacuator::Evacuator(TargetPlan* plan, float threshold, EvacStats* evac_stats, CycleQuota quota)
    : PageUsage(CollectPageStats::NO, threshold, quota),
      plan_(plan),
      threshold_(threshold),
      evac_stats_(evac_stats) {
}

bool Evacuator::ShouldDefragKeys() const {
  return ::absl::GetFlag(FLAGS_defrag_keys);
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

}  // namespace dfly
