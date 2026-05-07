// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/defrag.h"

#include <absl/container/flat_hash_map.h>
#include <mimalloc.h>
#include <mimalloc/internal.h>

#include <vector>

#include "base/flags.h"
#include "base/gtest.h"
#include "base/logging.h"
#include "core/page_usage/page_usage_visitors.h"

ABSL_DECLARE_FLAG(bool, defrag_use_skip_bit);

extern "C" {
#include "redis/zmalloc.h"
mi_page_usage_stats_t mi_heap_page_is_underutilized(mi_heap_t* heap, void* p, float ratio,
                                                    bool collect_stats);
}

namespace dfly {

namespace {

mi_page_usage_stats_t MakeStat(uintptr_t addr, uint16_t capacity, uint16_t used,
                               uint8_t flags = MI_DFLY_PAGE_BELOW_THRESHOLD,
                               size_t block_size = 64) {
  mi_page_usage_stats_t s{};
  s.page_address = addr;
  s.block_size = block_size;
  s.capacity = capacity;
  s.used = used;
  s.flags = flags;
  return s;
}

}  // namespace

TEST(PageCensusEvictionTest, EvictsLowestScorePageWhenOverCap) {
  if (!PageCensus::kEnableTopK) {
    GTEST_SKIP() << "PageCensus::kEnableTopK is false; eviction path inactive";
  }
  CensusStats cstats;
  PageCensus census(&cstats, /*max_retained_pages=*/4);

  // Scores (cap/used): 5.0, 3.33, 2.5, 2.0 — page 4 is the lowest.
  census.Observe(MakeStat(/*addr=*/1, /*capacity=*/10, /*used=*/2));
  census.Observe(MakeStat(/*addr=*/2, /*capacity=*/10, /*used=*/3));
  census.Observe(MakeStat(/*addr=*/3, /*capacity=*/10, /*used=*/4));
  census.Observe(MakeStat(/*addr=*/4, /*capacity=*/10, /*used=*/5));

  ASSERT_EQ(census.pages().size(), 4u);
  EXPECT_EQ(census.stats().pages_evicted_from_retained, 0u);

  // New page with score 2.5 — pushes us over cap, page 4 (score 2.0) should be evicted.
  census.Observe(MakeStat(/*addr=*/5, /*capacity=*/10, /*used=*/4));

  EXPECT_EQ(census.pages().size(), 4u);
  EXPECT_EQ(census.stats().pages_evicted_from_retained, 1u);
  EXPECT_TRUE(census.pages().contains(1));
  EXPECT_TRUE(census.pages().contains(2));
  EXPECT_TRUE(census.pages().contains(3));
  EXPECT_FALSE(census.pages().contains(4));
  EXPECT_TRUE(census.pages().contains(5));
}

TEST(PageCensusEvictionTest, StaleHeapEntryDoesNotEvictWrongPage) {
  if (!PageCensus::kEnableTopK) {
    GTEST_SKIP() << "PageCensus::kEnableTopK is false; eviction path inactive";
  }
  CensusStats cstats;
  PageCensus census(&cstats, /*max_retained_pages=*/4);

  // Scores: 5.0, 3.33, 2.5, 2.0. Page 4 starts as the lowest.
  census.Observe(MakeStat(/*addr=*/1, /*capacity=*/10, /*used=*/2));
  census.Observe(MakeStat(/*addr=*/2, /*capacity=*/10, /*used=*/3));
  census.Observe(MakeStat(/*addr=*/3, /*capacity=*/10, /*used=*/4));
  census.Observe(MakeStat(/*addr=*/4, /*capacity=*/10, /*used=*/5));

  // Re-observe page 4 with a much higher score (5.0). The old heap entry
  // (score 2.0, gen=1) is now stale — page 3 (score 2.5) is genuinely lowest.
  census.Observe(MakeStat(/*addr=*/4, /*capacity=*/20, /*used=*/4));

  ASSERT_EQ(census.pages().size(), 4u);
  EXPECT_EQ(census.stats().pages_evicted_from_retained, 0u);

  // New page with score 5.0 — pushes us over cap. Lazy-pop should skip the
  // stale page-4 entry and evict page 3 (the genuinely-lowest live page,
  // score 2.5). Use a score strictly above page 3's so page 3 is the
  // unambiguous next-lowest after the stale skip.
  census.Observe(MakeStat(/*addr=*/6, /*capacity=*/10, /*used=*/2));

  EXPECT_EQ(census.pages().size(), 4u);
  EXPECT_EQ(census.stats().pages_evicted_from_retained, 1u);
  EXPECT_TRUE(census.pages().contains(1));
  EXPECT_TRUE(census.pages().contains(2));
  EXPECT_FALSE(census.pages().contains(3));
  EXPECT_TRUE(census.pages().contains(4));
  EXPECT_TRUE(census.pages().contains(6));
}

TEST(PageCensusEvictionTest, RebuildFiresWhenHeapDoublesCap) {
  if (!PageCensus::kEnableTopK) {
    GTEST_SKIP() << "PageCensus::kEnableTopK is false; heap rebuild inactive";
  }
  CensusStats cstats;
  PageCensus census(&cstats, /*max_retained_pages=*/4);

  // Fill to cap. Heap size = 4.
  census.Observe(MakeStat(/*addr=*/1, /*capacity=*/10, /*used=*/2));
  census.Observe(MakeStat(/*addr=*/2, /*capacity=*/10, /*used=*/3));
  census.Observe(MakeStat(/*addr=*/3, /*capacity=*/10, /*used=*/4));
  census.Observe(MakeStat(/*addr=*/4, /*capacity=*/10, /*used=*/5));

  EXPECT_EQ(census.stats().heap_rebuilds, 0u);

  // Re-observe page 1 repeatedly. Each call pushes a new heap entry without
  // changing pages_.size(). Heap grows: 5, 6, 7, 8, 9 — rebuild fires once
  // we cross 2 * max_retained_pages_ (i.e., > 8).
  for (int i = 0; i < 5; ++i) {
    census.Observe(MakeStat(/*addr=*/1, /*capacity=*/10, /*used=*/2));
  }

  EXPECT_GE(census.stats().heap_rebuilds, 1u);
  EXPECT_EQ(census.pages().size(), 4u);
  EXPECT_EQ(census.stats().pages_evicted_from_retained, 0u);
}

TEST(PageCensusEvictionTest, RejectsNewPageWithScoreBelowWorstRetained) {
  if (!PageCensus::kEnableTopK) {
    GTEST_SKIP() << "PageCensus::kEnableTopK is false; reject-on-cap inactive";
  }
  CensusStats cstats;
  PageCensus census(&cstats, /*max_retained_pages=*/4);

  // Fill to cap with scores 5.0, 3.33, 2.5, 2.0. Worst retained is page 4 at 2.0.
  census.Observe(MakeStat(/*addr=*/1, /*capacity=*/10, /*used=*/2));
  census.Observe(MakeStat(/*addr=*/2, /*capacity=*/10, /*used=*/3));
  census.Observe(MakeStat(/*addr=*/3, /*capacity=*/10, /*used=*/4));
  census.Observe(MakeStat(/*addr=*/4, /*capacity=*/10, /*used=*/5));

  ASSERT_EQ(census.pages().size(), 4u);
  EXPECT_EQ(census.stats().skipped_low_score, 0u);

  // New page with score 1.42 (cap=10, used=7) — strictly below the worst retained.
  // Should be rejected upfront: not inserted, no eviction, no allocations_recorded bump.
  const uint64_t recorded_before = census.stats().allocations_recorded;
  census.Observe(MakeStat(/*addr=*/5, /*capacity=*/10, /*used=*/7));

  EXPECT_EQ(census.pages().size(), 4u);
  EXPECT_FALSE(census.pages().contains(5));
  EXPECT_EQ(census.stats().skipped_low_score, 1u);
  EXPECT_EQ(census.stats().pages_evicted_from_retained, 0u);
  EXPECT_EQ(census.stats().allocations_recorded, recorded_before);
}

namespace {

// Drives N observations on a single page so that observed_movable_blocks lands at N
// and used_blocks/capacity reflect the last call.
void ObserveTimes(PageCensus& census, int times, uintptr_t addr, uint16_t capacity, uint16_t used) {
  for (int i = 0; i < times; ++i) {
    census.Observe(MakeStat(addr, capacity, used));
  }
}

}  // namespace

TEST(TargetPlanTest, AppliesFilterClassification) {
  CensusStats cstats;
  PageCensus census(&cstats);

  // KEEP: movable == used.
  ObserveTimes(census, /*times=*/4, /*addr=*/1, /*capacity=*/10, /*used=*/4);
  // kAlreadyEmpty: used == 0.
  ObserveTimes(census, /*times=*/1, /*addr=*/2, /*capacity=*/10, /*used=*/0);
  // kStaleObservation: movable (5) > used (3).
  ObserveTimes(census, /*times=*/5, /*addr=*/3, /*capacity=*/10, /*used=*/3);
  // kHasImmovableData: movable (2) < used (5).
  ObserveTimes(census, /*times=*/2, /*addr=*/4, /*capacity=*/10, /*used=*/5);

  PlanStats pstats;
  TargetPlan plan(&pstats);
  plan.BuildFrom(census);

  EXPECT_EQ(plan.size(), 1u);
  EXPECT_EQ(plan.stats().targets_kept, 1u);
  EXPECT_EQ(plan.stats().filtered_already_empty, 1u);
  EXPECT_EQ(plan.stats().filtered_stale, 1u);
  EXPECT_EQ(plan.stats().filtered_has_immovable_data, 1u);
  EXPECT_EQ(plan.stats().filtered_no_observed_blocks, 0u);
  EXPECT_EQ(plan.stats().truncated_by_cap, 0u);
  EXPECT_TRUE(plan.Contains(1));
  EXPECT_FALSE(plan.Contains(2));
  EXPECT_FALSE(plan.Contains(3));
  EXPECT_FALSE(plan.Contains(4));
}

TEST(TargetPlanTest, SortsByScoreDescending) {
  CensusStats cstats;
  PageCensus census(&cstats);

  // Three KEEP-eligible pages with distinct scores: 5.0, 2.5, 2.0.
  ObserveTimes(census, /*times=*/2, /*addr=*/100, /*capacity=*/10, /*used=*/2);
  ObserveTimes(census, /*times=*/4, /*addr=*/200, /*capacity=*/10, /*used=*/4);
  ObserveTimes(census, /*times=*/5, /*addr=*/300, /*capacity=*/10, /*used=*/5);

  PlanStats pstats;
  TargetPlan plan(&pstats);
  plan.BuildFrom(census);

  ASSERT_EQ(plan.size(), 3u);
  EXPECT_EQ(plan.targets()[0].page_address, 100u);
  EXPECT_EQ(plan.targets()[1].page_address, 200u);
  EXPECT_EQ(plan.targets()[2].page_address, 300u);
  EXPECT_GT(plan.targets()[0].retention_score_at_census,
            plan.targets()[1].retention_score_at_census);
  EXPECT_GT(plan.targets()[1].retention_score_at_census,
            plan.targets()[2].retention_score_at_census);
}

TEST(TargetPlanTest, TruncatesToMaxTargets) {
  CensusStats cstats;
  PageCensus census(&cstats);

  // Four KEEP-eligible pages with descending scores.
  ObserveTimes(census, /*times=*/2, /*addr=*/100, /*capacity=*/10, /*used=*/2);  // 5.0
  ObserveTimes(census, /*times=*/3, /*addr=*/200, /*capacity=*/10, /*used=*/3);  // 3.33
  ObserveTimes(census, /*times=*/4, /*addr=*/300, /*capacity=*/10, /*used=*/4);  // 2.5
  ObserveTimes(census, /*times=*/5, /*addr=*/400, /*capacity=*/10, /*used=*/5);  // 2.0

  PlanStats pstats;
  TargetPlan plan(&pstats);
  plan.BuildFrom(census, /*max_targets=*/2);

  EXPECT_EQ(plan.size(), 2u);
  EXPECT_EQ(plan.stats().targets_kept, 2u);
  EXPECT_EQ(plan.stats().truncated_by_cap, 2u);
  EXPECT_TRUE(plan.Contains(100));
  EXPECT_TRUE(plan.Contains(200));
  EXPECT_FALSE(plan.Contains(300));
  EXPECT_FALSE(plan.Contains(400));
}

TEST(TargetPlanTest, AddressIndexLookup) {
  CensusStats cstats;
  PageCensus census(&cstats);
  ObserveTimes(census, /*times=*/2, /*addr=*/0x1000, /*capacity=*/10, /*used=*/2);
  ObserveTimes(census, /*times=*/4, /*addr=*/0x2000, /*capacity=*/10, /*used=*/4);

  PlanStats pstats;
  TargetPlan plan(&pstats);
  plan.BuildFrom(census);

  ASSERT_EQ(plan.size(), 2u);

  const TargetPage* found = plan.Find(0x1000);
  ASSERT_NE(found, nullptr);
  EXPECT_EQ(found->page_address, 0x1000u);
  EXPECT_EQ(found->blocks_at_census, 2u);
  EXPECT_EQ(found->capacity_blocks, 10u);

  EXPECT_EQ(plan.Find(0xDEAD), nullptr);
  EXPECT_FALSE(plan.Contains(0xDEAD));
}

TEST(TargetPlanTest, BuildFromIsIdempotent) {
  CensusStats cstats1;
  PageCensus census1(&cstats1);
  ObserveTimes(census1, /*times=*/2, /*addr=*/100, /*capacity=*/10, /*used=*/2);
  ObserveTimes(census1, /*times=*/4, /*addr=*/200, /*capacity=*/10, /*used=*/4);

  PlanStats pstats;
  TargetPlan plan(&pstats);
  plan.BuildFrom(census1);
  ASSERT_EQ(plan.size(), 2u);

  // Second census with different pages — plan should fully replace its state.
  CensusStats cstats2;
  PageCensus census2(&cstats2);
  ObserveTimes(census2, /*times=*/3, /*addr=*/500, /*capacity=*/10, /*used=*/3);

  plan.BuildFrom(census2);
  EXPECT_EQ(plan.size(), 1u);
  EXPECT_TRUE(plan.Contains(500));
  EXPECT_FALSE(plan.Contains(100));
  EXPECT_FALSE(plan.Contains(200));
  EXPECT_EQ(plan.stats().targets_kept, 1u);
}

namespace {

// Convenience: stat for an eligible page (BELOW_THRESHOLD set, no other dfly flags).
mi_page_usage_stats_t EligibleStat(uintptr_t addr) {
  return MakeStat(addr, /*capacity=*/10, /*used=*/4);
}

}  // namespace

TEST(EvacDecideTest, NotATargetWhenPageMissingFromPlan) {
  CensusStats cstats;
  PageCensus census(&cstats);
  ObserveTimes(census, /*times=*/4, /*addr=*/100, /*capacity=*/10, /*used=*/4);
  PlanStats pstats;
  TargetPlan plan(&pstats);
  plan.BuildFrom(census);

  EvacStats stats;
  EvacOutcome outcome = EvacDecide(plan, EligibleStat(/*addr=*/0xDEAD), stats);

  EXPECT_EQ(outcome, EvacOutcome::kNotATarget);
  EXPECT_EQ(stats.blocks_skipped_not_target, 1u);
  EXPECT_EQ(stats.blocks_skipped_target_done, 0u);
  EXPECT_EQ(stats.blocks_skipped_revalidation_failed, 0u);
  EXPECT_EQ(stats.blocks_move_committed, 0u);
}

TEST(EvacDecideTest, CommitsMoveAndBumpsCountersOnFirstCall) {
  CensusStats cstats;
  PageCensus census(&cstats);
  ObserveTimes(census, /*times=*/4, /*addr=*/100, /*capacity=*/10, /*used=*/4);
  PlanStats pstats;
  TargetPlan plan(&pstats);
  plan.BuildFrom(census);
  ASSERT_EQ(plan.size(), 1u);

  EvacStats stats;
  EvacOutcome outcome = EvacDecide(plan, EligibleStat(/*addr=*/100), stats);

  EXPECT_EQ(outcome, EvacOutcome::kCommitMove);
  EXPECT_EQ(stats.blocks_move_committed, 1u);
  EXPECT_EQ(stats.blocks_skipped_not_target, 0u);
  EXPECT_EQ(stats.blocks_skipped_target_done, 0u);
  EXPECT_EQ(stats.blocks_skipped_revalidation_failed, 0u);
  const TargetPage* target = plan.Find(100);
  ASSERT_NE(target, nullptr);
  EXPECT_EQ(target->blocks_evacuated, 1u);
  EXPECT_FALSE(target->revalidation_failed);
}

TEST(EvacDecideTest, ReturnsTargetAlreadyDoneOnceCounterReachesCensus) {
  CensusStats cstats;
  PageCensus census(&cstats);
  ObserveTimes(census, /*times=*/2, /*addr=*/100, /*capacity=*/10, /*used=*/2);
  PlanStats pstats;
  TargetPlan plan(&pstats);
  plan.BuildFrom(census);
  ASSERT_EQ(plan.Find(100)->blocks_at_census, 2u);

  EvacStats stats;
  EXPECT_EQ(EvacDecide(plan, EligibleStat(100), stats), EvacOutcome::kCommitMove);
  EXPECT_EQ(EvacDecide(plan, EligibleStat(100), stats), EvacOutcome::kCommitMove);
  EXPECT_EQ(EvacDecide(plan, EligibleStat(100), stats), EvacOutcome::kTargetAlreadyDone);

  EXPECT_EQ(stats.blocks_move_committed, 2u);
  EXPECT_EQ(stats.blocks_skipped_target_done, 1u);
  EXPECT_EQ(plan.Find(100)->blocks_evacuated, 2u);
}

TEST(EvacDecideTest, MultiTargetCountersAreIndependent) {
  CensusStats cstats;
  PageCensus census(&cstats);
  ObserveTimes(census, /*times=*/3, /*addr=*/100, /*capacity=*/10, /*used=*/3);
  ObserveTimes(census, /*times=*/2, /*addr=*/200, /*capacity=*/10, /*used=*/2);
  PlanStats pstats;
  TargetPlan plan(&pstats);
  plan.BuildFrom(census);

  EvacStats stats;
  EXPECT_EQ(EvacDecide(plan, EligibleStat(100), stats), EvacOutcome::kCommitMove);
  EXPECT_EQ(EvacDecide(plan, EligibleStat(200), stats), EvacOutcome::kCommitMove);
  EXPECT_EQ(EvacDecide(plan, EligibleStat(100), stats), EvacOutcome::kCommitMove);
  EXPECT_EQ(EvacDecide(plan, EligibleStat(200), stats), EvacOutcome::kCommitMove);
  EXPECT_EQ(EvacDecide(plan, EligibleStat(200), stats), EvacOutcome::kTargetAlreadyDone);

  EXPECT_EQ(plan.Find(100)->blocks_evacuated, 2u);
  EXPECT_EQ(plan.Find(200)->blocks_evacuated, 2u);
  EXPECT_EQ(stats.blocks_move_committed, 4u);
  EXPECT_EQ(stats.blocks_skipped_target_done, 1u);
}

TEST(EvacDecideTest, RevalidationFailsForIneligibleFlags) {
  CensusStats cstats;
  PageCensus census(&cstats);
  ObserveTimes(census, /*times=*/4, /*addr=*/100, /*capacity=*/10, /*used=*/4);
  PlanStats pstats;
  TargetPlan plan(&pstats);
  plan.BuildFrom(census);

  // Page is now FULL — no longer a defrag candidate.
  EvacStats stats;
  mi_page_usage_stats_t bad =
      MakeStat(/*addr=*/100, /*capacity=*/10, /*used=*/4, /*flags=*/MI_DFLY_PAGE_FULL);
  EXPECT_EQ(EvacDecide(plan, bad, stats), EvacOutcome::kRevalidationFailed);
  EXPECT_EQ(stats.blocks_skipped_revalidation_failed, 1u);
  EXPECT_TRUE(plan.Find(100)->revalidation_failed);
  EXPECT_EQ(plan.Find(100)->blocks_evacuated, 0u);
}

TEST(EvacDecideTest, RevalidationFailureIsSticky) {
  CensusStats cstats;
  PageCensus census(&cstats);
  ObserveTimes(census, /*times=*/4, /*addr=*/100, /*capacity=*/10, /*used=*/4);
  PlanStats pstats;
  TargetPlan plan(&pstats);
  plan.BuildFrom(census);

  EvacStats stats;
  // First call: page is no longer below threshold (flags=0). Sticky flag set.
  mi_page_usage_stats_t bad = MakeStat(/*addr=*/100, /*capacity=*/10, /*used=*/9, /*flags=*/0);
  EXPECT_EQ(EvacDecide(plan, bad, stats), EvacOutcome::kRevalidationFailed);

  // Subsequent calls — even with eligible flags — still fail via the sticky path.
  EXPECT_EQ(EvacDecide(plan, EligibleStat(100), stats), EvacOutcome::kRevalidationFailed);
  EXPECT_EQ(EvacDecide(plan, EligibleStat(100), stats), EvacOutcome::kRevalidationFailed);

  EXPECT_EQ(stats.blocks_skipped_revalidation_failed, 3u);
  EXPECT_EQ(stats.blocks_move_committed, 0u);
  EXPECT_EQ(plan.Find(100)->blocks_evacuated, 0u);
}

TEST(EvacDecideTest, RevalidationBreakdownAttributesBlocksToOriginatingReason) {
  CensusStats cstats;
  PageCensus census(&cstats);
  ObserveTimes(census, /*times=*/4, /*addr=*/100, /*capacity=*/10, /*used=*/4);
  ObserveTimes(census, /*times=*/4, /*addr=*/200, /*capacity=*/10, /*used=*/4);
  PlanStats pstats;
  TargetPlan plan(&pstats);
  plan.BuildFrom(census);

  EvacStats stats;
  constexpr uint32_t kBlockSize = 64;

  // Target 100 fails with HEAP_MISMATCH; revisit twice on sticky path.
  mi_page_usage_stats_t mismatch =
      MakeStat(/*addr=*/100, /*capacity=*/10, /*used=*/4, /*flags=*/MI_DFLY_HEAP_MISMATCH);
  EXPECT_EQ(EvacDecide(plan, mismatch, stats), EvacOutcome::kRevalidationFailed);
  EXPECT_EQ(EvacDecide(plan, EligibleStat(100), stats), EvacOutcome::kRevalidationFailed);
  EXPECT_EQ(EvacDecide(plan, EligibleStat(100), stats), EvacOutcome::kRevalidationFailed);

  // Target 200 fails with PAGE_FULL; revisit once.
  mi_page_usage_stats_t full =
      MakeStat(/*addr=*/200, /*capacity=*/10, /*used=*/4, /*flags=*/MI_DFLY_PAGE_FULL);
  EXPECT_EQ(EvacDecide(plan, full, stats), EvacOutcome::kRevalidationFailed);
  EXPECT_EQ(EvacDecide(plan, EligibleStat(200), stats), EvacOutcome::kRevalidationFailed);

  // Target-grain counters: one each.
  EXPECT_EQ(stats.targets_revalidation_heap_mismatch, 1u);
  EXPECT_EQ(stats.targets_revalidation_full_page, 1u);
  EXPECT_EQ(stats.targets_revalidation_active_malloc_page, 0u);
  EXPECT_EQ(stats.targets_revalidation_above_threshold, 0u);

  // Block-grain breakdown: 3 blocks attributed to heap_mismatch, 2 to full_page.
  EXPECT_EQ(stats.blocks_revalidation_heap_mismatch, 3u);
  EXPECT_EQ(stats.blocks_revalidation_full_page, 2u);
  EXPECT_EQ(stats.blocks_revalidation_active_malloc_page, 0u);
  EXPECT_EQ(stats.blocks_revalidation_above_threshold, 0u);

  // Bytes mirror blocks * block_size.
  EXPECT_EQ(stats.bytes_revalidation_heap_mismatch, 3u * kBlockSize);
  EXPECT_EQ(stats.bytes_revalidation_full_page, 2u * kBlockSize);

  // Aggregates equal the sum of the breakdown.
  EXPECT_EQ(stats.blocks_skipped_revalidation_failed, 5u);
  EXPECT_EQ(stats.bytes_skipped_revalidation_failed, 5u * kBlockSize);
  EXPECT_EQ(stats.blocks_revalidation_heap_mismatch + stats.blocks_revalidation_full_page,
            stats.blocks_skipped_revalidation_failed);
}

TEST(EvacDecideTest, AllTargetsDoneTrueForEmptyPlan) {
  PlanStats pstats;
  TargetPlan plan(&pstats);
  EXPECT_TRUE(plan.AllTargetsDone());
}

TEST(EvacDecideTest, AllTargetsDoneFalseAfterBuild) {
  CensusStats cstats;
  PageCensus census(&cstats);
  ObserveTimes(census, /*times=*/4, /*addr=*/100, /*capacity=*/10, /*used=*/4);
  PlanStats pstats;
  TargetPlan plan(&pstats);
  plan.BuildFrom(census);
  ASSERT_EQ(plan.size(), 1u);
  EXPECT_FALSE(plan.AllTargetsDone());
}

TEST(EvacDecideTest, CompletionFlipsAllTargetsDone) {
  CensusStats cstats;
  PageCensus census(&cstats);
  // Single target with blocks_at_census = 2.
  ObserveTimes(census, /*times=*/2, /*addr=*/100, /*capacity=*/10, /*used=*/2);
  PlanStats pstats;
  TargetPlan plan(&pstats);
  plan.BuildFrom(census);

  EvacStats stats;
  EXPECT_FALSE(plan.AllTargetsDone());
  EXPECT_EQ(EvacDecide(plan, EligibleStat(100), stats), EvacOutcome::kCommitMove);
  EXPECT_FALSE(plan.AllTargetsDone());  // 1 of 2 done, target still pending
  EXPECT_EQ(EvacDecide(plan, EligibleStat(100), stats), EvacOutcome::kCommitMove);
  EXPECT_TRUE(plan.AllTargetsDone());  // 2 of 2 done — target completed
  // Subsequent calls don't double-decrement.
  EXPECT_EQ(EvacDecide(plan, EligibleStat(100), stats), EvacOutcome::kTargetAlreadyDone);
  EXPECT_TRUE(plan.AllTargetsDone());
}

TEST(EvacDecideTest, RevalidationFailureFlipsAllTargetsDone) {
  CensusStats cstats;
  PageCensus census(&cstats);
  ObserveTimes(census, /*times=*/4, /*addr=*/100, /*capacity=*/10, /*used=*/4);
  PlanStats pstats;
  TargetPlan plan(&pstats);
  plan.BuildFrom(census);

  EvacStats stats;
  EXPECT_FALSE(plan.AllTargetsDone());
  mi_page_usage_stats_t bad =
      MakeStat(/*addr=*/100, /*capacity=*/10, /*used=*/4, /*flags=*/MI_DFLY_PAGE_FULL);
  EXPECT_EQ(EvacDecide(plan, bad, stats), EvacOutcome::kRevalidationFailed);
  EXPECT_TRUE(plan.AllTargetsDone());  // single target now abandoned
  // Subsequent sticky calls don't double-decrement.
  EXPECT_EQ(EvacDecide(plan, EligibleStat(100), stats), EvacOutcome::kRevalidationFailed);
  EXPECT_TRUE(plan.AllTargetsDone());
}

TEST(EvacDecideTest, AllTargetsDoneOnlyWhenEveryTargetSettled) {
  CensusStats cstats;
  PageCensus census(&cstats);
  ObserveTimes(census, /*times=*/2, /*addr=*/100, /*capacity=*/10, /*used=*/2);
  ObserveTimes(census, /*times=*/2, /*addr=*/200, /*capacity=*/10, /*used=*/2);
  PlanStats pstats;
  TargetPlan plan(&pstats);
  plan.BuildFrom(census);
  ASSERT_EQ(plan.size(), 2u);

  EvacStats stats;
  EXPECT_FALSE(plan.AllTargetsDone());

  // Complete target 100 fully — plan still has work pending on 200.
  EXPECT_EQ(EvacDecide(plan, EligibleStat(100), stats), EvacOutcome::kCommitMove);
  EXPECT_EQ(EvacDecide(plan, EligibleStat(100), stats), EvacOutcome::kCommitMove);
  EXPECT_FALSE(plan.AllTargetsDone());

  // Mix: revalidation-fail target 200 — plan now fully settled.
  mi_page_usage_stats_t bad =
      MakeStat(/*addr=*/200, /*capacity=*/10, /*used=*/2, /*flags=*/MI_DFLY_PAGE_FULL);
  EXPECT_EQ(EvacDecide(plan, bad, stats), EvacOutcome::kRevalidationFailed);
  EXPECT_TRUE(plan.AllTargetsDone());
}

TEST(VerifyTest, EmptyPlanGivesZeros) {
  PlanStats pstats;
  TargetPlan plan(&pstats);
  CycleProgress p = RunVerify(plan);
  EXPECT_EQ(p.targets_complete, 0u);
  EXPECT_EQ(p.targets_partial, 0u);
  EXPECT_EQ(p.targets_no_progress, 0u);
}

TEST(VerifyTest, ClassifiesByBlocksEvacuated) {
  CensusStats cstats;
  PageCensus census(&cstats);
  // Three targets, each with blocks_at_census = N (since movable == used).
  ObserveTimes(census, /*times=*/4, /*addr=*/100, /*capacity=*/10, /*used=*/4);  // complete
  ObserveTimes(census, /*times=*/4, /*addr=*/200, /*capacity=*/10, /*used=*/4);  // partial
  ObserveTimes(census, /*times=*/4, /*addr=*/300, /*capacity=*/10, /*used=*/4);  // no progress

  PlanStats pstats;
  TargetPlan plan(&pstats);
  plan.BuildFrom(census);
  ASSERT_EQ(plan.size(), 3u);

  // Simulate EVACUATE outcomes via direct mutation through FindMut.
  plan.FindMut(100)->blocks_evacuated = 4;  // == blocks_at_census → complete
  plan.FindMut(200)->blocks_evacuated = 2;  // 0 < x < blocks_at_census → partial
  plan.FindMut(300)->blocks_evacuated = 0;  // → no progress

  CycleProgress p = RunVerify(plan);
  EXPECT_EQ(p.targets_complete, 1u);
  EXPECT_EQ(p.targets_partial, 1u);
  EXPECT_EQ(p.targets_no_progress, 1u);
}

TEST(VerifyTest, OvershootCountsAsComplete) {
  // Defensive: blocks_evacuated > blocks_at_census shouldn't happen given the
  // EvacDecide guard, but verify the boundary check uses >= not ==.
  CensusStats cstats;
  PageCensus census(&cstats);
  ObserveTimes(census, /*times=*/2, /*addr=*/100, /*capacity=*/10, /*used=*/2);

  PlanStats pstats;
  TargetPlan plan(&pstats);
  plan.BuildFrom(census);
  plan.FindMut(100)->blocks_evacuated = 5;  // > blocks_at_census (2)

  CycleProgress p = RunVerify(plan);
  EXPECT_EQ(p.targets_complete, 1u);
  EXPECT_EQ(p.targets_partial, 0u);
  EXPECT_EQ(p.targets_no_progress, 0u);
}

// =====================================================================
// Microbenchmarks for the CENSUS / EVACUATE per-object hot path.
// Run with: ./defrag_test --benchmark_filter='BM_.*'
// =====================================================================
namespace {

constexpr size_t kBenchObjectCount = 10000;
constexpr size_t kBenchBlockSize = 64;

void InitBenchEnv() {
  static bool initialized = false;
  if (!initialized) {
    init_zmalloc_threadlocal(mi_heap_get_backing());
    initialized = true;
  }
}

// Holds a batch of allocations; frees them on destruction.
struct AllocationBatch {
  std::vector<void*> pointers;
  ~AllocationBatch() {
    for (void* p : pointers) {
      mi_free(p);
    }
  }
};

AllocationBatch AllocBatch(size_t count, size_t block_size) {
  AllocationBatch ab;
  ab.pointers.reserve(count);
  mi_heap_t* heap = mi_heap_get_default();
  for (size_t i = 0; i < count; ++i) {
    ab.pointers.push_back(mi_heap_malloc(heap, block_size));
  }
  return ab;
}

}  // namespace

void BM_PtrPage(benchmark::State& state) {
  InitBenchEnv();
  AllocationBatch ab = AllocBatch(state.range(0), kBenchBlockSize);
  for (auto _ : state) {
    for (void* p : ab.pointers) {
      benchmark::DoNotOptimize(_mi_ptr_page(p));
    }
  }
  state.SetItemsProcessed(state.iterations() * ab.pointers.size());
}
BENCHMARK(BM_PtrPage)->Arg(kBenchObjectCount);

void BM_ProbeHeap(benchmark::State& state) {
  InitBenchEnv();
  AllocationBatch ab = AllocBatch(state.range(0), kBenchBlockSize);
  mi_heap_t* heap = mi_heap_get_default();
  for (auto _ : state) {
    for (void* p : ab.pointers) {
      auto stat = mi_heap_page_is_underutilized(heap, p, 0.8f, /*collect_stats=*/true);
      benchmark::DoNotOptimize(stat);
    }
  }
  state.SetItemsProcessed(state.iterations() * ab.pointers.size());
}
BENCHMARK(BM_ProbeHeap)->Arg(kBenchObjectCount);

void BM_ProbeNoHeap(benchmark::State& state) {
  InitBenchEnv();
  AllocationBatch ab = AllocBatch(state.range(0), kBenchBlockSize);
  for (auto _ : state) {
    for (void* p : ab.pointers) {
      mi_page_usage_stats_t stat;
      zmalloc_page_is_underutilized(p, 0.8f, /*collect_stats=*/true, &stat);
      benchmark::DoNotOptimize(stat);
    }
  }
  state.SetItemsProcessed(state.iterations() * ab.pointers.size());
}
BENCHMARK(BM_ProbeNoHeap)->Arg(kBenchObjectCount);

void BM_HashFindMiss(benchmark::State& state) {
  InitBenchEnv();
  AllocationBatch ab = AllocBatch(state.range(0), kBenchBlockSize);
  absl::flat_hash_map<uintptr_t, size_t> empty;
  for (auto _ : state) {
    for (void* p : ab.pointers) {
      uintptr_t addr = reinterpret_cast<uintptr_t>(_mi_ptr_page(p));
      benchmark::DoNotOptimize(empty.find(addr));
    }
  }
  state.SetItemsProcessed(state.iterations() * ab.pointers.size());
}
BENCHMARK(BM_HashFindMiss)->Arg(kBenchObjectCount);

void BM_HashFindHit(benchmark::State& state) {
  InitBenchEnv();
  AllocationBatch ab = AllocBatch(state.range(0), kBenchBlockSize);
  absl::flat_hash_map<uintptr_t, size_t> populated;
  for (void* p : ab.pointers) {
    populated[reinterpret_cast<uintptr_t>(_mi_ptr_page(p))] = 1;
  }
  for (auto _ : state) {
    for (void* p : ab.pointers) {
      uintptr_t addr = reinterpret_cast<uintptr_t>(_mi_ptr_page(p));
      benchmark::DoNotOptimize(populated.find(addr));
    }
  }
  state.SetItemsProcessed(state.iterations() * ab.pointers.size());
}
BENCHMARK(BM_HashFindHit)->Arg(kBenchObjectCount);

void BM_HashEmplaceFresh(benchmark::State& state) {
  InitBenchEnv();
  AllocationBatch ab = AllocBatch(state.range(0), kBenchBlockSize);
  for (auto _ : state) {
    state.PauseTiming();
    absl::flat_hash_map<uintptr_t, size_t> m;
    state.ResumeTiming();
    for (void* p : ab.pointers) {
      m.emplace(reinterpret_cast<uintptr_t>(_mi_ptr_page(p)), 1);
    }
    benchmark::DoNotOptimize(m);
  }
  state.SetItemsProcessed(state.iterations() * ab.pointers.size());
}
BENCHMARK(BM_HashEmplaceFresh)->Arg(kBenchObjectCount);

void BM_ProbeAndObserve(benchmark::State& state) {
  InitBenchEnv();
  AllocationBatch ab = AllocBatch(state.range(0), kBenchBlockSize);
  mi_heap_t* heap = mi_heap_get_default();
  for (auto _ : state) {
    state.PauseTiming();
    CensusStats cs;
    PageCensus census(&cs);
    state.ResumeTiming();
    for (void* p : ab.pointers) {
      auto stat = mi_heap_page_is_underutilized(heap, p, 0.8f, /*collect_stats=*/true);
      census.Observe(stat, /*bucket_cursor=*/0);
    }
    benchmark::DoNotOptimize(census);
  }
  state.SetItemsProcessed(state.iterations() * ab.pointers.size());
}
BENCHMARK(BM_ProbeAndObserve)->Arg(kBenchObjectCount);

void BM_EvacDecideMiss(benchmark::State& state) {
  // Empty plan -> every pointer hits the kNotATarget early-bail. Models
  // the 95% non-target case in EVACUATE.
  InitBenchEnv();
  AllocationBatch ab = AllocBatch(state.range(0), kBenchBlockSize);
  PlanStats ps;
  TargetPlan plan(&ps);
  for (auto _ : state) {
    EvacStats es{};
    for (void* p : ab.pointers) {
      uintptr_t addr = reinterpret_cast<uintptr_t>(_mi_ptr_page(p));
      TargetPage* target = plan.FindMut(addr);
      if (target == nullptr) {
        ++es.blocks_skipped_not_target;
        continue;
      }
      // Unreachable in this microbench.
      auto stat = mi_heap_page_is_underutilized(mi_heap_get_default(), p, 0.8f, true);
      EvacDecide(plan, target, stat, es);
    }
    benchmark::DoNotOptimize(es);
  }
  state.SetItemsProcessed(state.iterations() * ab.pointers.size());
}
BENCHMARK(BM_EvacDecideMiss)->Arg(kBenchObjectCount);

void BM_EvacDecideHit(benchmark::State& state) {
  // Plan contains every page our allocations live on -> every pointer hits
  // the EvacDecide commit path. Models the 5% on-target case.
  InitBenchEnv();
  AllocationBatch ab = AllocBatch(state.range(0), kBenchBlockSize);
  mi_heap_t* heap = mi_heap_get_default();

  // Build a synthetic census whose retention scoring doesn't filter our pages.
  CensusStats cs;
  PageCensus census(&cs);
  for (void* p : ab.pointers) {
    mi_page_usage_stats_t s{};
    s.page_address = reinterpret_cast<uintptr_t>(_mi_ptr_page(p));
    s.block_size = kBenchBlockSize;
    s.capacity = 64;
    s.used = 4;  // very low used -> high retention score
    s.flags = MI_DFLY_PAGE_BELOW_THRESHOLD;
    census.Observe(s, 0);
  }
  PlanStats ps;
  TargetPlan plan(&ps);
  plan.BuildFrom(census);

  for (auto _ : state) {
    EvacStats es{};
    for (void* p : ab.pointers) {
      uintptr_t addr = reinterpret_cast<uintptr_t>(_mi_ptr_page(p));
      TargetPage* target = plan.FindMut(addr);
      if (target == nullptr) {
        ++es.blocks_skipped_not_target;
        continue;
      }
      auto stat = mi_heap_page_is_underutilized(heap, p, 0.8f, /*collect_stats=*/true);
      EvacDecide(plan, target, stat, es);
    }
    benchmark::DoNotOptimize(es);
  }
  state.SetItemsProcessed(state.iterations() * ab.pointers.size());
}
BENCHMARK(BM_EvacDecideHit)->Arg(kBenchObjectCount);

// Same shape as BM_EvacDecideMiss but goes through the full Evacuator:
// bloom precheck rejects every page (empty plan -> bloom is empty -> no
// hashes computed beyond bloom). Compared against BM_EvacDecideMiss this
// shows the bloom's contribution on the no-target hot path.
void BM_EvacDecideMiss_Evacuator(benchmark::State& state) {
  InitBenchEnv();
  AllocationBatch ab = AllocBatch(state.range(0), kBenchBlockSize);
  PlanStats ps;
  TargetPlan plan(&ps);
  EvacStats es;
  Evacuator visitor(&plan, 0.8f, &es);
  for (auto _ : state) {
    for (void* p : ab.pointers) {
      benchmark::DoNotOptimize(visitor.IsPageForObjectUnderUtilized(p));
    }
  }
  state.SetItemsProcessed(state.iterations() * ab.pointers.size());
}
BENCHMARK(BM_EvacDecideMiss_Evacuator)->Arg(kBenchObjectCount);

// Same shape as BM_EvacDecideHit but goes through the full Evacuator:
// bloom hits, FindMut hits, per-page slice cache fills on first object per
// page and short-circuits mi_heap_page_is_underutilized for siblings.
// Compared against BM_EvacDecideHit this shows the cache's contribution.
void BM_EvacDecideHit_Evacuator(benchmark::State& state) {
  InitBenchEnv();
  AllocationBatch ab = AllocBatch(state.range(0), kBenchBlockSize);

  CensusStats cs;
  PageCensus census(&cs);
  for (void* p : ab.pointers) {
    mi_page_usage_stats_t s{};
    s.page_address = reinterpret_cast<uintptr_t>(_mi_ptr_page(p));
    s.block_size = kBenchBlockSize;
    s.capacity = 64;
    s.used = 4;
    s.flags = MI_DFLY_PAGE_BELOW_THRESHOLD;
    census.Observe(s, 0);
  }
  PlanStats ps;
  TargetPlan plan(&ps);
  plan.BuildFrom(census);
  EvacStats es;
  Evacuator visitor(&plan, 0.8f, &es);

  for (auto _ : state) {
    for (void* p : ab.pointers) {
      benchmark::DoNotOptimize(visitor.IsPageForObjectUnderUtilized(p));
    }
  }
  state.SetItemsProcessed(state.iterations() * ab.pointers.size());
}
BENCHMARK(BM_EvacDecideHit_Evacuator)->Arg(kBenchObjectCount);

namespace {

// Non-virtual mirror of Evacuator's hot path. Identical body, but no
// inheritance, no vtable, and the method is defined inline so the compiler
// can fold it into the caller. Comparing against BM_EvacDecideHit_Evacuator
// isolates how much of the per-call overhead is virtual dispatch + worse
// inlining vs structural cost (member access through `this`).
class EvacuatorNonVirt {
 public:
  EvacuatorNonVirt(TargetPlan* plan, float threshold, EvacStats* evac_stats)
      : plan_(plan), threshold_(threshold), evac_stats_(evac_stats) {
  }

  bool IsPageForObjectUnderUtilized(void* object) {
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

 private:
  TargetPlan* plan_;
  float threshold_;
  EvacStats* evac_stats_;
};

}  // namespace

void BM_EvacDecideHit_NonVirt(benchmark::State& state) {
  InitBenchEnv();
  AllocationBatch ab = AllocBatch(state.range(0), kBenchBlockSize);

  CensusStats cs;
  PageCensus census(&cs);
  for (void* p : ab.pointers) {
    mi_page_usage_stats_t s{};
    s.page_address = reinterpret_cast<uintptr_t>(_mi_ptr_page(p));
    s.block_size = kBenchBlockSize;
    s.capacity = 64;
    s.used = 4;
    s.flags = MI_DFLY_PAGE_BELOW_THRESHOLD;
    census.Observe(s, 0);
  }
  PlanStats ps;
  TargetPlan plan(&ps);
  plan.BuildFrom(census);
  EvacStats es;
  EvacuatorNonVirt visitor(&plan, 0.8f, &es);

  for (auto _ : state) {
    for (void* p : ab.pointers) {
      benchmark::DoNotOptimize(visitor.IsPageForObjectUnderUtilized(p));
    }
  }
  state.SetItemsProcessed(state.iterations() * ab.pointers.size());
}
BENCHMARK(BM_EvacDecideHit_NonVirt)->Arg(kBenchObjectCount);

namespace {

// Tagged-dispatch variant: simulates the design alternative where PageUsage
// has a non-virtual IsPageForObjectUnderUtilized that switches on a kind_
// enum and forwards to the concrete subclass's non-virtual impl. All bodies
// are inline so the compiler can see end-to-end. The bench calls through a
// base-class pointer to mimic how the production walker holds PageUsage*.
//
// If this matches NonVirt, tagged dispatch is a viable refactor — no virtual
// dispatch, no template cascade, just an enum + switch in the base class.

enum class TestVisitorKind : uint8_t { kEvacuator };

class TestEvacuatorTagged;

class TestPageUsageBase {
 public:
  // Non-virtual, defined inline (after subclass impls) so the switch can
  // inline the called method directly.
  inline bool IsPageForObjectUnderUtilized(void* object);

 protected:
  TestVisitorKind kind_;
};

class TestEvacuatorTagged : public TestPageUsageBase {
 public:
  TestEvacuatorTagged(TargetPlan* plan, float threshold, EvacStats* evac_stats)
      : plan_(plan), threshold_(threshold), evac_stats_(evac_stats) {
    kind_ = TestVisitorKind::kEvacuator;
  }

  // Non-virtual, inline. Body is identical to Evacuator's production impl.
  bool IsPageForObjectUnderUtilizedImpl(void* object) {
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

 private:
  TargetPlan* plan_;
  float threshold_;
  EvacStats* evac_stats_;
};

inline bool TestPageUsageBase::IsPageForObjectUnderUtilized(void* object) {
  switch (kind_) {
    case TestVisitorKind::kEvacuator:
      return static_cast<TestEvacuatorTagged*>(this)->IsPageForObjectUnderUtilizedImpl(object);
  }
  __builtin_unreachable();
}

}  // namespace

void BM_EvacDecideHit_TaggedDispatch(benchmark::State& state) {
  InitBenchEnv();
  AllocationBatch ab = AllocBatch(state.range(0), kBenchBlockSize);

  CensusStats cs;
  PageCensus census(&cs);
  for (void* p : ab.pointers) {
    mi_page_usage_stats_t s{};
    s.page_address = reinterpret_cast<uintptr_t>(_mi_ptr_page(p));
    s.block_size = kBenchBlockSize;
    s.capacity = 64;
    s.used = 4;
    s.flags = MI_DFLY_PAGE_BELOW_THRESHOLD;
    census.Observe(s, 0);
  }
  PlanStats ps;
  TargetPlan plan(&ps);
  plan.BuildFrom(census);
  EvacStats es;
  TestEvacuatorTagged visitor(&plan, 0.8f, &es);
  // Call through base pointer to mimic how the production walker dispatches.
  TestPageUsageBase* base = &visitor;

  for (auto _ : state) {
    for (void* p : ab.pointers) {
      benchmark::DoNotOptimize(base->IsPageForObjectUnderUtilized(p));
    }
  }
  state.SetItemsProcessed(state.iterations() * ab.pointers.size());
}
BENCHMARK(BM_EvacDecideHit_TaggedDispatch)->Arg(kBenchObjectCount);

// Populated plan + queries that all miss. Models the dominant production
// case: EVAC walks the prime table over millions of objects while the plan
// holds a few thousand target pages — most objects are on non-target pages
// and need fast rejection. Plan is built from synthetic addresses unrelated
// to the real allocations so every query miss hits the populated-map find()
// path (raw variant) or the bloom-rejection path (Evacuator variant).
namespace {

constexpr size_t kBenchSyntheticPlanSize = 4000;

void PopulatePlanWithSyntheticAddrs(PageCensus* census, size_t count) {
  for (size_t i = 0; i < count; ++i) {
    mi_page_usage_stats_t s{};
    // Addresses well above any mimalloc-managed range; 64 KiB stride matches
    // mimalloc page alignment so the addresses look plausible to the bloom
    // hasher.
    s.page_address = 0x100000000ULL + i * 0x10000ULL;
    s.block_size = kBenchBlockSize;
    s.capacity = 64;
    s.used = 4;
    s.flags = MI_DFLY_PAGE_BELOW_THRESHOLD;
    census->Observe(s, 0);
  }
}

// RAII: turn off the skip-bit setter for the duration of this object. The
// populated-plan benchmarks use synthetic addresses that aren't valid
// mi_page_t*; SetDefragSkipIfEnabled would dereference them and segfault on
// BuildFrom and on ~TargetPlan. Restoring on dtor (declare this BEFORE the
// TargetPlan so the plan destructs first while the flag is still off).
struct DefragSkipBitOff {
  bool prev;
  DefragSkipBitOff() : prev(absl::GetFlag(FLAGS_defrag_use_skip_bit)) {
    absl::SetFlag(&FLAGS_defrag_use_skip_bit, false);
  }
  ~DefragSkipBitOff() {
    absl::SetFlag(&FLAGS_defrag_use_skip_bit, prev);
  }
};

}  // namespace

void BM_EvacDecideMiss_Populated(benchmark::State& state) {
  DefragSkipBitOff skip_bit_off;  // declared first → destroyed last
  InitBenchEnv();
  AllocationBatch ab = AllocBatch(state.range(0), kBenchBlockSize);

  CensusStats cs;
  PageCensus census(&cs);
  PopulatePlanWithSyntheticAddrs(&census, kBenchSyntheticPlanSize);
  PlanStats ps;
  TargetPlan plan(&ps);
  plan.BuildFrom(census);

  for (auto _ : state) {
    EvacStats es{};
    for (void* p : ab.pointers) {
      uintptr_t addr = reinterpret_cast<uintptr_t>(_mi_ptr_page(p));
      TargetPage* target = plan.FindMut(addr);
      if (target == nullptr) {
        ++es.blocks_skipped_not_target;
        continue;
      }
      auto stat = mi_heap_page_is_underutilized(mi_heap_get_default(), p, 0.8f, true);
      EvacDecide(plan, target, stat, es);
    }
    benchmark::DoNotOptimize(es);
  }
  state.SetItemsProcessed(state.iterations() * ab.pointers.size());
}
BENCHMARK(BM_EvacDecideMiss_Populated)->Arg(kBenchObjectCount);

void BM_EvacDecideMiss_Populated_Evacuator(benchmark::State& state) {
  DefragSkipBitOff skip_bit_off;  // declared first → destroyed last
  InitBenchEnv();
  AllocationBatch ab = AllocBatch(state.range(0), kBenchBlockSize);

  CensusStats cs;
  PageCensus census(&cs);
  PopulatePlanWithSyntheticAddrs(&census, kBenchSyntheticPlanSize);
  PlanStats ps;
  TargetPlan plan(&ps);
  plan.BuildFrom(census);
  EvacStats es;
  Evacuator visitor(&plan, 0.8f, &es);

  for (auto _ : state) {
    for (void* p : ab.pointers) {
      benchmark::DoNotOptimize(visitor.IsPageForObjectUnderUtilized(p));
    }
  }
  state.SetItemsProcessed(state.iterations() * ab.pointers.size());
}
BENCHMARK(BM_EvacDecideMiss_Populated_Evacuator)->Arg(kBenchObjectCount);

// =====================================================================
// Microbenchmarks for the underutil-callback per-free overhead.
//
// Each variant allocates a fresh batch of objects (untimed) and frees them
// (timed); the delta between variants isolates the cost the dragonfly
// mimalloc patch adds to mi_free_block_local:
//
//   BM_Free_CallbackOff     callback unregistered. Only the unconditional
//                           prev_used load runs; the arith block and
//                           indirect call are short-circuited by the NULL
//                           check on _mi_dfly_underutil_cb.
//   BM_Free_CallbackNoOp    callback registered, body just bumps a counter.
//                           Adds the per-free arith block (cap_thr,
//                           prev_x100, cur_x100, two compares) on EVERY
//                           free, plus an indirect call on edge crossings.
//   BM_Free_CallbackInsert  callback registered, body inserts into a
//                           thread_local flat_hash_set. Adds hash work on
//                           top of NoOp, only on edge crossings.
//
// Differences:
//   (NoOp  - Off)   = per-free arith cost (paid by every delete)
//   (Insert - NoOp) = per-edge insert cost amortized over total frees
//
// The "edges/iter" counter reports the number of callback invocations per
// iteration so per-edge cost can be derived.
// Run with: ./defrag_test --benchmark_filter='BM_Free_.*'
// =====================================================================

namespace {

thread_local absl::flat_hash_set<uintptr_t> g_bench_underutil_set;
thread_local size_t g_bench_underutil_count = 0;

void BenchCallbackNoOp(uintptr_t /*addr*/) {
  ++g_bench_underutil_count;
}

void BenchCallbackInsert(uintptr_t addr) {
  ++g_bench_underutil_count;
  g_bench_underutil_set.insert(addr);
}

void RunFreeBench(benchmark::State& state, size_t count) {
  InitBenchEnv();
  size_t total_edges = 0;
  for (auto _ : state) {
    state.PauseTiming();
    g_bench_underutil_set.clear();
    g_bench_underutil_count = 0;
    std::vector<void*> ptrs;
    ptrs.reserve(count);
    mi_heap_t* heap = mi_heap_get_default();
    for (size_t i = 0; i < count; ++i) {
      ptrs.push_back(mi_heap_malloc(heap, kBenchBlockSize));
    }
    state.ResumeTiming();
    for (void* p : ptrs) {
      mi_free(p);
    }
    state.PauseTiming();
    total_edges += g_bench_underutil_count;
    state.ResumeTiming();
  }
  state.counters["edges/iter"] =
      benchmark::Counter(static_cast<double>(total_edges) / state.iterations());
  state.SetItemsProcessed(state.iterations() * count);
}

}  // namespace

void BM_Free_CallbackOff(benchmark::State& state) {
  mi_dfly_set_underutil_callback(nullptr);
  RunFreeBench(state, state.range(0));
}
BENCHMARK(BM_Free_CallbackOff)->Arg(kBenchObjectCount);

void BM_Free_CallbackNoOp(benchmark::State& state) {
  mi_dfly_set_underutil_threshold_pct(80);
  mi_dfly_set_underutil_callback(&BenchCallbackNoOp);
  RunFreeBench(state, state.range(0));
  mi_dfly_set_underutil_callback(nullptr);
}
BENCHMARK(BM_Free_CallbackNoOp)->Arg(kBenchObjectCount);

void BM_Free_CallbackInsert(benchmark::State& state) {
  mi_dfly_set_underutil_threshold_pct(80);
  mi_dfly_set_underutil_callback(&BenchCallbackInsert);
  RunFreeBench(state, state.range(0));
  mi_dfly_set_underutil_callback(nullptr);
}
BENCHMARK(BM_Free_CallbackInsert)->Arg(kBenchObjectCount);

}  // namespace dfly
