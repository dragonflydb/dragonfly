// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/topk.h"

#include <absl/strings/str_cat.h>

#include <cmath>
#include <string>
#include <utility>
#include <vector>

#include "base/gtest.h"

namespace dfly {

using namespace std;

class TOPKTest : public ::testing::Test {
 protected:
  // Use decay=0 to disable probabilistic decay, making tests deterministic.
  // With decay=0, ShouldDecay always returns false (0^count = 0 for count>0),
  // so counters only grow and are never decremented by colliding items.
  // Having a decay != 0 will cause probabilistic flakiness in tests, as items may be randomly
  // evicted due to decay rather than true count comparisons.
  TOPKTest() : topk_(PMR_NS::get_default_resource(), 5, 100, 5, 0.0) {
  }

  double ComputeDecayProbability(TOPK* topk, uint32_t count) const {
    return topk->ComputeDecayProbability(count);
  }
  TOPK topk_;
};

// ---------------------------------------------------------------------------
// Construction & Configuration
// ---------------------------------------------------------------------------

// Verify K(), Width(), Depth(), Decay() return the exact values passed to the constructor.
TEST(TOPKBasic, ConstructorStoresParameters) {
  TOPK topk(PMR_NS::get_default_resource(), 10, 200, 7, 0.85);
  EXPECT_EQ(topk.K(), 10u);
  EXPECT_EQ(topk.Width(), 200u);
  EXPECT_EQ(topk.Depth(), 7u);
  EXPECT_DOUBLE_EQ(topk.Decay(), 0.85);
}

// Verify that default decay reuses the static process-wide table (saving memory),
// while a custom decay value allocates its own ~32KB lookup table.
TEST(TOPKBasic, DecayTableMemoryAllocation) {
  TOPK default_topk(PMR_NS::get_default_resource(), 5, 100, 5, TOPK::kDefaultDecay);
  TOPK custom_topk(PMR_NS::get_default_resource(), 5, 100, 5, 0.75);

  size_t default_mem = default_topk.MallocUsed();
  size_t custom_mem = custom_topk.MallocUsed();

  // Test that the custom one uses strictly more memory
  EXPECT_LT(default_mem, custom_mem);

  // Test that the difference in memory is exactly the size of the custom decay array
  size_t expected_table_size = TOPK::kDecayLookupSize * sizeof(double);
  EXPECT_GE(custom_mem - default_mem, expected_table_size);
}

// Move-construct a populated TOPK; source should be emptied and destination should hold the items.
TEST_F(TOPKTest, MoveConstructorTransfersOwnership) {
  topk_.Add("alpha");
  topk_.Add("beta");

  TOPK moved(std::move(topk_));

  EXPECT_EQ(moved.K(), 5u);
  auto list = moved.List();
  EXPECT_FALSE(list.empty());

  // Source is zeroed out.
  EXPECT_EQ(topk_.K(), 0u);
}

// Move-assign a populated TOPK into another; verify same post-conditions as move constructor.
TEST(TOPKBasic, MoveAssignmentTransfersOwnership) {
  TOPK src(PMR_NS::get_default_resource(), 3, 50, 3, 0.0);
  src.Add("x");
  src.Add("y");

  TOPK dst(PMR_NS::get_default_resource(), 1, 10, 1, 0.0);
  dst = std::move(src);

  EXPECT_EQ(dst.K(), 3u);
  EXPECT_EQ(dst.Width(), 50u);
  auto list = dst.List();
  EXPECT_EQ(list.size(), 2u);
  EXPECT_EQ(src.K(), 0u);
}

// ---------------------------------------------------------------------------
// Add / AddMultiple
// ---------------------------------------------------------------------------

// Add exactly K distinct items; List() should return exactly K items with no evictions.
TEST_F(TOPKTest, AddFillsHeapUpToK) {
  for (uint32_t i{}; i < topk_.K(); ++i) {
    auto evicted = topk_.Add(absl::StrCat("item", i));
    EXPECT_FALSE(evicted.has_value()) << "Unexpected eviction at i=" << i;
  }
  EXPECT_EQ(topk_.List().size(), topk_.K());
}

// Each Add() while the heap has room returns std::nullopt.
// Note: adding a K+1th item with the same count as the minimum also returns nullopt,
// because the fast-reject path correctly requires new_count > min to trigger an eviction.
TEST_F(TOPKTest, AddReturnsNulloptWhileHeapNotFull) {
  for (uint32_t i{}; i < topk_.K(); ++i) {
    EXPECT_EQ(topk_.Add(absl::StrCat("item", i)), nullopt);
  }
}

// After filling the heap, IncrBy a new item with a large count to force an eviction.
TEST_F(TOPKTest, AddEvictsMinimumWhenHeapFull) {
  // Fill the heap with K items, each added once (count=1).
  for (uint32_t i{}; i < topk_.K(); ++i) {
    topk_.Add(absl::StrCat("filler", i));
  }

  // Force a new item in with a large count; it must evict the minimum.
  auto evicted = topk_.IncrBy("heavy_hitter", 1000);
  EXPECT_TRUE(evicted.has_value());
}

// After filling the heap, adding an item whose count can't exceed the minimum shouldn't evict.
TEST_F(TOPKTest, AddDoesNotEvictWhenNewItemScoreTooLow) {
  // Fill the heap with items pumped to high counts.
  for (uint32_t i{}; i < topk_.K(); ++i) {
    topk_.IncrBy(absl::StrCat("big", i), 1000);
  }

  // Single add of a brand-new item (count=1) won't beat any existing item.
  auto evicted = topk_.Add("tiny_newcomer");
  EXPECT_FALSE(evicted.has_value());
}

// AddMultiple returns a vector with exactly as many elements as the input, 1:1.
TEST_F(TOPKTest, AddMultipleReturnsOneToOneMapping) {
  vector<string_view> items = {"a", "b", "c", "d"};
  auto results = topk_.AddMultiple(items);
  EXPECT_EQ(results.size(), items.size());
}

// In a batch add, an eviction at a specific index is correctly reported.
TEST_F(TOPKTest, AddMultiplePropagatesEvictions) {
  // Fill the heap (K=5). All items have count=1.
  for (uint32_t i{}; i < topk_.K(); ++i) {
    topk_.Add(absl::StrCat("fill", i));
  }

  // Pump items 1 through 4 to high counts (100).
  // This guarantees "fill0" (count 1) is absolutely sitting at the root of the min-heap.
  for (uint32_t i{1}; i < topk_.K(); ++i) {
    topk_.IncrBy(absl::StrCat("fill", i), 100);
  }

  // Create batch that forces an eviction exactly on the 3rd operation
  vector<string_view> batch = {
      "fill1",    // Already in heap (count 100 -> 101). No eviction.
      "new_guy",  // New item. Count becomes 1. Does not beat "fill0" (count 1). No eviction.
      "new_guy",  // New item again! Count becomes 2. Beats "fill0". EVICTS "fill0"!
      "fill2"     // Already in heap (count 100 -> 101). No eviction.
  };

  auto results = topk_.AddMultiple(batch);

  // Assert the exact sequence of events
  // The 3rd item should contain the exact string of the evicted key
  ASSERT_EQ(results.size(), 4u);
  EXPECT_FALSE(results[0].has_value());
  EXPECT_FALSE(results[1].has_value());
  ASSERT_TRUE(results[2].has_value());
  EXPECT_EQ(results[2].value(), "fill0");
  EXPECT_FALSE(results[3].has_value());
}

// Adding the same item repeatedly increases its count in the heap.
// Because decay=0.0 and there are no collisions, the count must be exactly 100.
TEST_F(TOPKTest, AddSameItemRepeatedlyIncreasesCount) {
  for (int i{}; i < 100; ++i) {
    topk_.Add("repeat");
  }

  auto list = topk_.List();
  bool found = false;
  for (const auto& item : list) {
    if (item.item == "repeat") {
      EXPECT_EQ(item.count, 100u);
      found = true;
    }
  }
  EXPECT_TRUE(found);
}

// ---------------------------------------------------------------------------
// IncrBy / IncrByMultiple
// ---------------------------------------------------------------------------

// IncrBy with increment=0 must return nullopt and not modify state.
TEST_F(TOPKTest, IncrByZeroReturnsNullopt) {
  topk_.Add("existing");
  auto before = topk_.Count({"existing"});
  auto result = topk_.IncrBy("existing", 0);
  EXPECT_EQ(result, nullopt);
  auto after = topk_.Count({"existing"});
  EXPECT_EQ(before, after);
}

// IncrBy(item, 1) should behave the same as Add(item) — both increment by 1.
TEST(TOPKBasic, IncrByOneBehavesLikeAdd) {
  TOPK a(PMR_NS::get_default_resource(), 3, 100, 5, 0.0);
  TOPK b(PMR_NS::get_default_resource(), 3, 100, 5, 0.0);

  a.Add("x");
  b.IncrBy("x", 1);

  EXPECT_EQ(a.Count({"x"})[0], b.Count({"x"})[0]);
}

// A single IncrBy with a large increment should immediately promote the item into the heap,
// evicting the current minimum.
TEST_F(TOPKTest, IncrByLargeValueCausesImmediateEviction) {
  for (uint32_t i{}; i < topk_.K(); ++i) {
    topk_.Add(absl::StrCat("base", i));
  }
  auto evicted = topk_.IncrBy("newcomer", 10000);
  EXPECT_TRUE(evicted.has_value());

  auto qr = topk_.Query({"newcomer"});
  EXPECT_EQ(qr[0], 1);
}

// IncrBy on an item already in the heap should increase its count without eviction.
TEST_F(TOPKTest, IncrByExistingHeapItemUpdatesCount) {
  topk_.IncrBy("item_a", 50);
  auto count_before = topk_.Count({"item_a"})[0];

  auto evicted = topk_.IncrBy("item_a", 100);
  EXPECT_EQ(evicted, nullopt);

  auto count_after = topk_.Count({"item_a"})[0];
  EXPECT_GT(count_after, count_before);
}

// IncrByMultiple returns a result vector with exactly as many elements as the input.
TEST_F(TOPKTest, IncrByMultipleReturnsOneToOneMapping) {
  vector<pair<string_view, uint32_t>> items = {{"a", 1}, {"b", 2}, {"c", 3}};
  auto results = topk_.IncrByMultiple(items);
  EXPECT_EQ(results.size(), items.size());
}

// ---------------------------------------------------------------------------
// Query
// ---------------------------------------------------------------------------

// All K items currently in the heap should return 1 from Query.
TEST_F(TOPKTest, QueryReturnsTrueForHeapItems) {
  vector<string> keys;
  for (uint32_t i{}; i < topk_.K(); ++i) {
    keys.push_back(absl::StrCat("key", i));
    topk_.Add(keys.back());
  }
  vector<string_view> views(keys.begin(), keys.end());
  auto results = topk_.Query(views);
  for (size_t i{}; i < results.size(); ++i) {
    EXPECT_EQ(results[i], 1) << "key" << i << " should be in heap";
  }
}

// Items that were never inserted should return 0 from Query.
TEST_F(TOPKTest, QueryReturnsFalseForNonHeapItems) {
  auto results = topk_.Query({"never_seen", "also_absent", "nope"});
  for (int r : results) {
    EXPECT_EQ(r, 0);
  }
}

// An item that was once in the heap but got evicted should return 0 from Query.
TEST_F(TOPKTest, QueryReturnsFalseForEvictedItems) {
  // Add our target victim. Count = 1.
  string victim = "low0";
  topk_.Add(victim);

  // Fill the rest of the heap (K=5) with items that are heavier.
  for (uint32_t i{1}; i < topk_.K(); ++i) {
    topk_.IncrBy(absl::StrCat("heavier", i), 50);
  }

  // Verify the victim is currently in the heap.
  EXPECT_EQ(topk_.Query({victim})[0], 1);

  // Evict by adding a massive item.
  // Because "low0" (count 1) is strictly smaller than the others (count 50),
  // it is mathematically guaranteed to be the one evicted.
  topk_.IncrBy("massive", 10000);

  // Strictly assert that the victim is gone. No "if" statements!
  EXPECT_EQ(topk_.Query({victim})[0], 0);
}

// Mixed batch: some items in heap, some not. Verify correct 0/1 pattern.
TEST_F(TOPKTest, QueryMixedBatch) {
  topk_.IncrBy("inheap", 100);
  auto results = topk_.Query({"inheap", "notheap"});
  EXPECT_EQ(results[0], 1);
  EXPECT_EQ(results[1], 0);
}

// ---------------------------------------------------------------------------
// Count
// ---------------------------------------------------------------------------

// Items never inserted should return count 0.
TEST_F(TOPKTest, CountReturnsZeroForUnseen) {
  auto counts = topk_.Count({"never_added", "also_missing"});
  for (auto c : counts) {
    EXPECT_EQ(c, 0u);
  }
}

// Items that have been added should return a count >= 1.
TEST_F(TOPKTest, CountReturnsNonZeroForSeenItems) {
  topk_.Add("seen");
  auto counts = topk_.Count({"seen"});
  EXPECT_GE(counts[0], 1u);
}

// The count from Count() for a heap item should match the count reported in List().
TEST_F(TOPKTest, CountForHeapItemMatchesListCount) {
  topk_.IncrBy("match_me", 50);
  auto count_val = topk_.Count({"match_me"})[0];
  auto list = topk_.List();

  bool found = false;
  for (const auto& item : list) {
    if (item.item == "match_me") {
      EXPECT_EQ(item.count, count_val);
      found = true;
    }
  }
  EXPECT_TRUE(found);
}

// ---------------------------------------------------------------------------
// List
// ---------------------------------------------------------------------------

// List() returns an empty vector on a freshly constructed TOPK.
TEST(TOPKBasic, ListEmptyOnConstruction) {
  TOPK fresh(PMR_NS::get_default_resource(), 5, 100, 5, 0.0);
  EXPECT_TRUE(fresh.List().empty());
}

// List() output is sorted in descending order by count.
TEST_F(TOPKTest, ListReturnsSortedByCountDescending) {
  topk_.IncrBy("low", 10);
  topk_.IncrBy("mid", 50);
  topk_.IncrBy("high", 100);

  auto list = topk_.List();

  // 1. Guarantee the items actually returned
  ASSERT_EQ(list.size(), 3u);

  // 2. Exact match the deterministic order
  EXPECT_EQ(list[0].item, "high");
  EXPECT_EQ(list[0].count, 100u);

  EXPECT_EQ(list[1].item, "mid");
  EXPECT_EQ(list[1].count, 50u);

  EXPECT_EQ(list[2].item, "low");
  EXPECT_EQ(list[2].count, 10u);
}

// After inserting more than K distinct items, List().size() == K.
TEST_F(TOPKTest, ListNeverExceedsKItems) {
  for (int i{}; i < 100; ++i) {
    topk_.IncrBy(absl::StrCat("x", i), (i + 1) * 10);
  }
  // We inserted 100 items. The heap MUST be exactly full.
  EXPECT_EQ(topk_.List().size(), topk_.K());
}

// ---------------------------------------------------------------------------
// Decay & ComputeDecayProbability
// ---------------------------------------------------------------------------

// For count < kDecayLookupSize, ComputeDecayProbability equals std::pow(decay, count).
TEST_F(TOPKTest, ProbabilityBelowTableSize) {
  double decay_val = 0.85;
  TOPK topk(PMR_NS::get_default_resource(), 5, 100, 5, decay_val);

  // ComputeDecayProbability enforces DCHECK_GT(count, 0u), so we start at 1.
  for (uint32_t count = 1; count < TOPK::kDecayLookupSize; ++count) {
    double expected = std::pow(decay_val, static_cast<double>(count));

    // EXPECT_DOUBLE_EQ allows up to 4 ULPs of rounding difference.
    EXPECT_DOUBLE_EQ(ComputeDecayProbability(&topk, count), expected);
  }
}

// For count >= kDecayLookupSize, the extrapolation path should not crash or produce NaN.
TEST(TOPKBasic, ProbabilityAboveTableSizeNoCrash) {
  TOPK topk(PMR_NS::get_default_resource(), 3, 10, 3, 0.999);

  // Push counter safely above kDecayLookupSize (4097)
  topk.IncrBy("big", 5000);

  // 2. NOW call Add. This forces ShouldDecay(5000) to execute!
  // It shouldn't crash, segfault, or produce NaN.
  for (int i = 0; i < 10; ++i) {
    topk.Add("big");
  }

  // Just verify the state isn't corrupted (count is still around 5000)
  EXPECT_GT(topk.Count({"big"})[0], 4000u);
}

// For an extremely large count with a small decay, probability drops to effectively zero.
// This means ShouldDecay always returns false for very high counts, so counters aren't decremented.
TEST(TOPKBasic, VeryHighCountApproachesZero) {
  // decay=0.5: 0.5^4096 is astronomically small (< kDecayEpsilon). The extrapolation
  // path should return 0.0, meaning no decay fires for counts above the table range.
  TOPK topk(PMR_NS::get_default_resource(), 3, 10, 3, 0.5);
  topk.IncrBy("stable", 10000);
  auto count_before = topk.Count({"stable"})[0];
  // Adding more items should not decay "stable"'s counter because the decay
  // probability for such high counts is effectively zero.
  for (int i{}; i < 100; ++i) {
    topk.Add(absl::StrCat("other", i));
  }
  auto count_after = topk.Count({"stable"})[0];
  // Count may increase from hash collisions but should never decrease.
  EXPECT_GE(count_after, count_before);
}

// With decay=0.0, the decay probability is always 0 (0^n = 0 for n>0),
// so counters should grow monotonically.
TEST(TOPKBasic, ZeroDecayNeverDecays) {
  TOPK topk(PMR_NS::get_default_resource(), 3, 50, 3, 0.0);
  topk.IncrBy("mono", 100);
  auto count1 = topk.Count({"mono"})[0];
  topk.IncrBy("mono", 50);
  auto count2 = topk.Count({"mono"})[0];
  EXPECT_GE(count2, count1);
  EXPECT_EQ(count2, 150u);
}

// With decay=1.0, every non-zero counter has ShouldDecay probability exactly 1.0 (1^n = 1).
// Because this implementation uses no fingerprints (unlike the original HeavyKeeper paper),
// decay fires even when re-adding the same item to its own non-zero counter.
// The counter therefore oscillates: 0 → 1 (add to zero-counter) → 0 (decay fires) → repeat.
// It is mathematically impossible for the counter to exceed 1.
TEST(TOPKBasic, DecayOneAlwaysDecays) {
  TOPK topk(PMR_NS::get_default_resource(), 3, 10, 3, 1.0);

  for (int i{}; i < 1000; ++i) {
    topk.Add("suppressed");
  }

  // Because decay is 100%, the counter just oscillates between 0 and 1.
  // It is mathematically impossible for it to exceed 1.
  EXPECT_LE(topk.Count({"suppressed"})[0], 1u);
}

// ---------------------------------------------------------------------------
// MallocUsed
// ---------------------------------------------------------------------------

// MallocUsed() after filling the heap should be larger than right after construction.
TEST(TOPKBasic, MallocUsedIncreaseWithHeapGrowth) {
  TOPK topk(PMR_NS::get_default_resource(), 5, 100, 5, 0.0);
  size_t before = topk.MallocUsed();
  for (int i{}; i < 5; ++i) {
    topk.IncrBy(absl::StrCat("item_with_a_long_name_", i), 100);
  }
  size_t after = topk.MallocUsed();
  EXPECT_GT(after, before);
}

// ---------------------------------------------------------------------------
// Serialize / Deserialize
// ---------------------------------------------------------------------------

// After Serialize() + Deserialize(), K(), Width(), Depth(), Decay() are unchanged.
TEST_F(TOPKTest, SerializeRoundTripPreservesConfiguration) {
  topk_.IncrBy("a", 10);
  auto data = topk_.Serialize();

  TOPK restored(PMR_NS::get_default_resource(), data.k, data.width, data.depth, data.decay);
  restored.Deserialize(data);

  EXPECT_EQ(restored.K(), topk_.K());
  EXPECT_EQ(restored.Width(), topk_.Width());
  EXPECT_EQ(restored.Depth(), topk_.Depth());
  EXPECT_DOUBLE_EQ(restored.Decay(), topk_.Decay());
}

// After round-trip, List() returns the same items with the same counts.
TEST_F(TOPKTest, SerializeRoundTripPreservesHeapItems) {
  topk_.IncrBy("alpha", 100);
  topk_.IncrBy("beta", 50);
  topk_.IncrBy("gamma", 25);

  auto data = topk_.Serialize();
  TOPK restored(PMR_NS::get_default_resource(), data.k, data.width, data.depth, data.decay);
  restored.Deserialize(data);

  auto orig_list = topk_.List();
  auto rest_list = restored.List();
  ASSERT_EQ(orig_list.size(), rest_list.size());
  for (size_t i{}; i < orig_list.size(); ++i) {
    EXPECT_EQ(orig_list[i].item, rest_list[i].item);
    EXPECT_EQ(orig_list[i].count, rest_list[i].count);
  }
}

// After round-trip, Count() returns the same estimated frequencies.
TEST_F(TOPKTest, SerializeRoundTripPreservesCounters) {
  topk_.IncrBy("foo", 42);
  topk_.IncrBy("bar", 77);

  auto data = topk_.Serialize();
  TOPK restored(PMR_NS::get_default_resource(), data.k, data.width, data.depth, data.decay);
  restored.Deserialize(data);

  EXPECT_EQ(topk_.Count({"foo"})[0], restored.Count({"foo"})[0]);
  EXPECT_EQ(topk_.Count({"bar"})[0], restored.Count({"bar"})[0]);
}

// After Deserialize(), subsequent Add() calls work correctly and evictions are reported.
TEST_F(TOPKTest, DeserializeRebuildsValidHeapProperty) {
  for (uint32_t i{}; i < topk_.K(); ++i) {
    topk_.IncrBy(absl::StrCat("pre", i), 10);
  }

  auto data = topk_.Serialize();
  TOPK restored(PMR_NS::get_default_resource(), data.k, data.width, data.depth, data.decay);
  restored.Deserialize(data);

  // The restored heap is full (K items). A heavy new item should evict the minimum.
  auto evicted = restored.IncrBy("post_restore_big", 10000);
  EXPECT_TRUE(evicted.has_value());
  EXPECT_EQ(restored.Query({"post_restore_big"})[0], 1);
}

// Serializing a fresh TOPK produces empty heap_items and a zero-filled counters vector.
TEST(TOPKBasic, SerializeEmptyTOPK) {
  TOPK topk(PMR_NS::get_default_resource(), 5, 100, 5, 0.0);
  auto data = topk.Serialize();

  EXPECT_TRUE(data.heap_items.empty());
  EXPECT_EQ(data.counters.size(), 100u * 5);
  for (auto c : data.counters) {
    EXPECT_EQ(c, 0u);
  }
}

// ---------------------------------------------------------------------------
// PMR Allocator
// ---------------------------------------------------------------------------

// Explicitly passing get_default_resource() works correctly without crashing.
TEST(TOPKBasic, PMRExplicitDefaultResourceWorks) {
  TOPK topk(PMR_NS::get_default_resource(), 5, 100, 5, 0.9);
  topk.Add("works");
  EXPECT_EQ(topk.List().size(), 1u);
}

// ---------------------------------------------------------------------------
// Statistical / Accuracy
// ---------------------------------------------------------------------------

// Verify that the Top-K correctly identifies "Hot" items even when
// the sketch is flooded with "Cold" noise (many items seen only once).
//
// SETUP:
// 1. We disable Decay (decay=0.0) to make the test 100% predictable (no RNG).
// 2. We use IncrBy to give 5 "Hot" items a guaranteed high score of 1000.
// 3. We use Add to insert 200 "Cold" items once each (score of 1).
//
// WHY INCRBY?
// In a real-world scenario with decay, an item's count eventually hits a
// "ceiling" where decay and growth balance out. By using IncrBy and decay=0,
// we bypass that math to ensure our "Hot" items are strictly,
// deterministically larger than the noise.
TEST(TOPKBasic, TopKItemsIdentifiedUnderHeavyLoad) {
  TOPK topk(PMR_NS::get_default_resource(), 5, 500, 5, 0.0);
  // Hot items get a large, deterministic count via IncrBy.
  for (int h{}; h < 5; ++h) {
    topk.IncrBy(absl::StrCat("hot", h), 1000);
  }
  // Cold items are each seen only once.
  for (int c{}; c < 200; ++c) {
    topk.Add(absl::StrCat("cold", c));
  }

  auto list = topk.List();
  ASSERT_EQ(list.size(), 5u);
  // All 5 hot items should be present in the top-K list.
  for (int h{}; h < 5; ++h) {
    string hot_key = absl::StrCat("hot", h);
    bool found{};
    for (const auto& item : list) {
      if (item.item == hot_key) {
        found = true;
        break;
      }
    }
    EXPECT_TRUE(found) << hot_key << " should be in the top-K list";
  }
}

// With k=1, only the single most-frequent item survives in the heap.
// Uses decay=0.0 and IncrBy so "dominant" has a deterministically high count
// that minor items (each added once, count=1) can never exceed.
TEST(TOPKBasic, KEqualsOneTracksOnlyTopItem) {
  TOPK topk(PMR_NS::get_default_resource(), 1, 500, 5, 0.0);

  // "dominant" gets a large, fixed count.
  topk.IncrBy("dominant", 1000);
  // Minor items are each seen only once; count=1 < 1000, so none can displace dominant.
  for (int i{}; i < 50; ++i) {
    topk.Add(absl::StrCat("minor", i));
  }

  auto list = topk.List();
  ASSERT_EQ(list.size(), 1u);
  EXPECT_EQ(list[0].item, "dominant");
}

// ---------------------------------------------------------------------------
// Deserialization Heap Repair
// ---------------------------------------------------------------------------

// Deserialize() must call std::make_heap to restore the min-heap invariant even when
// heap_items are stored out-of-order in the RDB snapshot (e.g. saved in List() order).
TEST(TOPKBasic, DeserializeRestoresHeapProperty) {
  TOPK::SerializedData data;
  data.k = 5;
  data.width = 100;
  data.depth = 5;
  data.decay = 0.0;
  data.counters.resize(500, 0);

  // Items deliberately out of min-heap order: smallest must end up at the root.
  data.heap_items.push_back({"heavy", 1000});
  data.heap_items.push_back({"medium", 500});
  data.heap_items.push_back({"light", 10});

  TOPK restored(PMR_NS::get_default_resource(), 5, 100, 5, 0.0);
  restored.Deserialize(data);

  // List() sorts descending — correct only if make_heap built a valid heap.
  auto list = restored.List();
  ASSERT_EQ(list.size(), 3u);
  EXPECT_EQ(list[0].item, "heavy");
  EXPECT_EQ(list[1].item, "medium");
  EXPECT_EQ(list[2].item, "light");

  // Heap is not yet full (3 of 5 slots used), so fill it to capacity.
  restored.IncrBy("filler1", 20);
  restored.IncrBy("filler2", 30);

  // Now heap is full (5 items: light=10, filler1=20, filler2=30, medium=500, heavy=1000).
  // A new item with count > 10 must evict "light" — the min-heap root.
  auto evicted = restored.IncrBy("newcomer", 50);
  ASSERT_TRUE(evicted.has_value());
  EXPECT_EQ(evicted.value(), "light");
}

// ---------------------------------------------------------------------------
// Counter Saturation (Overflow Prevention)
// ---------------------------------------------------------------------------

// IncrBy must saturate at UINT32_MAX rather than wrapping around to 0.
// A wrap-around would trick the heap into evicting a top item — a correctness
// and security issue (malicious TOPK.INCRBY with a huge increment).
TEST_F(TOPKTest, CounterSaturationPreventsOverflow) {
  const uint32_t max_val = numeric_limits<uint32_t>::max();
  topk_.IncrBy("max_item", max_val);
  EXPECT_EQ(topk_.Count({"max_item"})[0], max_val);

  // Adding more must not wrap the counter back to a small number.
  topk_.IncrBy("max_item", 100);
  EXPECT_EQ(topk_.Count({"max_item"})[0], max_val);
}

// ---------------------------------------------------------------------------
// Death Tests (DCHECKs active in debug builds only)
// ---------------------------------------------------------------------------

#ifndef NDEBUG
// k=0 violates DCHECK_GT(k_, 0u) in the constructor.
TEST(TOPKDeathTest, ZeroKCrashes) {
  EXPECT_DEBUG_DEATH(TOPK(PMR_NS::get_default_resource(), 0, 100, 5, 0.9), "k_ > 0");
}

// width=0 violates DCHECK_GT(width_, 0u) in the constructor.
TEST(TOPKDeathTest, ZeroWidthCrashes) {
  EXPECT_DEBUG_DEATH(TOPK(PMR_NS::get_default_resource(), 5, 0, 5, 0.9), "width_ > 0");
}

// decay=1.5 violates DCHECK_LE(decay_, 1.0) in the constructor.
TEST(TOPKDeathTest, DecayAboveOneCrashes) {
  EXPECT_DEBUG_DEATH(TOPK(PMR_NS::get_default_resource(), 5, 100, 5, 1.5), "decay_ <= 1.0");
}

// Deserializing data with a mismatched k violates DCHECK_EQ(data.k, k_).
TEST(TOPKDeathTest, DeserializeDimensionMismatchCrashes) {
  TOPK topk(PMR_NS::get_default_resource(), 5, 100, 5, 0.9);
  TOPK::SerializedData bad;
  bad.k = 10;  // Mismatch: object was constructed with k=5.
  bad.width = 100;
  bad.depth = 5;
  bad.decay = 0.9;
  bad.counters.resize(500, 0);
  EXPECT_DEBUG_DEATH(topk.Deserialize(bad), "data.k == k_");
}
#endif

}  // namespace dfly
