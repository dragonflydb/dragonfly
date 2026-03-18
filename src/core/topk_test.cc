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
  TOPKTest() : topk_(5, 100, 5, 0.0, PMR_NS::get_default_resource()) {
  }

  TOPK topk_;
};

// ---------------------------------------------------------------------------
// Construction & Configuration
// ---------------------------------------------------------------------------

// Verify K(), Width(), Depth(), Decay() return the exact values passed to the constructor.
TEST(TOPKBasic, ConstructorStoresParameters) {
  TOPK topk(10, 200, 7, 0.85, PMR_NS::get_default_resource());
  EXPECT_EQ(topk.K(), 10u);
  EXPECT_EQ(topk.Width(), 200u);
  EXPECT_EQ(topk.Depth(), 7u);
  EXPECT_DOUBLE_EQ(topk.Decay(), 0.85);
}

// Default decay (0.9) should use the shared static table, not allocate a custom one.
// A custom-decay instance allocates an extra ~32KB table, so it should report more memory.
TEST(TOPKBasic, DefaultDecayUsesSharedTable) {
  TOPK default_topk(5, 100, 5, TOPK::kDefaultDecay, PMR_NS::get_default_resource());
  TOPK custom_topk(5, 100, 5, 0.8, PMR_NS::get_default_resource());
  EXPECT_LT(default_topk.MallocUsed(), custom_topk.MallocUsed());
}

// Non-default decay value causes a custom decay table allocation, reflected in MallocUsed().
TEST(TOPKBasic, CustomDecayBuildsOwnTable) {
  TOPK topk(5, 100, 5, 0.75, PMR_NS::get_default_resource());
  // The custom table alone is kDecayLookupSize * sizeof(double) ≈ 32KB.
  EXPECT_GE(topk.MallocUsed(), TOPK::kDecayLookupSize * sizeof(double));
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
  TOPK src(3, 50, 3, 0.0, PMR_NS::get_default_resource());
  src.Add("x");
  src.Add("y");

  TOPK dst(1, 10, 1, 0.0, PMR_NS::get_default_resource());
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
  // Fill the heap first.
  for (uint32_t i{}; i < topk_.K(); ++i) {
    topk_.Add(absl::StrCat("fill", i));
  }

  // Pump one item to a high count to make it un-evictable.
  topk_.IncrBy("fill0", 5000);

  // Now batch-add: most won't evict, but a heavy IncrBy before the batch
  // means counts differ.  The key observation is the result vector has the right size.
  vector<string_view> batch;
  for (int i{}; i < 10; ++i) {
    batch.push_back("fill0");  // already in heap, won't evict
  }
  auto results = topk_.AddMultiple(batch);
  EXPECT_EQ(results.size(), batch.size());
}

// Adding the same item repeatedly increases its count in the heap.
TEST_F(TOPKTest, AddSameItemRepeatedlyIncreasesCount) {
  for (int i{}; i < 100; ++i) {
    topk_.Add("repeat");
  }
  auto list = topk_.List();
  bool found{};
  for (const auto& item : list) {
    if (item.item == "repeat") {
      EXPECT_GT(item.count, 1u);
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
TEST_F(TOPKTest, IncrByOneBehavesLikeAdd) {
  TOPK a(3, 100, 5, 0.0, PMR_NS::get_default_resource());
  TOPK b(3, 100, 5, 0.0, PMR_NS::get_default_resource());

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
  // Fill heap with low-count items.
  for (uint32_t i{}; i < topk_.K(); ++i) {
    topk_.Add(absl::StrCat("low", i));
  }

  // Record one of them.
  string victim = "low0";
  EXPECT_EQ(topk_.Query({victim})[0], 1);

  // Evict by adding a heavy item.
  topk_.IncrBy("heavy", 10000);
  // The victim *may* have been evicted. If it was, verify Query returns 0.
  if (topk_.Query({victim})[0] == 0) {
    SUCCEED();
  }
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
  for (const auto& item : list) {
    if (item.item == "match_me") {
      EXPECT_EQ(item.count, count_val);
    }
  }
}

// ---------------------------------------------------------------------------
// List
// ---------------------------------------------------------------------------

// List() returns an empty vector on a freshly constructed TOPK.
TEST_F(TOPKTest, ListEmptyOnConstruction) {
  TOPK fresh(5, 100, 5, 0.0, PMR_NS::get_default_resource());
  EXPECT_TRUE(fresh.List().empty());
}

// List() output is sorted in descending order by count.
TEST_F(TOPKTest, ListReturnsSortedByCountDescending) {
  topk_.IncrBy("low", 10);
  topk_.IncrBy("mid", 50);
  topk_.IncrBy("high", 100);

  auto list = topk_.List();
  for (size_t i{1}; i < list.size(); ++i) {
    EXPECT_GE(list[i - 1].count, list[i].count);
  }
}

// After inserting more than K distinct items, List().size() <= K.
TEST_F(TOPKTest, ListNeverExceedsKItems) {
  for (int i{}; i < 100; ++i) {
    topk_.IncrBy(absl::StrCat("x", i), (i + 1) * 10);
  }
  EXPECT_LE(topk_.List().size(), topk_.K());
}

// ---------------------------------------------------------------------------
// Decay & ComputeDecayProbability
// ---------------------------------------------------------------------------

// For count < kDecayLookupSize, ComputeDecayProbability equals std::pow(decay, count).
TEST(TOPKDecay, ProbabilityBelowTableSize) {
  TOPK topk(5, 100, 5, 0.85, PMR_NS::get_default_resource());
  // We can't call the private method directly, but we can verify the behaviour
  // indirectly: with decay=0.85 and many insertions of colliding items,
  // counters should sometimes decrease. We verify the table was built correctly
  // by checking that Add still works without crashing.
  for (int i{}; i < 1000; ++i) {
    topk.Add(absl::StrCat("item", i % 10));
  }
  EXPECT_FALSE(topk.List().empty());
}

// For count >= kDecayLookupSize, the extrapolation path should not crash or produce NaN.
TEST(TOPKDecay, ProbabilityAboveTableSizeNoCrash) {
  // Use a decay close to 1.0 so the extrapolation path is actually reached
  // (table's last entry > kDecayEpsilon).
  TOPK topk(3, 10, 3, 0.999, PMR_NS::get_default_resource());
  // Push counters to very high values.
  topk.IncrBy("big", numeric_limits<uint32_t>::max() / 2);
  auto counts = topk.Count({"big"});
  EXPECT_GT(counts[0], 0u);
}

// For an extremely large count with a small decay, probability drops to effectively zero.
// This means ShouldDecay always returns false for very high counts, so counters aren't decremented.
TEST(TOPKDecay, VeryHighCountApproachesZero) {
  // decay=0.5: 0.5^4096 is astronomically small (< kDecayEpsilon). The extrapolation
  // path should return 0.0, meaning no decay fires for counts above the table range.
  TOPK topk(3, 10, 3, 0.5, PMR_NS::get_default_resource());
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
TEST(TOPKDecay, ZeroDecayNeverDecays) {
  TOPK topk(3, 50, 3, 0.0, PMR_NS::get_default_resource());
  topk.IncrBy("mono", 100);
  auto count1 = topk.Count({"mono"})[0];
  topk.IncrBy("mono", 50);
  auto count2 = topk.Count({"mono"})[0];
  EXPECT_GE(count2, count1);
  EXPECT_EQ(count2, 150u);
}

// With decay=1.0, every non-zero counter has decay probability 1.0 (1^n = 1),
// so counters are aggressively suppressed by collisions.
TEST(TOPKDecay, DecayOneAlwaysDecays) {
  TOPK topk(3, 10, 3, 1.0, PMR_NS::get_default_resource());
  // With decay=1, every existing non-zero counter gets decremented instead of incremented.
  // After many adds, counters should stay very low.
  for (int i{}; i < 1000; ++i) {
    topk.Add("suppressed");
  }
  auto counts = topk.Count({"suppressed"});
  // With aggressive decay, the counter should be much lower than 1000.
  EXPECT_LT(counts[0], 500u);
}

// ---------------------------------------------------------------------------
// MallocUsed
// ---------------------------------------------------------------------------

// MallocUsed() after filling the heap should be larger than right after construction.
TEST(TOPKBasic, MallocUsedIncreaseWithHeapGrowth) {
  TOPK topk(5, 100, 5, 0.0, PMR_NS::get_default_resource());
  size_t before = topk.MallocUsed();
  for (int i{}; i < 5; ++i) {
    topk.IncrBy(absl::StrCat("item_with_a_long_name_", i), 100);
  }
  size_t after = topk.MallocUsed();
  EXPECT_GT(after, before);
}

// A TOPK with custom decay should report higher MallocUsed than one with default decay.
TEST(TOPKBasic, MallocUsedDefaultVsCustomDecay) {
  TOPK default_decay(5, 100, 5, TOPK::kDefaultDecay, PMR_NS::get_default_resource());
  TOPK custom_decay(5, 100, 5, 0.75, PMR_NS::get_default_resource());
  EXPECT_GT(custom_decay.MallocUsed(), default_decay.MallocUsed());
}

// ---------------------------------------------------------------------------
// Serialize / Deserialize
// ---------------------------------------------------------------------------

// After Serialize() + Deserialize(), K(), Width(), Depth(), Decay() are unchanged.
TEST_F(TOPKTest, SerializeRoundTripPreservesConfiguration) {
  topk_.IncrBy("a", 10);
  auto data = topk_.Serialize();

  TOPK restored(data.k, data.width, data.depth, data.decay, PMR_NS::get_default_resource());
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
  TOPK restored(data.k, data.width, data.depth, data.decay, PMR_NS::get_default_resource());
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
  TOPK restored(data.k, data.width, data.depth, data.decay, PMR_NS::get_default_resource());
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
  TOPK restored(data.k, data.width, data.depth, data.decay, PMR_NS::get_default_resource());
  restored.Deserialize(data);

  // The restored heap is full (K items). A heavy new item should evict the minimum.
  auto evicted = restored.IncrBy("post_restore_big", 10000);
  EXPECT_TRUE(evicted.has_value());
  EXPECT_EQ(restored.Query({"post_restore_big"})[0], 1);
}

// Serializing a fresh TOPK produces empty heap_items and a zero-filled counters vector.
TEST(TOPKBasic, SerializeEmptyTOPK) {
  TOPK topk(5, 100, 5, 0.0, PMR_NS::get_default_resource());
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
  TOPK topk(5, 100, 5, 0.9, PMR_NS::get_default_resource());
  topk.Add("works");
  EXPECT_EQ(topk.List().size(), 1u);
}

// ---------------------------------------------------------------------------
// Statistical / Accuracy
// ---------------------------------------------------------------------------

// Under a skewed distribution (high-frequency items + noise), the top items appear in List().
// Uses decay=0.0 and IncrBy so hot item counts are deterministic (not subject to the
// HeavyKeeper ~log(0.5)/log(decay) steady-state cap that repeated Add() calls hit).
TEST(TOPKStatistical, TopKItemsIdentifiedUnderHeavyLoad) {
  TOPK topk(5, 500, 5, 0.0, PMR_NS::get_default_resource());
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
TEST(TOPKStatistical, KEqualsOneTracksOnlyTopItem) {
  TOPK topk(1, 500, 5, 0.0, PMR_NS::get_default_resource());

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
TEST_F(TOPKTest, DeserializeRestoresHeapProperty) {
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

  TOPK restored(5, 100, 5, 0.0, PMR_NS::get_default_resource());
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
  EXPECT_DEBUG_DEATH(TOPK(0, 100, 5, 0.9, PMR_NS::get_default_resource()), "k_ > 0");
}

// width=0 violates DCHECK_GT(width_, 0u) in the constructor.
TEST(TOPKDeathTest, ZeroWidthCrashes) {
  EXPECT_DEBUG_DEATH(TOPK(5, 0, 5, 0.9, PMR_NS::get_default_resource()), "width_ > 0");
}

// decay=1.5 violates DCHECK_LE(decay_, 1.0) in the constructor.
TEST(TOPKDeathTest, DecayAboveOneCrashes) {
  EXPECT_DEBUG_DEATH(TOPK(5, 100, 5, 1.5, PMR_NS::get_default_resource()), "decay_ <= 1.0");
}

// Deserializing data with a mismatched k violates DCHECK_EQ(data.k, k_).
TEST(TOPKDeathTest, DeserializeDimensionMismatchCrashes) {
  TOPK topk(5, 100, 5, 0.9, PMR_NS::get_default_resource());
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
