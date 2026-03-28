// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "facade/facade_test.h"
#include "server/test_utils.h"

namespace dfly {

using testing::ElementsAre;
using testing::HasSubstr;

class TopkFamilyTest : public BaseFamilyTest {
 protected:
  // Creates a TOPK key with default parameters (width=8, depth=7, decay=0.9).
  void ReserveDefault(std::string_view key, uint32_t k) {
    auto resp = Run({"topk.reserve", key, std::to_string(k)});
    ASSERT_EQ(resp, "OK") << "Failed to reserve TOPK key: " << key;
  }

  // Creates a TOPK key with custom parameters.
  void ReserveCustom(std::string_view key, uint32_t k, uint32_t width, uint32_t depth,
                     double decay) {
    auto resp = Run({"topk.reserve", key, std::to_string(k), std::to_string(width),
                     std::to_string(depth), std::to_string(decay)});
    ASSERT_EQ(resp, "OK") << "Failed to reserve custom TOPK key: " << key;
  }

  // Adds a single item via TOPK.ADD and returns the response.
  RespExpr AddItem(std::string_view key, std::string_view item) {
    return Run({"topk.add", key, item});
  }

  // Adds multiple items via TOPK.ADD.
  RespExpr AddItems(std::string_view key, std::initializer_list<std::string_view> items) {
    std::vector<std::string> args = {"topk.add", std::string(key)};
    for (auto item : items)
      args.emplace_back(item);
    return Run(absl::Span<const std::string>(args));
  }

  // Increments an item by the given amount via TOPK.INCRBY.
  RespExpr IncrByItem(std::string_view key, std::string_view item, uint32_t incr) {
    return Run({"topk.incrby", key, item, std::to_string(incr)});
  }
};

// =============================================================================
// I. General Key & Type Management
// =============================================================================

// Tests TOPK.ADD, TOPK.INCRBY, TOPK.QUERY, TOPK.COUNT, TOPK.LIST, TOPK.INFO
// all return an error when the key does not exist.
TEST_F(TopkFamilyTest, CommandsOnNonExistentKey) {
  EXPECT_THAT(Run({"topk.add", "noexist", "foo"}), ErrArg("no such key"));
  EXPECT_THAT(Run({"topk.incrby", "noexist", "foo", "1"}), ErrArg("no such key"));
  EXPECT_THAT(Run({"topk.query", "noexist", "foo"}), ErrArg("no such key"));
  EXPECT_THAT(Run({"topk.count", "noexist", "foo"}), ErrArg("no such key"));
  EXPECT_THAT(Run({"topk.list", "noexist"}), ErrArg("no such key"));
  EXPECT_THAT(Run({"topk.info", "noexist"}), ErrArg("no such key"));
}

// Tests that all TOPK commands return WRONGTYPE when used on a non-TOPK key (e.g., a string).
TEST_F(TopkFamilyTest, WrongTypeErrors) {
  Run({"set", "mystr", "value"});
  EXPECT_THAT(Run({"topk.add", "mystr", "foo"}), ErrArg("WRONGTYPE"));
  EXPECT_THAT(Run({"topk.incrby", "mystr", "foo", "1"}), ErrArg("WRONGTYPE"));
  EXPECT_THAT(Run({"topk.query", "mystr", "foo"}), ErrArg("WRONGTYPE"));
  EXPECT_THAT(Run({"topk.count", "mystr", "foo"}), ErrArg("WRONGTYPE"));
  EXPECT_THAT(Run({"topk.list", "mystr"}), ErrArg("WRONGTYPE"));
  EXPECT_THAT(Run({"topk.info", "mystr"}), ErrArg("WRONGTYPE"));
}

// Tests that a TOPK key reports the correct TYPE.
TEST_F(TopkFamilyTest, TypeCommand) {
  ReserveDefault("myk", 5);
  EXPECT_EQ(Run({"type", "myk"}), "TopK-TYPE");
}

// Tests that DEL removes a TOPK key and subsequent commands on it fail.
TEST_F(TopkFamilyTest, DeleteKey) {
  ReserveDefault("myk", 5);
  Run({"topk.add", "myk", "foo"});
  EXPECT_THAT(Run({"del", "myk"}), IntArg(1));
  EXPECT_THAT(Run({"topk.add", "myk", "foo"}), ErrArg("no such key"));
}

// Tests that TOPK.RESERVE fails when the key already exists as a different type (e.g., a string).
TEST_F(TopkFamilyTest, ReserveOnExistingWrongType) {
  Run({"set", "mystr", "val"});
  EXPECT_THAT(Run({"topk.reserve", "mystr", "5"}), ErrArg("WRONGTYPE"));
}

// =============================================================================
// II. TOPK.RESERVE
// =============================================================================

// Tests basic TOPK.RESERVE with only the required 'k' parameter, using default
// width/depth/decay. Verifies via TOPK.INFO that defaults are applied.
TEST_F(TopkFamilyTest, ReserveDefaultParams) {
  ReserveDefault("tk", 10);
  auto resp = Run({"topk.info", "tk"});
  EXPECT_THAT(resp, RespArray(ElementsAre("k", IntArg(10), "width", IntArg(8), "depth", IntArg(7),
                                          "decay", "0.9")));
}

// Tests TOPK.RESERVE with all four parameters (k, width, depth, decay).
TEST_F(TopkFamilyTest, ReserveAllCustomParams) {
  ReserveCustom("tk", 20, 100, 5, 0.95);
  auto resp = Run({"topk.info", "tk"});
  EXPECT_THAT(resp, RespArray(ElementsAre("k", IntArg(20), "width", IntArg(100), "depth", IntArg(5),
                                          "decay", "0.95")));
}

// Tests that TOPK.RESERVE with k=1 is accepted (minimum valid k).
TEST_F(TopkFamilyTest, ReserveMinK) {
  ReserveDefault("tk", 1);
  auto resp = Run({"topk.info", "tk"});
  EXPECT_THAT(resp, RespArray(ElementsAre("k", IntArg(1), "width", IntArg(8), "depth", IntArg(7),
                                          "decay", "0.9")));
}

// Tests that TOPK.RESERVE with a large k (e.g. 10000) is accepted.
TEST_F(TopkFamilyTest, ReserveLargeK) {
  ReserveDefault("tk", 10000);
  auto resp = Run({"topk.info", "tk"});
  EXPECT_THAT(resp, RespArray(ElementsAre("k", IntArg(10000), "width", IntArg(8), "depth",
                                          IntArg(7), "decay", "0.9")));
}

// Tests TOPK.RESERVE with decay=0.0 (boundary).
TEST_F(TopkFamilyTest, ReserveDecayZero) {
  ReserveCustom("tk", 5, 8, 7, 0.0);
  auto resp = Run({"topk.info", "tk"});
  EXPECT_THAT(resp, RespArray(ElementsAre("k", IntArg(5), "width", IntArg(8), "depth", IntArg(7),
                                          "decay", "0")));
}

// Tests TOPK.RESERVE with decay=1.0 (boundary).
TEST_F(TopkFamilyTest, ReserveDecayOne) {
  ReserveCustom("tk", 5, 8, 7, 1.0);
  auto resp = Run({"topk.info", "tk"});
  EXPECT_THAT(resp, RespArray(ElementsAre("k", IntArg(5), "width", IntArg(8), "depth", IntArg(7),
                                          "decay", "1")));
}

// Tests that TOPK.RESERVE fails when k=0.
TEST_F(TopkFamilyTest, ReserveKZero) {
  EXPECT_THAT(Run({"topk.reserve", "tk", "0"}), ErrArg("k must be greater than 0"));
}

// Tests that TOPK.RESERVE fails when k is negative (parsed as uint, so invalid).
TEST_F(TopkFamilyTest, ReserveKNegative) {
  auto resp = Run({"topk.reserve", "tk", "-1"});
  EXPECT_THAT(resp, ErrArg("not an integer"));
}

// Tests that TOPK.RESERVE fails when k is not a number.
TEST_F(TopkFamilyTest, ReserveKNotANumber) {
  auto resp = Run({"topk.reserve", "tk", "abc"});
  EXPECT_THAT(resp, ErrArg("not an integer"));
}

// Tests that TOPK.RESERVE fails when width=0.
TEST_F(TopkFamilyTest, ReserveWidthZero) {
  EXPECT_THAT(Run({"topk.reserve", "tk", "5", "0", "7", "0.9"}),
              ErrArg("width and depth must be greater than 0"));
}

// Tests that TOPK.RESERVE fails when depth=0.
TEST_F(TopkFamilyTest, ReserveDepthZero) {
  EXPECT_THAT(Run({"topk.reserve", "tk", "5", "8", "0", "0.9"}),
              ErrArg("width and depth must be greater than 0"));
}

// Tests that TOPK.RESERVE fails when decay > 1.0.
TEST_F(TopkFamilyTest, ReserveDecayAboveOne) {
  EXPECT_THAT(Run({"topk.reserve", "tk", "5", "8", "7", "1.5"}),
              ErrArg("decay must be between 0 and 1"));
}

// Tests that TOPK.RESERVE fails when decay is negative.
TEST_F(TopkFamilyTest, ReserveDecayNegative) {
  EXPECT_THAT(Run({"topk.reserve", "tk", "5", "8", "7", "-0.1"}),
              ErrArg("decay must be between 0 and 1"));
}

// Tests that TOPK.RESERVE rejects a duplicate key (already exists as TOPK).
TEST_F(TopkFamilyTest, ReserveDuplicateKey) {
  ReserveDefault("tk", 5);
  EXPECT_THAT(Run({"topk.reserve", "tk", "10"}), ErrArg("item exists"));
}

// Tests that TOPK.RESERVE fails with too few arguments (missing k).
TEST_F(TopkFamilyTest, ReserveTooFewArgs) {
  auto resp = Run({"topk.reserve", "tk"});
  EXPECT_THAT(resp, ErrArg("wrong number of arguments"));
}

// Tests that TOPK.RESERVE with partial optional params fails (all-or-nothing:
// exactly 3 or 6 args after command name). Providing only width without depth/decay
// should fail.
TEST_F(TopkFamilyTest, ReservePartialOptionalParams) {
  // Only width, missing depth and decay — parser OUT_OF_BOUNDS → syntax error
  auto resp = Run({"topk.reserve", "tk", "5", "100"});
  EXPECT_THAT(resp, ErrArg("syntax error"));

  // width + depth, missing decay
  resp = Run({"topk.reserve", "tk", "5", "100", "7"});
  EXPECT_THAT(resp, ErrArg("syntax error"));
}

// Tests that TOPK.RESERVE fails with trailing garbage arguments after valid params.
TEST_F(TopkFamilyTest, ReserveTrailingArgs) {
  EXPECT_THAT(Run({"topk.reserve", "tk", "5", "8", "7", "0.9", "extra"}), ErrArg("syntax error"));
}

// Tests that TOPK.RESERVE safely rejects dimensions that exceed our hard caps,
// preventing massive memory allocations (DoS protection).
TEST_F(TopkFamilyTest, ReserveDimensionsExceedCaps) {
  // width = 1,000,001 (exceeds kMaxWidth of 1,000,000)
  auto resp1 = Run({"topk.reserve", "tk1", "50", "1000001", "7", "0.9"});
  EXPECT_THAT(resp1, ErrArg("must not exceed"));

  // depth = 101 (exceeds kMaxDepth of 100)
  auto resp2 = Run({"topk.reserve", "tk2", "50", "100000", "101", "0.9"});
  EXPECT_THAT(resp2, ErrArg("must not exceed"));
}

// =============================================================================
// III. TOPK.ADD
// =============================================================================

// Tests adding a single item to the top-k. Should return nil (no eviction).
TEST_F(TopkFamilyTest, AddSingleItem) {
  ReserveDefault("tk", 5);
  auto resp = AddItem("tk", "foo");
  EXPECT_EQ(resp.type, RespExpr::NIL);
}

// Tests adding multiple items in a single TOPK.ADD call.
// Each should return nil (no eviction) when the heap is not full.
TEST_F(TopkFamilyTest, AddMultipleItems) {
  ReserveDefault("tk", 5);
  auto resp = AddItems("tk", {"a", "b", "c"});
  EXPECT_THAT(resp, RespArray(ElementsAre(ArgType(RespExpr::NIL), ArgType(RespExpr::NIL),
                                          ArgType(RespExpr::NIL))));
}

// Tests that adding the same item multiple times does not cause errors.
// Adds accumulate the count.
TEST_F(TopkFamilyTest, AddDuplicateItem) {
  ReserveDefault("tk", 5);
  AddItem("tk", "foo");
  AddItem("tk", "foo");
  AddItem("tk", "foo");

  auto resp = Run({"topk.count", "tk", "foo"});
  EXPECT_GE(resp.GetInt(), 1);

  resp = Run({"topk.query", "tk", "foo"});
  EXPECT_THAT(resp, IntArg(1));
}

// Tests that TOPK.ADD fails when called with no items (only key).
TEST_F(TopkFamilyTest, AddNoItems) {
  ReserveDefault("tk", 5);
  EXPECT_THAT(Run({"topk.add", "tk"}), ErrArg("wrong number of arguments"));
}

// Tests eviction behavior: when more than k distinct items are inserted,
// the top-k heap stays at k items and eviction occurs.
TEST_F(TopkFamilyTest, AddEviction) {
  // Use wider sketch for more deterministic behavior
  ReserveCustom("tk", 2, 50, 7, 0.9);

  // Build up very strong counts so the top-2 are deterministic
  IncrByItem("tk", "heavy1", 10000);
  IncrByItem("tk", "heavy2", 5000);

  // Adding a weak item should return NIL (no eviction, because it can't beat the min)
  auto resp = AddItem("tk", "weak");
  EXPECT_EQ(resp.type, RespExpr::NIL);

  auto list = Run({"topk.list", "tk"});
  EXPECT_EQ(list.GetVec().size(), 2u);

  // The two heavy items should still be in the top-k
  EXPECT_THAT(Run({"topk.query", "tk", "heavy1"}), IntArg(1));
  EXPECT_THAT(Run({"topk.query", "tk", "heavy2"}), IntArg(1));

  // Now add a very strong item that should evict the weakest (heavy2)
  resp = IncrByItem("tk", "newcomer", 100000);
  // The return value should be a string (the evicted item), not NIL
  EXPECT_EQ(resp.type, RespExpr::STRING);
}

// Tests adding items with special characters (spaces, emoji-like, binary-safe).
TEST_F(TopkFamilyTest, AddSpecialCharacters) {
  ReserveDefault("tk", 5);
  AddItem("tk", "hello world");
  AddItem("tk", "foo\tbar");
  AddItem("tk", "");  // Empty string

  auto resp = Run({"topk.query", "tk", "hello world"});
  EXPECT_THAT(resp, IntArg(1));
}

// Tests adding a large number of items exceeding k to verify batch behavior.
TEST_F(TopkFamilyTest, AddLargeBatch) {
  ReserveDefault("tk", 10);
  std::vector<std::string> args = {"topk.add", "tk"};
  for (int i = 0; i < 100; i++)
    args.push_back("item" + std::to_string(i));
  Run(absl::Span<const std::string>(args));

  auto resp = Run({"topk.list", "tk"});
  // Should have at most k=10 items
  EXPECT_LE(resp.GetVec().size(), 10u);
}

// =============================================================================
// IV. TOPK.INCRBY
// =============================================================================

// Tests incrementing a single item by a specific amount.
TEST_F(TopkFamilyTest, IncrBySingleItem) {
  ReserveDefault("tk", 5);
  auto resp = IncrByItem("tk", "foo", 10);
  EXPECT_EQ(resp.type, RespExpr::NIL);

  resp = Run({"topk.count", "tk", "foo"});
  EXPECT_GE(resp.GetInt(), 1);
}

// Tests incrementing multiple items in a single TOPK.INCRBY call.
TEST_F(TopkFamilyTest, IncrByMultipleItems) {
  ReserveDefault("tk", 5);
  auto resp = Run({"topk.incrby", "tk", "a", "5", "b", "3", "c", "7"});
  EXPECT_THAT(resp, RespArray(ElementsAre(ArgType(RespExpr::NIL), ArgType(RespExpr::NIL),
                                          ArgType(RespExpr::NIL))));
}

// Tests that the count increases after multiple INCRBY calls on the same item.
TEST_F(TopkFamilyTest, IncrByAccumulates) {
  ReserveDefault("tk", 5);
  IncrByItem("tk", "foo", 10);
  IncrByItem("tk", "foo", 20);

  auto resp = Run({"topk.count", "tk", "foo"});
  // Probabilistic, but should be at least 1
  EXPECT_GE(resp.GetInt(), 1);
}

// Tests TOPK.INCRBY with increment=1 (minimum valid increment).
TEST_F(TopkFamilyTest, IncrByMinIncrement) {
  ReserveDefault("tk", 5);
  auto resp = IncrByItem("tk", "foo", 1);
  EXPECT_EQ(resp.type, RespExpr::NIL);
}

// Tests TOPK.INCRBY with increment=100000 (maximum valid increment per Redis docs).
TEST_F(TopkFamilyTest, IncrByMaxIncrement) {
  ReserveDefault("tk", 5);
  auto resp = IncrByItem("tk", "foo", 100000);
  EXPECT_EQ(resp.type, RespExpr::NIL);
}

// Tests that TOPK.INCRBY with increment=0 returns an error.
TEST_F(TopkFamilyTest, IncrByZeroIncrement) {
  ReserveDefault("tk", 5);
  EXPECT_THAT(IncrByItem("tk", "foo", 0), ErrArg("increment must be between 1 and 100000"));
}

// Tests that TOPK.INCRBY with increment > 100000 returns an error.
TEST_F(TopkFamilyTest, IncrByExceedsMaxIncrement) {
  ReserveDefault("tk", 5);
  EXPECT_THAT(Run({"topk.incrby", "tk", "foo", "100001"}),
              ErrArg("increment must be between 1 and 100000"));
}

// Tests that TOPK.INCRBY with a non-numeric increment returns an error.
TEST_F(TopkFamilyTest, IncrByNonNumericIncrement) {
  ReserveDefault("tk", 5);
  EXPECT_THAT(Run({"topk.incrby", "tk", "foo", "notanumber"}), ErrArg("not an integer"));
}

// Tests that TOPK.INCRBY with an odd number of item/incr arguments (missing the
// increment for the last item) returns an error. With arity -4, providing only
// 3 args (key + 1 item) is rejected by the framework; providing 5 args
// (key + item + incr + item) hits the handler's syntax check.
TEST_F(TopkFamilyTest, IncrByOddArgs) {
  ReserveDefault("tk", 5);
  // 3 args total: rejected by CommandRegistry arity check
  EXPECT_THAT(Run({"topk.incrby", "tk", "foo"}), ErrArg("wrong number of arguments"));
  // 5 args total: passes arity, but handler sees odd item/incr pairs
  EXPECT_THAT(Run({"topk.incrby", "tk", "foo", "1", "bar"}), ErrArg("syntax error"));
}

// Tests that TOPK.INCRBY with no item/incr pairs returns an error.
TEST_F(TopkFamilyTest, IncrByNoItems) {
  ReserveDefault("tk", 5);
  EXPECT_THAT(Run({"topk.incrby", "tk"}), ErrArg("wrong number of arguments"));
}

// =============================================================================
// V. TOPK.QUERY
// =============================================================================

// Tests TOPK.QUERY for an item that is present in the top-k.
TEST_F(TopkFamilyTest, QueryPresentItem) {
  ReserveDefault("tk", 5);
  AddItem("tk", "foo");
  EXPECT_THAT(Run({"topk.query", "tk", "foo"}), IntArg(1));
}

// Tests TOPK.QUERY for an item that has never been added.
TEST_F(TopkFamilyTest, QueryAbsentItem) {
  ReserveDefault("tk", 5);
  EXPECT_THAT(Run({"topk.query", "tk", "neveradded"}), IntArg(0));
}

// Tests TOPK.QUERY with multiple items, mixing present and absent.
TEST_F(TopkFamilyTest, QueryMultipleMixed) {
  ReserveDefault("tk", 5);
  AddItem("tk", "a");
  AddItem("tk", "b");
  auto resp = Run({"topk.query", "tk", "a", "b", "c"});
  EXPECT_THAT(resp, RespArray(ElementsAre(IntArg(1), IntArg(1), IntArg(0))));
}

// Tests TOPK.QUERY on a just-reserved (empty) top-k.
TEST_F(TopkFamilyTest, QueryEmptyTopk) {
  ReserveDefault("tk", 5);
  EXPECT_THAT(Run({"topk.query", "tk", "anything"}), IntArg(0));
}

// Tests that TOPK.QUERY with no items returns an error.
TEST_F(TopkFamilyTest, QueryNoItems) {
  ReserveDefault("tk", 5);
  EXPECT_THAT(Run({"topk.query", "tk"}), ErrArg("wrong number of arguments"));
}

// =============================================================================
// VI. TOPK.COUNT
// =============================================================================

// Tests TOPK.COUNT for an item with a known increment.
// Since counts are probabilistic, we just verify the count is positive.
TEST_F(TopkFamilyTest, CountSingleItem) {
  ReserveDefault("tk", 5);
  IncrByItem("tk", "foo", 10);
  auto resp = Run({"topk.count", "tk", "foo"});
  EXPECT_GE(resp.GetInt(), 1);
}

// Tests that TOPK.COUNT for a never-added item returns 0.
TEST_F(TopkFamilyTest, CountAbsentItem) {
  ReserveDefault("tk", 5);
  EXPECT_THAT(Run({"topk.count", "tk", "neveradded"}), IntArg(0));
}

// Tests TOPK.COUNT with multiple items and verifies relative ordering.
// Item with higher increment should have count >= item with lower increment.
TEST_F(TopkFamilyTest, CountMultipleRelativeOrder) {
  ReserveDefault("tk", 5);
  IncrByItem("tk", "low", 10);
  IncrByItem("tk", "high", 100);

  auto resp = Run({"topk.count", "tk", "high", "low"});
  auto vec = resp.GetVec();
  EXPECT_GE(vec[0].GetInt(), vec[1].GetInt());
}

// Tests TOPK.COUNT on an empty top-k for any item.
TEST_F(TopkFamilyTest, CountEmptyTopk) {
  ReserveDefault("tk", 5);
  EXPECT_THAT(Run({"topk.count", "tk", "anything"}), IntArg(0));
}

// Tests that TOPK.COUNT returns the estimated frequency from the sketch
// even if the item is not currently in the Top-K heap.
TEST_F(TopkFamilyTest, CountItemOutsideOfHeap) {
  // k=1, decay=1.0 (disables decay of existing counters, though hash collisions remain
  // probabilistic)
  ReserveCustom("tk", 1, 50, 7, 1.0);

  IncrByItem("tk", "heavy", 1000);
  IncrByItem("tk", "victim", 5);

  EXPECT_THAT(Run({"topk.query", "tk", "victim"}), IntArg(0));

  // Use EXPECT_GE because Count-Min Sketch guarantees count >= actual,
  // but hash collisions can technically cause overestimation.
  auto count_victim = Run({"topk.count", "tk", "victim"});
  EXPECT_GE(count_victim.GetInt().value_or(0), 5);
  auto count_heavy = Run({"topk.count", "tk", "heavy"});
  EXPECT_GE(count_heavy.GetInt().value_or(0), 1000);
}

// Tests that TOPK.COUNT with no items returns an error.
TEST_F(TopkFamilyTest, CountNoItems) {
  ReserveDefault("tk", 5);
  EXPECT_THAT(Run({"topk.count", "tk"}), ErrArg("wrong number of arguments"));
}

// =============================================================================
// VII. TOPK.LIST
// =============================================================================

// Tests TOPK.LIST returns an empty array on a freshly reserved (empty) top-k.
TEST_F(TopkFamilyTest, ListEmpty) {
  ReserveDefault("tk", 5);
  auto resp = Run({"topk.list", "tk"});
  EXPECT_THAT(resp, ArrLen(0));
}

// Tests TOPK.LIST returns items after adding them.
TEST_F(TopkFamilyTest, ListAfterAdds) {
  ReserveDefault("tk", 5);
  AddItems("tk", {"a", "b", "c"});
  auto resp = Run({"topk.list", "tk"});
  EXPECT_EQ(resp.GetVec().size(), 3u);
}

// Tests TOPK.LIST returns exactly k items when more than k distinct items are added.
TEST_F(TopkFamilyTest, ListCappedAtK) {
  ReserveDefault("tk", 3);
  // Add 10 distinct items (more than k=3), each with enough count to enter the heap
  for (int i = 0; i < 10; i++) {
    IncrByItem("tk", "item" + std::to_string(i), 100);
  }
  auto resp = Run({"topk.list", "tk"});
  EXPECT_EQ(resp.GetVec().size(), 3u);
}

// Tests TOPK.LIST WITHCOUNT returns items interleaved with their counts.
TEST_F(TopkFamilyTest, ListWithCount) {
  ReserveDefault("tk", 3);
  IncrByItem("tk", "a", 100);
  IncrByItem("tk", "b", 50);
  IncrByItem("tk", "c", 10);

  auto resp = Run({"topk.list", "tk", "WITHCOUNT"});
  auto vec = resp.GetVec();
  EXPECT_EQ(vec.size(), 6u);  // 3 items * 2 (name + count)

  // Verify structure: pairs of (string, integer)
  for (size_t i = 0; i < vec.size(); i += 2) {
    EXPECT_EQ(vec[i].type, RespExpr::STRING);
    EXPECT_GE(vec[i + 1].GetInt(), 1);
  }
}

// Tests that TOPK.LIST WITHCOUNT is case-insensitive (e.g., "withcount").
TEST_F(TopkFamilyTest, ListWithCountCaseInsensitive) {
  ReserveDefault("tk", 3);
  AddItem("tk", "a");
  auto resp = Run({"topk.list", "tk", "withcount"});
  auto vec = resp.GetVec();
  EXPECT_EQ(vec.size(), 2u);  // 1 item * 2
}

// Tests that TOPK.LIST results are sorted in descending order by count.
TEST_F(TopkFamilyTest, ListDescendingOrder) {
  ReserveDefault("tk", 3);
  IncrByItem("tk", "low", 10);
  IncrByItem("tk", "mid", 50);
  IncrByItem("tk", "high", 100);

  auto resp = Run({"topk.list", "tk", "WITHCOUNT"});
  auto vec = resp.GetVec();
  EXPECT_EQ(vec.size(), 6u);

  // Verify counts are in descending order
  int64_t prev_count = INT64_MAX;
  for (size_t i = 1; i < vec.size(); i += 2) {
    auto count = vec[i].GetInt();
    ASSERT_TRUE(count.has_value());
    EXPECT_LE(*count, prev_count);
    prev_count = *count;
  }
}

// Tests that TOPK.LIST with an invalid flag (not WITHCOUNT) returns a syntax error.
TEST_F(TopkFamilyTest, ListInvalidFlag) {
  ReserveDefault("tk", 3);
  EXPECT_THAT(Run({"topk.list", "tk", "INVALID"}), ErrArg("syntax error"));
}

// Tests that TOPK.LIST WITHCOUNT with trailing arguments returns a syntax error.
TEST_F(TopkFamilyTest, ListTrailingArgs) {
  ReserveDefault("tk", 3);
  EXPECT_THAT(Run({"topk.list", "tk", "WITHCOUNT", "extra"}), ErrArg("syntax error"));
}

// =============================================================================
// VIII. TOPK.INFO
// =============================================================================

// Tests that TOPK.INFO returns the correct default configuration.
TEST_F(TopkFamilyTest, InfoDefaultParams) {
  ReserveDefault("tk", 5);
  auto resp = Run({"topk.info", "tk"});
  EXPECT_THAT(resp, RespArray(ElementsAre("k", IntArg(5), "width", IntArg(8), "depth", IntArg(7),
                                          "decay", "0.9")));
}

// Tests that TOPK.INFO returns the correct custom configuration.
TEST_F(TopkFamilyTest, InfoCustomParams) {
  ReserveCustom("tk", 20, 200, 10, 0.75);
  auto resp = Run({"topk.info", "tk"});
  EXPECT_THAT(resp, RespArray(ElementsAre("k", IntArg(20), "width", IntArg(200), "depth",
                                          IntArg(10), "decay", "0.75")));
}

// Tests that TOPK.INFO with trailing arguments returns an error.
// Since TOPK.INFO has fixed arity 2, the framework rejects extra args.
TEST_F(TopkFamilyTest, InfoTrailingArgs) {
  ReserveDefault("tk", 5);
  EXPECT_THAT(Run({"topk.info", "tk", "extra"}), ErrArg("wrong number of arguments"));
}

// Tests that TOPK.INFO returns the correct response format (flat array of 8 elements).
TEST_F(TopkFamilyTest, InfoResponseFormat) {
  ReserveDefault("tk", 5);
  auto resp = Run({"topk.info", "tk"});
  auto vec = resp.GetVec();
  EXPECT_EQ(vec.size(), 8u);

  // Verify field names
  EXPECT_EQ(vec[0].GetString(), "k");
  EXPECT_EQ(vec[2].GetString(), "width");
  EXPECT_EQ(vec[4].GetString(), "depth");
  EXPECT_EQ(vec[6].GetString(), "decay");
}

// =============================================================================
// IX. Advanced & Integrity
// =============================================================================

// Tests that after many adds, the items returned by TOPK.LIST are the highest-frequency
// items (statistical sanity check). Uses INCRBY with large counts and a wider sketch
// for deterministic results.
TEST_F(TopkFamilyTest, FrequencyAccuracy) {
  // Use a wider sketch for more deterministic behavior
  ReserveCustom("tk", 3, 50, 7, 0.9);

  // Add 3 items with clearly differentiated, large counts
  IncrByItem("tk", "alpha", 50000);
  IncrByItem("tk", "beta", 30000);
  IncrByItem("tk", "gamma", 20000);

  // Add noise items with low counts
  for (int i = 0; i < 50; i++) {
    AddItem("tk", "noise" + std::to_string(i));
  }

  // The top-3 should still contain our heavy items
  auto resp = Run({"topk.query", "tk", "alpha"});
  EXPECT_THAT(resp, IntArg(1));
  resp = Run({"topk.query", "tk", "beta"});
  EXPECT_THAT(resp, IntArg(1));
  resp = Run({"topk.query", "tk", "gamma"});
  EXPECT_THAT(resp, IntArg(1));
}

// Tests that multiple independent TOPK keys do not interfere with each other.
TEST_F(TopkFamilyTest, MultipleKeysIsolation) {
  ReserveDefault("tk1", 3);
  ReserveDefault("tk2", 5);

  AddItem("tk1", "onlyin1");
  AddItem("tk2", "onlyin2");

  EXPECT_THAT(Run({"topk.query", "tk1", "onlyin1"}), IntArg(1));
  EXPECT_THAT(Run({"topk.query", "tk1", "onlyin2"}), IntArg(0));

  EXPECT_THAT(Run({"topk.query", "tk2", "onlyin2"}), IntArg(1));
  EXPECT_THAT(Run({"topk.query", "tk2", "onlyin1"}), IntArg(0));

  // Verify INFO returns correct per-key parameters
  auto info1 = Run({"topk.info", "tk1"});
  EXPECT_THAT(info1, RespArray(ElementsAre("k", IntArg(3), "width", IntArg(8), "depth", IntArg(7),
                                           "decay", "0.9")));
  auto info2 = Run({"topk.info", "tk2"});
  EXPECT_THAT(info2, RespArray(ElementsAre("k", IntArg(5), "width", IntArg(8), "depth", IntArg(7),
                                           "decay", "0.9")));
}

// Tests that ADD and INCRBY correctly interact: both increase the item's presence
// in the top-k. After ADD + INCRBY, the item should remain queryable in the heap.
TEST_F(TopkFamilyTest, AddAndIncrByInteraction) {
  // Use wider sketch for deterministic behavior
  ReserveCustom("tk", 5, 100, 7, 0.9);

  // Add item via ADD (count +1)
  AddItem("tk", "foo");
  auto resp = Run({"topk.query", "tk", "foo"});
  EXPECT_THAT(resp, IntArg(1));

  // Increment same item via INCRBY — item should still be in top-k
  IncrByItem("tk", "foo", 100);
  resp = Run({"topk.query", "tk", "foo"});
  EXPECT_THAT(resp, IntArg(1));

  // Add a different item via ADD, then boost via INCRBY
  AddItem("tk", "bar");
  IncrByItem("tk", "bar", 50);
  resp = Run({"topk.query", "tk", "bar"});
  EXPECT_THAT(resp, IntArg(1));
}

// Tests behavior under high contention: many distinct items with the same count
// competing for limited k slots.
TEST_F(TopkFamilyTest, HighContentionEqualCounts) {
  ReserveDefault("tk", 5);

  // Add 20 items each with the same increment
  for (int i = 0; i < 20; i++) {
    IncrByItem("tk", "item" + std::to_string(i), 10);
  }

  // Should have exactly k items in the list
  auto resp = Run({"topk.list", "tk"});
  auto vec = resp.GetVec();
  EXPECT_EQ(vec.size(), 5u);
}

}  // namespace dfly
