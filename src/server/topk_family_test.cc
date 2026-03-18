// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "facade/facade_test.h"
#include "server/test_utils.h"

namespace dfly {

using testing::ElementsAre;
using testing::UnorderedElementsAre;

class TopkFamilyTest : public BaseFamilyTest {};

TEST_F(TopkFamilyTest, Reserve) {
  auto resp = Run({"topk.reserve", "topk1", "5"});
  EXPECT_EQ(resp, "OK");
  EXPECT_EQ(Run({"type", "topk1"}), "TopK-TYPE");

  // Should fail on existing key
  resp = Run({"topk.reserve", "topk1", "10"});
  EXPECT_THAT(resp, ErrArg("item exists"));

  // Reserve with custom parameters
  resp = Run({"topk.reserve", "topk2", "10", "100", "7", "0.95"});
  EXPECT_EQ(resp, "OK");

  // Should fail with invalid k
  resp = Run({"topk.reserve", "topk3", "0"});
  EXPECT_THAT(resp, ErrArg("k must be greater than 0"));

  // Should fail with invalid decay
  resp = Run({"topk.reserve", "topk4", "5", "100", "7", "1.5"});
  EXPECT_THAT(resp, ErrArg("decay must be between 0 and 1"));
}

TEST_F(TopkFamilyTest, Add) {
  Run({"topk.reserve", "topk", "3"});

  // Add single item
  auto resp = Run({"topk.add", "topk", "foo"});
  EXPECT_EQ(resp.type, RespExpr::NIL);  // No expulsion

  // Add multiple items
  resp = Run({"topk.add", "topk", "bar", "baz"});
  EXPECT_THAT(resp, RespArray(ElementsAre(ArgType(RespExpr::NIL), ArgType(RespExpr::NIL))));

  // Fill the top-k
  Run({"topk.add", "topk", "item1", "item2", "item3"});

  // Should fail on non-existent key
  resp = Run({"topk.add", "noexist", "foo"});
  EXPECT_THAT(resp, ErrArg("no such key"));
}

TEST_F(TopkFamilyTest, IncrBy) {
  Run({"topk.reserve", "topk", "5"});

  // Increment single item
  auto resp = Run({"topk.incrby", "topk", "foo", "3"});
  EXPECT_EQ(resp.type, RespExpr::NIL);  // No expulsion

  // Increment multiple items
  resp = Run({"topk.incrby", "topk", "foo", "4", "bar", "1"});
  EXPECT_THAT(resp, RespArray(ElementsAre(ArgType(RespExpr::NIL), ArgType(RespExpr::NIL))));

  // Verify counts - note: counts are probabilistic and may not be exact
  resp = Run({"topk.count", "topk", "foo", "bar"});
  auto arr = resp.GetVec();
  EXPECT_GE(arr[0].GetInt(), 1);  // At least 1
  EXPECT_GE(arr[1].GetInt(), 1);  // At least 1

  // Should fail on non-existent key
  resp = Run({"topk.incrby", "noexist", "foo", "1"});
  EXPECT_THAT(resp, ErrArg("no such key"));

  // Should fail with invalid increment
  resp = Run({"topk.incrby", "topk", "foo", "0"});
  EXPECT_THAT(resp, ErrArg("increment must be between 1 and 100000"));

  resp = Run({"topk.incrby", "topk", "foo", "100001"});
  EXPECT_THAT(resp, ErrArg("increment must be between 1 and 100000"));

  // Should fail with invalid number
  resp = Run({"topk.incrby", "topk", "foo", "notanumber"});
  EXPECT_THAT(resp, ErrArg("value is not an integer"));
}

TEST_F(TopkFamilyTest, Query) {
  Run({"topk.reserve", "topk", "5"});
  Run({"topk.add", "topk", "foo", "bar", "baz"});

  // Query single item
  auto resp = Run({"topk.query", "topk", "foo"});
  EXPECT_THAT(resp, IntArg(1));  // Item is in top-k

  // Query multiple items
  resp = Run({"topk.query", "topk", "foo", "bar", "noexist"});
  EXPECT_THAT(resp, RespArray(ElementsAre(IntArg(1), IntArg(1), IntArg(0))));

  // Should fail on non-existent key
  resp = Run({"topk.query", "noexist", "foo"});
  EXPECT_THAT(resp, ErrArg("no such key"));
}

TEST_F(TopkFamilyTest, Count) {
  Run({"topk.reserve", "topk", "5"});
  Run({"topk.incrby", "topk", "foo", "5", "bar", "3", "baz", "9"});

  // Count single item. Counts are probabilistic check > 0
  auto resp = Run({"topk.count", "topk", "foo"});
  EXPECT_GE(resp.GetInt(), 1);

  // Count multiple items. Counts are probabilistic check > 0
  resp = Run({"topk.count", "topk", "foo", "bar", "baz"});
  auto arr = resp.GetVec();
  EXPECT_GE(arr[0].GetInt(), 1);
  EXPECT_GE(arr[1].GetInt(), 1);
  EXPECT_GE(arr[2].GetInt(), 1);

  // Count non-existent item
  resp = Run({"topk.count", "topk", "noexist"});
  EXPECT_THAT(resp, IntArg(0));

  // Should fail on non-existent key
  resp = Run({"topk.count", "noexist", "foo"});
  EXPECT_THAT(resp, ErrArg("no such key"));
}

TEST_F(TopkFamilyTest, List) {
  Run({"topk.reserve", "topk", "3"});
  Run({"topk.incrby", "topk", "foo", "100", "bar", "50", "baz", "150"});

  // List without counts
  auto resp = Run({"topk.list", "topk"});
  auto vec = resp.GetVec();
  EXPECT_EQ(vec.size(), 3u);
  // All three items should be in the list
  EXPECT_EQ(vec[0].GetString(), "baz");  // Highest count should be first

  // List with counts
  resp = Run({"topk.list", "topk", "WITHCOUNT"});
  vec = resp.GetVec();
  EXPECT_EQ(vec.size(), 6u);  // 3 items × 2 (item + count)

  // Verify structure: [item1, count1, item2, count2, item3, count3]
  // Counts should be in descending order (probabilistic, so just check they're positive)
  EXPECT_EQ(vec[0].GetString(), "baz");
  EXPECT_GE(vec[1].GetInt(), 1);  // Count is probabilistic, just verify > 0

  EXPECT_EQ(vec[2].GetString(), "foo");
  EXPECT_GE(vec[3].GetInt(), 1);

  EXPECT_EQ(vec[4].GetString(), "bar");
  EXPECT_GE(vec[5].GetInt(), 1);

  // Verify counts are in descending order
  EXPECT_GE(vec[1].GetInt(), vec[3].GetInt());  // baz >= foo
  EXPECT_GE(vec[3].GetInt(), vec[5].GetInt());  // foo >= bar

  // Should fail on non-existent key
  resp = Run({"topk.list", "noexist"});
  EXPECT_THAT(resp, ErrArg("no such key"));

  // Should fail with invalid flag
  resp = Run({"topk.list", "topk", "INVALID"});
  EXPECT_THAT(resp, ErrArg("syntax error"));
}

TEST_F(TopkFamilyTest, Info) {
  Run({"topk.reserve", "topk", "10", "100", "7", "0.95"});

  auto resp = Run({"topk.info", "topk"});
  EXPECT_THAT(resp, RespArray(ElementsAre("k", IntArg(10), "width", IntArg(100), "depth", IntArg(7),
                                          "decay", "0.95")));

  // Should fail on non-existent key
  resp = Run({"topk.info", "noexist"});
  EXPECT_THAT(resp, ErrArg("no such key"));
}

TEST_F(TopkFamilyTest, Expulsion) {
  // Create a small top-k to test expulsion
  Run({"topk.reserve", "topk", "2"});

  // Add items
  Run({"topk.incrby", "topk", "foo", "100"});
  Run({"topk.incrby", "topk", "bar", "80"});

  // Add new item with higher count to trigger expulsion (HeavyKeeper should evict the lowest)
  Run({"topk.incrby", "topk", "baz", "90"});

  // Verify we have exactly 2 items in top-k (after potential expulsion)
  auto resp = Run({"topk.list", "topk"});
  EXPECT_EQ(resp.GetVec().size(), 2u);

  // Verify at least the highest count item is still there
  resp = Run({"topk.query", "topk", "foo"});
  EXPECT_THAT(resp, IntArg(1));
}

TEST_F(TopkFamilyTest, DefaultParameters) {
  // Reserve with only k parameter (should use defaults for width, depth, decay)
  Run({"topk.reserve", "topk", "5"});

  auto resp = Run({"topk.info", "topk"});
  // Check that default parameters are used
  // Default width=8, depth=7, decay=0.9 (https://redis.io/docs/latest/commands/topk.reserve/)
  EXPECT_THAT(resp, RespArray(ElementsAre("k", IntArg(5), "width", IntArg(8), "depth", IntArg(7),
                                          "decay", "0.9")));
}

TEST_F(TopkFamilyTest, EmptyList) {
  Run({"topk.reserve", "topk", "5"});

  // List should be empty initially
  auto resp = Run({"topk.list", "topk"});
  EXPECT_THAT(resp, RespArray(ElementsAre()));

  resp = Run({"topk.list", "topk", "WITHCOUNT"});
  EXPECT_THAT(resp, RespArray(ElementsAre()));
}

TEST_F(TopkFamilyTest, WrongType) {
  // Create a string key
  Run({"set", "mystring", "value"});

  // TOPK commands should fail on wrong type
  auto resp = Run({"topk.add", "mystring", "foo"});
  EXPECT_THAT(resp, ErrArg("WRONGTYPE"));

  resp = Run({"topk.query", "mystring", "foo"});
  EXPECT_THAT(resp, ErrArg("WRONGTYPE"));

  resp = Run({"topk.list", "mystring"});
  EXPECT_THAT(resp, ErrArg("WRONGTYPE"));

  resp = Run({"topk.info", "mystring"});
  EXPECT_THAT(resp, ErrArg("WRONGTYPE"));
}

}  // namespace dfly
