// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "facade/facade_test.h"
#include "server/test_utils.h"

namespace dfly {

using testing::ElementsAre;

class CmsFamilyTest : public BaseFamilyTest {
 protected:
};

TEST_F(CmsFamilyTest, InitByDim) {
  auto resp = Run({"cms.initbydim", "cms1", "1000", "5"});
  EXPECT_EQ(resp, "OK");
  EXPECT_EQ(Run({"type", "cms1"}), "CMSk-TYPE-");

  // Should fail on existing key
  resp = Run({"cms.initbydim", "cms1", "1000", "5"});
  EXPECT_THAT(resp, ErrArg("key already exists"));

  // Invalid width
  resp = Run({"cms.initbydim", "cms2", "0", "5"});
  EXPECT_THAT(resp, ErrArg("width must be a positive integer"));

  // Invalid depth
  resp = Run({"cms.initbydim", "cms3", "1000", "0"});
  EXPECT_THAT(resp, ErrArg("depth must be a positive integer"));
}

TEST_F(CmsFamilyTest, InitByProb) {
  auto resp = Run({"cms.initbyprob", "cms1", "0.01", "0.001"});
  EXPECT_EQ(resp, "OK");
  EXPECT_EQ(Run({"type", "cms1"}), "CMSk-TYPE-");

  // Should fail on existing key
  resp = Run({"cms.initbyprob", "cms1", "0.01", "0.001"});
  EXPECT_THAT(resp, ErrArg("key already exists"));

  // Invalid error rate
  resp = Run({"cms.initbyprob", "cms2", "0", "0.001"});
  EXPECT_THAT(resp, ErrArg("error must be a value between 0 and 1 exclusive"));

  resp = Run({"cms.initbyprob", "cms3", "1", "0.001"});
  EXPECT_THAT(resp, ErrArg("error must be a value between 0 and 1 exclusive"));

  // Invalid probability
  resp = Run({"cms.initbyprob", "cms4", "0.01", "0"});
  EXPECT_THAT(resp, ErrArg("probability must be a value between 0 and 1 exclusive"));

  resp = Run({"cms.initbyprob", "cms5", "0.01", "1"});
  EXPECT_THAT(resp, ErrArg("probability must be a value between 0 and 1 exclusive"));
}

TEST_F(CmsFamilyTest, IncrBy) {
  // IncrBy on non-existing key should fail (Redis behavior)
  auto resp = Run({"cms.incrby", "noexist", "item1", "5"});
  EXPECT_THAT(resp, ErrArg("key does not exist"));

  // Create CMS first
  Run({"cms.initbydim", "cms1", "100", "5"});

  // Single item increment
  resp = Run({"cms.incrby", "cms1", "foo", "3"});
  EXPECT_THAT(resp, IntArg(3));  // Single-element array collapsed by test framework

  // Multiple items
  resp = Run({"cms.incrby", "cms1", "foo", "4", "bar", "1"});
  EXPECT_THAT(resp, RespArray(ElementsAre(IntArg(7), IntArg(1))));

  // Query to verify
  resp = Run({"cms.query", "cms1", "foo", "bar"});
  EXPECT_THAT(resp, RespArray(ElementsAre(IntArg(7), IntArg(1))));

  // Query non-existing item returns 0
  resp = Run({"cms.query", "cms1", "noexist"});
  EXPECT_THAT(resp, IntArg(0));  // Single-element array collapsed by test framework

  // Wrong type
  Run({"set", "str", "foo"});
  resp = Run({"cms.incrby", "str", "item1", "5"});
  EXPECT_THAT(resp, ErrArg("WRONGTYPE"));

  // Invalid increment
  resp = Run({"cms.incrby", "cms1", "foo", "invalid"});
  EXPECT_THAT(resp, ErrArg("Cannot parse number"));
}

TEST_F(CmsFamilyTest, Query) {
  Run({"cms.initbydim", "cms1", "1000", "5"});
  Run({"cms.incrby", "cms1", "item1", "10", "item2", "20", "item3", "30"});

  auto resp = Run({"cms.query", "cms1", "item1", "item2", "item3", "item4"});
  EXPECT_THAT(resp, RespArray(ElementsAre(IntArg(10), IntArg(20), IntArg(30), IntArg(0))));

  // Query non-existing key should fail (Redis behavior)
  resp = Run({"cms.query", "nonexistent", "item1"});
  EXPECT_THAT(resp, ErrArg("key does not exist"));

  // Wrong type
  Run({"set", "str", "foo"});
  resp = Run({"cms.query", "str", "item1"});
  EXPECT_THAT(resp, ErrArg("WRONGTYPE"));
}

TEST_F(CmsFamilyTest, Info) {
  Run({"cms.initbydim", "cms1", "1000", "5"});
  Run({"cms.incrby", "cms1", "item1", "10", "item2", "20"});

  auto resp = Run({"cms.info", "cms1"});
  EXPECT_THAT(resp, RespArray(ElementsAre("width", IntArg(1000), "depth", IntArg(5), "count",
                                          IntArg(30))));

  // Non-existing key should fail (Redis behavior)
  resp = Run({"cms.info", "nonexistent"});
  EXPECT_THAT(resp, ErrArg("key does not exist"));
}

TEST_F(CmsFamilyTest, Merge) {
  // Create source and destination sketches
  Run({"cms.initbydim", "src1", "1000", "5"});
  Run({"cms.initbydim", "dest", "1000", "5"});
  Run({"cms.incrby", "src1", "item1", "10", "item2", "20"});

  // Merge single source into destination
  auto resp = Run({"cms.merge", "dest", "1", "src1"});
  EXPECT_EQ(resp, "OK");

  // Verify destination has the merged data
  resp = Run({"cms.query", "dest", "item1", "item2"});
  EXPECT_THAT(resp, RespArray(ElementsAre(IntArg(10), IntArg(20))));

  // Verify info shows correct count
  resp = Run({"cms.info", "dest"});
  ASSERT_THAT(resp, ArrLen(6));
  EXPECT_THAT(resp.GetVec()[1], IntArg(1000));  // width
  EXPECT_THAT(resp.GetVec()[3], IntArg(5));     // depth
  EXPECT_THAT(resp.GetVec()[5], IntArg(30));    // count = 10 + 20

  // Merge on non-existing destination should fail
  resp = Run({"cms.merge", "noexist", "1", "src1"});
  EXPECT_THAT(resp, ErrArg("key does not exist"));

  // Merge with non-existing source should fail
  resp = Run({"cms.merge", "dest", "1", "noexist"});
  EXPECT_THAT(resp, ErrArg("key does not exist"));
}

TEST_F(CmsFamilyTest, MergeDimensionMismatch) {
  Run({"cms.initbydim", "src1", "1000", "5"});
  Run({"cms.initbydim", "src2", "2000", "5"});  // Different width
  Run({"cms.initbydim", "dest", "1000", "5"});

  // Sources have different dimensions
  auto resp = Run({"cms.merge", "dest", "2", "src1", "src2"});
  EXPECT_THAT(resp, ErrArg("width/depth"));

  // Destination has different dimensions than source
  Run({"cms.initbydim", "dest2", "500", "5"});
  resp = Run({"cms.merge", "dest2", "1", "src1"});
  EXPECT_THAT(resp, ErrArg("width/depth"));
}

TEST_F(CmsFamilyTest, IncrByNegative) {
  Run({"cms.initbydim", "cms1", "1000", "5"});
  Run({"cms.incrby", "cms1", "item1", "100"});
  Run({"cms.incrby", "cms1", "item1", "-30"});

  auto resp = Run({"cms.query", "cms1", "item1"});
  EXPECT_THAT(resp, IntArg(70));
}

TEST_F(CmsFamilyTest, MergeMultipleSources) {
  // Create two source sketches and destination with same dimensions
  Run({"cms.initbydim", "src1", "1000", "5"});
  Run({"cms.initbydim", "src2", "1000", "5"});
  Run({"cms.initbydim", "dest", "1000", "5"});

  // Add different items to each source
  Run({"cms.incrby", "src1", "foo", "10", "bar", "20"});
  Run({"cms.incrby", "src2", "baz", "30", "qux", "40"});

  // Merge both sources into destination
  auto resp = Run({"cms.merge", "dest", "2", "src1", "src2"});
  EXPECT_EQ(resp, "OK");

  // Verify all items from both sources are in destination
  resp = Run({"cms.query", "dest", "foo", "bar", "baz", "qux"});
  EXPECT_THAT(resp, RespArray(ElementsAre(IntArg(10), IntArg(20), IntArg(30), IntArg(40))));

  // Verify total count
  resp = Run({"cms.info", "dest"});
  EXPECT_THAT(resp, RespArray(ElementsAre("width", IntArg(1000), "depth", IntArg(5), "count",
                                          IntArg(100))));  // 10+20+30+40 = 100
}

TEST_F(CmsFamilyTest, MergeWithWeights) {
  Run({"cms.initbydim", "src1", "1000", "5"});
  Run({"cms.initbydim", "src2", "1000", "5"});
  Run({"cms.initbydim", "dest", "1000", "5"});

  Run({"cms.incrby", "src1", "item", "10"});
  Run({"cms.incrby", "src2", "item", "10"});

  // Merge with weights: src1*2 + src2*3 = 10*2 + 10*3 = 50
  auto resp = Run({"cms.merge", "dest", "2", "src1", "src2", "WEIGHTS", "2", "3"});
  EXPECT_EQ(resp, "OK");

  resp = Run({"cms.query", "dest", "item"});
  EXPECT_THAT(resp, IntArg(50));
}

TEST_F(CmsFamilyTest, MergeReplaces) {
  // Test that merge replaces destination instead of adding to it (Redis behavior)
  Run({"cms.initbydim", "A", "1000", "5"});
  Run({"cms.initbydim", "B", "1000", "5"});
  Run({"cms.initbydim", "C", "1000", "5"});

  Run({"cms.incrby", "A", "foo", "5", "bar", "3", "baz", "9"});
  Run({"cms.incrby", "B", "foo", "2", "bar", "3", "baz", "1"});

  // First merge: C = A + B
  auto resp = Run({"cms.merge", "C", "2", "A", "B"});
  EXPECT_EQ(resp, "OK");
  resp = Run({"cms.query", "C", "foo", "bar", "baz"});
  EXPECT_THAT(resp, RespArray(ElementsAre(IntArg(7), IntArg(6), IntArg(10))));

  // Second merge with weights: C = A*1 + B*2 (should REPLACE, not add to existing C)
  resp = Run({"cms.merge", "C", "2", "A", "B", "WEIGHTS", "1", "2"});
  EXPECT_EQ(resp, "OK");
  // Expected: foo=5*1+2*2=9, bar=3*1+3*2=9, baz=9*1+1*2=11
  resp = Run({"cms.query", "C", "foo", "bar", "baz"});
  EXPECT_THAT(resp, RespArray(ElementsAre(IntArg(9), IntArg(9), IntArg(11))));

  // Verify count is replaced too
  resp = Run({"cms.info", "C"});
  // count = (5+3+9)*1 + (2+3+1)*2 = 17 + 12 = 29
  EXPECT_THAT(resp, RespArray(ElementsAre("width", IntArg(1000), "depth", IntArg(5), "count",
                                          IntArg(29))));
}

}  // namespace dfly
