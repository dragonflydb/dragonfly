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
  EXPECT_EQ(Run({"type", "cms1"}), "CMSk-TYPE");

  // Should fail on existing key
  resp = Run({"cms.initbydim", "cms1", "100", "5"});
  EXPECT_THAT(resp, ErrArg("item exists"));

  // Should fail with invalid dimensions
  resp = Run({"cms.initbydim", "cms2", "0", "5"});
  EXPECT_THAT(resp, ErrArg("width and depth must be greater than 0"));

  resp = Run({"cms.initbydim", "cms3", "5", "0"});
  EXPECT_THAT(resp, ErrArg("width and depth must be greater than 0"));
}

TEST_F(CmsFamilyTest, InitByProb) {
  auto resp = Run({"cms.initbyprob", "cms1", "0.01", "0.01"});
  EXPECT_EQ(resp, "OK");

  // Should fail on existing key
  resp = Run({"cms.initbyprob", "cms1", "0.01", "0.01"});
  EXPECT_THAT(resp, ErrArg("item exists"));

  // Should fail with invalid parameters
  resp = Run({"cms.initbyprob", "cms2", "2", "0.01"});
  EXPECT_THAT(resp, ErrArg("error must be between 0 and 1"));

  resp = Run({"cms.initbyprob", "cms3", "0.01", "0"});
  EXPECT_THAT(resp, ErrArg("probability must be between 0 and 1"));
}

TEST_F(CmsFamilyTest, IncrBy) {
  Run({"cms.initbydim", "cms", "100", "5"});

  auto resp = Run({"cms.incrby", "cms", "foo", "3"});
  EXPECT_THAT(resp, IntArg(3));  // single-element array are collapsed

  resp = Run({"cms.incrby", "cms", "foo", "4", "bar", "1"});
  EXPECT_THAT(resp, RespArray(ElementsAre(IntArg(7), IntArg(1))));

  // Should fail on non-existent key
  resp = Run({"cms.incrby", "noexist", "foo", "1"});
  EXPECT_THAT(resp, ErrArg("CMS: key does not exist"));

  // Should fail with invalid number
  resp = Run({"cms.incrby", "cms", "foo", "notanumber"});
  EXPECT_THAT(resp, ErrArg("CMS: Cannot parse number"));
}

TEST_F(CmsFamilyTest, Query) {
  Run({"cms.initbydim", "cms", "100", "5"});
  Run({"cms.incrby", "cms", "foo", "5", "bar", "3"});

  auto resp = Run({"cms.query", "cms", "foo"});
  EXPECT_THAT(resp, IntArg(5));  // single-element array are collapsed

  resp = Run({"cms.query", "cms", "foo", "bar"});
  EXPECT_THAT(resp, RespArray(ElementsAre(IntArg(5), IntArg(3))));

  resp = Run({"cms.query", "cms", "noexist"});
  EXPECT_THAT(resp, IntArg(0));

  // Should fail on non-existent key
  resp = Run({"cms.query", "noexist", "foo"});
  EXPECT_THAT(resp, ErrArg("CMS: key does not exist"));
}

TEST_F(CmsFamilyTest, Info) {
  Run({"cms.initbydim", "cms", "1000", "5"});
  Run({"cms.incrby", "cms", "foo", "5", "bar", "3", "baz", "9"});

  auto resp = Run({"cms.info", "cms"});
  EXPECT_THAT(
      resp, RespArray(ElementsAre("width", IntArg(1000), "depth", IntArg(5), "count", IntArg(17))));

  // Should fail on non-existent key
  resp = Run({"cms.info", "noexist"});
  EXPECT_THAT(resp, ErrArg("CMS: key does not exist"));
}

TEST_F(CmsFamilyTest, Merge) {
  Run({"cms.initbydim", "A", "100", "5"});
  Run({"cms.initbydim", "B", "100", "5"});
  Run({"cms.initbydim", "C", "100", "5"});

  Run({"cms.incrby", "A", "foo", "5", "bar", "3", "baz", "9"});
  Run({"cms.incrby", "B", "foo", "2", "foobar", "3", "baz", "1"});

  // Verify initial values
  auto resp = Run({"cms.query", "A", "foo", "bar", "baz"});
  EXPECT_THAT(resp, RespArray(ElementsAre(IntArg(5), IntArg(3), IntArg(9))));

  resp = Run({"cms.query", "B", "foo", "foobar", "baz"});
  EXPECT_THAT(resp, RespArray(ElementsAre(IntArg(2), IntArg(3), IntArg(1))));

  // Merge A and B into C
  resp = Run({"cms.merge", "C", "2", "A", "B"});
  EXPECT_EQ(resp, "OK");

  resp = Run({"cms.query", "C", "foo", "bar", "baz", "foobar"});
  EXPECT_THAT(resp, RespArray(ElementsAre(IntArg(7), IntArg(3), IntArg(10), IntArg(3))));

  // Should fail on non-existent destination
  resp = Run({"cms.merge", "noexist", "1", "A"});
  EXPECT_THAT(resp, ErrArg("CMS: key does not exist"));

  // Should fail with wrong number of keys
  resp = Run({"cms.merge", "C", "0", "A"});
  EXPECT_THAT(resp, ErrArg("CMS: wrong number of keys"));
}

TEST_F(CmsFamilyTest, MergeWithWeights) {
  Run({"cms.initbydim", "A", "100", "5"});
  Run({"cms.initbydim", "B", "100", "5"});
  Run({"cms.initbydim", "C", "100", "5"});

  Run({"cms.incrby", "A", "foo", "5", "bar", "3", "baz", "9"});
  Run({"cms.incrby", "B", "foo", "2", "bar", "3", "baz", "1"});

  // Merge with weights
  auto resp = Run({"cms.merge", "C", "2", "A", "B", "WEIGHTS", "2", "3"});
  EXPECT_EQ(resp, "OK");

  // A contributes 2x, B contributes 3x
  // foo: 5*2 + 2*3 = 16
  // bar: 3*2 + 3*3 = 15
  // baz: 9*2 + 1*3 = 21
  resp = Run({"cms.query", "C", "foo", "bar", "baz"});
  EXPECT_THAT(resp, RespArray(ElementsAre(IntArg(16), IntArg(15), IntArg(21))));
}

}  // namespace dfly
