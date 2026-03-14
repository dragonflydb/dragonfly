// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/cms.h"

#include <absl/strings/str_cat.h>

#include <cmath>

#include "base/gtest.h"

namespace dfly {

using namespace std;

class CMSTest : public ::testing::Test {
 protected:
  CMSTest() : cms_(CMS(1000, 5, PMR_NS::get_default_resource())) {
  }

  CMS cms_;
};

// A freshly created CMS must return 0 for any item.
TEST_F(CMSTest, InitialCountIsZero) {
  EXPECT_EQ(cms_.Query("nonexistent"), 0);
  EXPECT_EQ(cms_.Query(""), 0);
  EXPECT_EQ(cms_.Query("anything"), 0);
}

// Use width=1 so every item maps to column 0, exercising all counters.
// This catches initialization bugs (e.g. counters not zeroed).
TEST(CMSBasic, InitialCountIsZeroSmall) {
  CMS cms(1, 1, PMR_NS::get_default_resource());
  EXPECT_EQ(cms.Query("x"), 0);
  EXPECT_EQ(cms.Query("y"), 0);
}

TEST(CMSBasic, IncrBySmall) {
  CMS cms(1, 1, PMR_NS::get_default_resource());
  EXPECT_EQ(cms.IncrBy("a", 3), 3);
  // width=1 means all items collide; "b" should also return 3.
  EXPECT_EQ(cms.Query("b"), 3);
}

// Inspired by fakeredis test_cms_create: initbyprob computes correct dimensions.
TEST(CMSBasic, InitByProb) {
  CMS cms = CMS::CreateByProb(0.01, 0.01, PMR_NS::get_default_resource());

  // width = ceil(e / 0.01) = ceil(271.8..) = 272
  EXPECT_EQ(cms.width(), static_cast<uint32_t>(std::ceil(std::exp(1.0) / 0.01)));
  // depth = ceil(ln(1/0.01)) = ceil(4.605..) = 5
  EXPECT_EQ(cms.depth(), static_cast<uint32_t>(std::ceil(std::log(100.0))));
  EXPECT_EQ(cms.Query("anything"), 0);
}

// Inspired by fakeredis test_cms_incrby: multiple items, incremental updates.
TEST_F(CMSTest, IncrByMultipleItems) {
  EXPECT_EQ(cms_.IncrBy("foo", 3), 3);
  cms_.IncrBy("foo", 4);
  cms_.IncrBy("bar", 1);

  EXPECT_GE(cms_.Query("foo"), 7);
  EXPECT_GE(cms_.Query("bar"), 1);
  EXPECT_EQ(cms_.Query("noexist"), 0);
}

TEST_F(CMSTest, BasicIncrBy) {
  int64_t count = cms_.IncrBy("foo", 5);
  EXPECT_EQ(count, 5);

  count = cms_.IncrBy("foo", 3);
  EXPECT_EQ(count, 8);

  EXPECT_EQ(cms_.Query("foo"), 8);
}

TEST_F(CMSTest, QueryReturnsMinimum) {
  cms_.IncrBy("a", 10);
  cms_.IncrBy("b", 20);

  // CMS can overestimate, but never underestimate.
  EXPECT_GE(cms_.Query("a"), 10);
  EXPECT_GE(cms_.Query("b"), 20);
}

TEST_F(CMSTest, NeverUnderestimates) {
  for (int i = 0; i < 500; ++i) {
    string key = absl::StrCat("item", i);
    cms_.IncrBy(key, i + 1);
  }

  for (int i = 0; i < 500; ++i) {
    string key = absl::StrCat("item", i);
    EXPECT_GE(cms_.Query(key), i + 1) << "Underestimate for " << key;
  }
}

TEST_F(CMSTest, UnseenItemIsZero) {
  cms_.IncrBy("known", 100);
  // With width=1000 and depth=5 and only one item inserted, collisions are unlikely.
  EXPECT_LE(cms_.Query("unknown"), 5);
}

TEST_F(CMSTest, Dimensions) {
  EXPECT_EQ(cms_.width(), 1000u);
  EXPECT_EQ(cms_.depth(), 5u);
}

TEST_F(CMSTest, MallocUsed) {
  EXPECT_EQ(cms_.MallocUsed(), 1000u * 5 * sizeof(int64_t));
}

// Inspired by fakeredis test_cms_merge: basic merge of two sketches.
TEST_F(CMSTest, MergeFrom) {
  CMS other(1000, 5, PMR_NS::get_default_resource());
  cms_.IncrBy("foo", 3);
  other.IncrBy("foo", 4);
  other.IncrBy("bar", 1);

  EXPECT_TRUE(cms_.MergeFrom(other));
  EXPECT_GE(cms_.Query("foo"), 7);
  EXPECT_GE(cms_.Query("bar"), 1);
}

TEST_F(CMSTest, MergeFromWithWeight) {
  CMS other(1000, 5, PMR_NS::get_default_resource());
  other.IncrBy("x", 5);

  cms_.IncrBy("x", 10);
  EXPECT_TRUE(cms_.MergeFrom(other, 3));
  // 10 + 5*3 = 25
  EXPECT_GE(cms_.Query("x"), 25);
}

TEST_F(CMSTest, MergeDimensionMismatch) {
  CMS other(500, 5, PMR_NS::get_default_resource());
  EXPECT_FALSE(cms_.MergeFrom(other));

  CMS other2(1000, 3, PMR_NS::get_default_resource());
  EXPECT_FALSE(cms_.MergeFrom(other2));
}

// Inspired by fakeredis test_cms_info: merge multiple sources with weights, verify counts.
// Mirrors the exact sequence: C=A+B, C+=A*1+B*2, C+=A*2+B*3, then check info.count.
TEST(CMSBasic, MergeMultipleWithWeights) {
  auto* mr = PMR_NS::get_default_resource();
  CMS a(1000, 5, mr);
  CMS b(1000, 5, mr);
  CMS c(1000, 5, mr);

  a.IncrBy("foo", 5);
  a.IncrBy("bar", 3);
  a.IncrBy("baz", 9);

  b.IncrBy("foo", 2);
  b.IncrBy("bar", 3);
  b.IncrBy("baz", 1);

  EXPECT_EQ(a.Query("foo"), 5);
  EXPECT_EQ(a.Query("bar"), 3);
  EXPECT_EQ(a.Query("baz"), 9);
  EXPECT_EQ(b.Query("foo"), 2);
  EXPECT_EQ(b.Query("bar"), 3);
  EXPECT_EQ(b.Query("baz"), 1);

  // C = A*1 + B*1
  EXPECT_TRUE(c.MergeFrom(a));
  EXPECT_TRUE(c.MergeFrom(b));
  EXPECT_EQ(c.Query("foo"), 7);
  EXPECT_EQ(c.Query("bar"), 6);
  EXPECT_EQ(c.Query("baz"), 10);

  // C += A*1 + B*2
  EXPECT_TRUE(c.MergeFrom(a, 1));
  EXPECT_TRUE(c.MergeFrom(b, 2));
  EXPECT_EQ(c.Query("foo"), 16);
  EXPECT_EQ(c.Query("bar"), 15);
  EXPECT_EQ(c.Query("baz"), 21);

  // C += A*2 + B*3
  EXPECT_TRUE(c.MergeFrom(a, 2));
  EXPECT_TRUE(c.MergeFrom(b, 3));
  EXPECT_EQ(c.Query("foo"), 32);
  EXPECT_EQ(c.Query("bar"), 30);
  EXPECT_EQ(c.Query("baz"), 42);
}

// Inspired by fakeredis test_cms_info: verify count tracks total of all IncrBy operations.
TEST(CMSBasic, CountTracking) {
  auto* mr = PMR_NS::get_default_resource();
  CMS a(1000, 5, mr);

  EXPECT_EQ(a.total_count(), 0);

  a.IncrBy("foo", 5);
  a.IncrBy("bar", 3);
  a.IncrBy("baz", 9);
  // total_count = 5 + 3 + 9 = 17 (matches fakeredis test_cms_info assertion)
  EXPECT_EQ(a.total_count(), 17);
}

// Inspired by fakeredis test_cms_info: count is updated by MergeFrom.
TEST(CMSBasic, CountAfterMerge) {
  auto* mr = PMR_NS::get_default_resource();
  CMS a(1000, 5, mr);
  CMS b(1000, 5, mr);
  CMS c(1000, 5, mr);

  a.IncrBy("foo", 5);
  a.IncrBy("bar", 3);
  a.IncrBy("baz", 9);
  EXPECT_EQ(a.total_count(), 17);

  b.IncrBy("foo", 2);
  b.IncrBy("bar", 3);
  b.IncrBy("baz", 1);
  EXPECT_EQ(b.total_count(), 6);

  // C = A + B -> total_count = 17 + 6 = 23
  c.MergeFrom(a);
  c.MergeFrom(b);
  EXPECT_EQ(c.total_count(), 23);

  // C += A*1 + B*2 -> total_count = 23 + 17*1 + 6*2 = 52
  // (matches fakeredis test_cms_merge_fail assertion: count == 52)
  c.MergeFrom(a, 1);
  c.MergeFrom(b, 2);
  EXPECT_EQ(c.total_count(), 52);
}

TEST_F(CMSTest, MoveConstruct) {
  cms_.IncrBy("foo", 42);
  CMS moved(std::move(cms_));

  EXPECT_EQ(moved.Query("foo"), 42);
  EXPECT_EQ(moved.width(), 1000u);
  EXPECT_EQ(moved.depth(), 5u);
}

TEST_F(CMSTest, MoveAssign) {
  cms_.IncrBy("foo", 42);
  CMS other(500, 3, PMR_NS::get_default_resource());
  other = std::move(cms_);

  EXPECT_EQ(other.Query("foo"), 42);
  EXPECT_EQ(other.width(), 1000u);
  EXPECT_EQ(other.depth(), 5u);
}

}  // namespace dfly
