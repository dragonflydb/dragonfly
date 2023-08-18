// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/sorted_map.h"

#include <gmock/gmock.h>
#include <mimalloc.h>

#include "base/gtest.h"
#include "base/logging.h"
#include "core/mi_memory_resource.h"

extern "C" {
#include "redis/zmalloc.h"
}

using namespace std;
using testing::ElementsAre;
using testing::Pair;
using testing::StrEq;

namespace dfly {

using detail::SortedMap;

class SortedMapTest : public ::testing::Test {
 protected:
  SortedMapTest() : mr_(mi_heap_get_backing()) {
  }

  static void SetUpTestSuite() {
    // configure redis lib zmalloc which requires mimalloc heap to work.
    auto* tlh = mi_heap_get_backing();
    init_zmalloc_threadlocal(tlh);
  }

  void AddMember(zskiplist* zsl, double score, sds ele) {
    zslInsert(zsl, score, ele);
  }

  MiMemoryResource mr_;
};

TEST_F(SortedMapTest, Add) {
  SortedMap sm(&mr_);

  int out_flags;
  double new_score;

  sds ele = sdsnew("a");
  int res = sm.Add(1.0, ele, 0, &out_flags, &new_score);
  EXPECT_EQ(1, res);
  EXPECT_EQ(ZADD_OUT_ADDED, out_flags);
  EXPECT_EQ(1, new_score);

  res = sm.Add(2.0, ele, ZADD_IN_NX, &out_flags, &new_score);
  EXPECT_EQ(1, res);
  EXPECT_EQ(ZADD_OUT_NOP, out_flags);

  res = sm.Add(2.0, ele, ZADD_IN_INCR, &out_flags, &new_score);
  EXPECT_EQ(1, res);
  EXPECT_EQ(ZADD_OUT_UPDATED, out_flags);
  EXPECT_EQ(3, new_score);
  EXPECT_EQ(3, sm.GetScore(ele));
}

TEST_F(SortedMapTest, Scan) {
  SortedMap sm(&mr_);

  for (unsigned i = 0; i < 972; ++i) {
    sm.Insert(i, sdsfromlonglong(i));
  }
  uint64_t cursor = 0;

  unsigned cnt = 0;
  do {
    cursor = sm.Scan(cursor, [&](string_view str, double score) { ++cnt; });
  } while (cursor != 0);
  EXPECT_EQ(972, cnt);
}

TEST_F(SortedMapTest, InsertPop) {
  SortedMap sm(&mr_);
  for (unsigned i = 0; i < 256; ++i) {
    sds s = sdsempty();

    s = sdscatfmt(s, "a%u", i);
    ASSERT_TRUE(sm.Insert(1000, s));
  }

  vector<sds> vec;
  bool res = sm.Iterate(1, 2, false, [&](sds ele, double score) {
    vec.push_back(ele);
    return true;
  });
  EXPECT_TRUE(res);
  EXPECT_THAT(vec, ElementsAre(StrEq("a1"), StrEq("a10")));

  sds s = sdsnew("a1");
  EXPECT_EQ(1, sm.GetRank(s, false));
  EXPECT_EQ(254, sm.GetRank(s, true));
  sdsfree(s);

  auto top_scores = sm.PopTopScores(3, false);
  EXPECT_THAT(top_scores, ElementsAre(Pair(StrEq("a0"), 1000), Pair(StrEq("a1"), 1000),
                                      Pair(StrEq("a10"), 1000)));
  top_scores = sm.PopTopScores(3, true);
  EXPECT_THAT(top_scores, ElementsAre(Pair(StrEq("a99"), 1000), Pair(StrEq("a98"), 1000),
                                      Pair(StrEq("a97"), 1000)));
}

TEST_F(SortedMapTest, ScoreRanges) {
  SortedMap sm(&mr_);

  for (unsigned i = 0; i < 10; ++i) {
    sds s = sdsempty();

    s = sdscatfmt(s, "a%u", i);
    ASSERT_TRUE(sm.Insert(1, s));
  }

  for (unsigned i = 0; i < 10; ++i) {
    sds s = sdsempty();

    s = sdscatfmt(s, "b%u", i);
    ASSERT_TRUE(sm.Insert(2, s));
  }

  zrangespec range;
  range.max = 5;
  range.min = 1;
  range.maxex = 0;
  range.minex = 0;
  EXPECT_EQ(20, sm.Count(range));

  range.minex = 1;  // exclude all the "1" scores.
  EXPECT_EQ(10, sm.Count(range));

  range.max = 1;
  range.minex = 0;
  range.min = -HUGE_VAL;
  EXPECT_EQ(10, sm.Count(range));

  range.maxex = 1;
  EXPECT_EQ(0, sm.Count(range));
}

}  // namespace dfly
