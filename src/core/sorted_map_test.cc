// Copyright 2024, DragonflyDB authors.  All rights reserved.
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
  SortedMapTest() : mr_(mi_heap_get_backing()), sm_(&mr_) {
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
  SortedMap sm_;
};

TEST_F(SortedMapTest, Add) {
  int out_flags;
  double new_score;

  sds ele = sdsnew("a");
  int res = sm_.Add(1.0, ele, 0, &out_flags, &new_score);
  EXPECT_EQ(1, res);
  EXPECT_EQ(ZADD_OUT_ADDED, out_flags);
  EXPECT_EQ(1, new_score);

  res = sm_.Add(2.0, ele, ZADD_IN_NX, &out_flags, &new_score);
  EXPECT_EQ(1, res);
  EXPECT_EQ(ZADD_OUT_NOP, out_flags);

  res = sm_.Add(2.0, ele, ZADD_IN_INCR, &out_flags, &new_score);
  EXPECT_EQ(1, res);
  EXPECT_EQ(ZADD_OUT_UPDATED, out_flags);
  EXPECT_EQ(3, new_score);
  EXPECT_EQ(3, sm_.GetScore(ele));
}

TEST_F(SortedMapTest, Scan) {
  for (unsigned i = 0; i < 972; ++i) {
    sm_.Insert(i, sdsfromlonglong(i));
  }
  uint64_t cursor = 0;

  unsigned cnt = 0;
  do {
    cursor = sm_.Scan(cursor, [&](string_view str, double score) { ++cnt; });
  } while (cursor != 0);
  EXPECT_EQ(972, cnt);
}

TEST_F(SortedMapTest, InsertPop) {
  for (unsigned i = 0; i < 256; ++i) {
    sds s = sdsempty();

    s = sdscatfmt(s, "a%u", i);
    ASSERT_TRUE(sm_.Insert(1000, s));
  }

  vector<sds> vec;
  bool res = sm_.Iterate(1, 2, false, [&](sds ele, double score) {
    vec.push_back(ele);
    return true;
  });
  EXPECT_TRUE(res);
  EXPECT_THAT(vec, ElementsAre(StrEq("a1"), StrEq("a10")));

  sds s = sdsnew("a1");
  EXPECT_EQ(1, sm_.GetRank(s, false));
  EXPECT_EQ(254, sm_.GetRank(s, true));
  sdsfree(s);

  auto top_scores = sm_.PopTopScores(3, false);
  EXPECT_THAT(top_scores, ElementsAre(Pair(StrEq("a0"), 1000), Pair(StrEq("a1"), 1000),
                                      Pair(StrEq("a10"), 1000)));
  top_scores = sm_.PopTopScores(3, true);
  EXPECT_THAT(top_scores, ElementsAre(Pair(StrEq("a99"), 1000), Pair(StrEq("a98"), 1000),
                                      Pair(StrEq("a97"), 1000)));
}

TEST_F(SortedMapTest, LexRanges) {
  for (unsigned i = 0; i < 100; ++i) {
    sds s = sdsempty();

    s = sdscatfmt(s, "a%u", i);
    ASSERT_TRUE(sm_.Insert(1, s));
  }

  zlexrangespec range;
  range.max = sdsnew("a96");
  range.min = sdsnew("a93");
  range.maxex = 0;
  range.minex = 0;
  EXPECT_EQ(4, sm_.LexCount(range));
  auto array = sm_.GetLexRange(range, 1, 1000, false);
  ASSERT_EQ(3, array.size());
  EXPECT_THAT(array.front(), Pair("a94", 1));

  range.maxex = 1;
  EXPECT_EQ(3, sm_.LexCount(range));
  array = sm_.GetLexRange(range, 1, 1000, true);
  ASSERT_EQ(2, array.size());
  EXPECT_THAT(array.front(), Pair("a94", 1));

  range.minex = 1;
  EXPECT_EQ(2, sm_.LexCount(range));
  array = sm_.GetLexRange(range, 1, 1000, false);
  ASSERT_EQ(1, array.size());
  EXPECT_THAT(array.front(), Pair("a95", 1));
  sdsfree(range.min);

  range.min = range.max;
  EXPECT_EQ(0, sm_.LexCount(range));
  range.minex = 0;
  EXPECT_EQ(0, sm_.LexCount(range));
  sdsfree(range.max);

  range.maxex = 0;
  range.min = cminstring;
  range.max = sdsnew("a");
  EXPECT_EQ(0, sm_.LexCount(range));
  sdsfree(range.max);

  range.max = sdsnew("a0");
  EXPECT_EQ(1, sm_.LexCount(range));
  range.maxex = 1;
  EXPECT_EQ(0, sm_.LexCount(range));
  sdsfree(range.max);
}

TEST_F(SortedMapTest, ScoreRanges) {
  for (unsigned i = 0; i < 10; ++i) {
    sds s = sdsempty();

    s = sdscatfmt(s, "a%u", i);
    ASSERT_TRUE(sm_.Insert(1, s));
  }

  for (unsigned i = 0; i < 10; ++i) {
    sds s = sdsempty();

    s = sdscatfmt(s, "b%u", i);
    ASSERT_TRUE(sm_.Insert(2, s));
  }

  zrangespec range;
  range.max = 5;
  range.min = 1;
  range.maxex = 0;
  range.minex = 0;
  EXPECT_EQ(20, sm_.Count(range));
  detail::SortedMap::ScoredArray array = sm_.GetRange(range, 0, 1000, false);
  ASSERT_EQ(20, array.size());
  EXPECT_THAT(array.front(), Pair("a0", 1));
  EXPECT_THAT(array.back(), Pair("b9", 2));

  range.minex = 1;  // exclude all the "1" scores.
  EXPECT_EQ(10, sm_.Count(range));
  array = sm_.GetRange(range, 2, 1, false);
  ASSERT_EQ(1, array.size());
  EXPECT_THAT(array.front(), Pair("b2", 2));

  range.max = 1;
  range.minex = 0;
  range.min = -HUGE_VAL;
  EXPECT_EQ(10, sm_.Count(range));
  array = sm_.GetRange(range, 2, 2, true);
  ASSERT_EQ(2, array.size());
  EXPECT_THAT(array.back(), Pair("a6", 1));

  range.maxex = 1;
  EXPECT_EQ(0, sm_.Count(range));
  array = sm_.GetRange(range, 0, 2, true);
  ASSERT_EQ(0, array.size());

  range.min = 3;
  array = sm_.GetRange(range, 0, 2, true);
  ASSERT_EQ(0, array.size());
}

TEST_F(SortedMapTest, DeleteRange) {
  for (unsigned i = 0; i <= 100; ++i) {
    sds s = sdsempty();

    s = sdscatfmt(s, "a%u", i);
    ASSERT_TRUE(sm_.Insert(i * 2, s));
  }

  zrangespec range;
  range.min = range.max = 200;
  range.minex = range.maxex = 1;
  EXPECT_EQ(0, sm_.DeleteRangeByScore(range));

  range.min = 199;
  EXPECT_EQ(0, sm_.DeleteRangeByScore(range));

  range.minex = 0;
  EXPECT_EQ(0, sm_.DeleteRangeByScore(range));

  range.max = 199;
  range.min = 198;
  EXPECT_EQ(1, sm_.DeleteRangeByScore(range));

  range.max = 197;
  range.min = 193;
  EXPECT_EQ(2, sm_.DeleteRangeByScore(range));

  EXPECT_EQ(2, sm_.DeleteRangeByRank(0, 1));

  zlexrangespec lex_range;
  lex_range.min = sdsnew("b");
  lex_range.max = sdsnew("c");
  EXPECT_EQ(0, sm_.DeleteRangeByLex(lex_range));

  sdsfree(lex_range.min);
  sdsfree(lex_range.max);
  lex_range.min = cminstring;
  lex_range.max = cmaxstring;
  EXPECT_EQ(96, sm_.DeleteRangeByLex(lex_range));
}

// not a real test, just to see how much memory is used by zskiplist.
TEST_F(SortedMapTest, MemoryUsage) {
  zskiplist* zsl = zslCreate();
  std::vector<sds> sds_vec;
  for (size_t i = 0; i < 10'000; ++i) {
    sds_vec.push_back(sdsnew("f"));
  }
  size_t sz_before = zmalloc_used_memory_tl;
  LOG(INFO) << "zskiplist before: " << sz_before << " bytes";

  for (size_t i = 0; i < sds_vec.size(); ++i) {
    zslInsert(zsl, i, sds_vec[i]);
  }
  size_t delta = zmalloc_used_memory_tl - sz_before;
  LOG(INFO) << "zskiplist took: " << delta << " bytes for " << zsl->length << " elements, or "
            << double(delta) / zsl->length << " per element";
  zslFree(zsl);
}

uint64_t total_wasted_memory = 0;

TEST_F(SortedMapTest, ReallocIfNeeded) {
  auto build_str = [](size_t i) { return to_string(i) + string(131, 'a'); };

  auto count_waste = [](const mi_heap_t* heap, const mi_heap_area_t* area, void* block,
                        size_t block_size, void* arg) {
    size_t used = block_size * area->used;
    total_wasted_memory += area->committed - used;
    return true;
  };

  for (size_t i = 0; i < 10'000; i++) {
    int out_flags;
    double new_val;
    auto str = build_str(i);
    sds ele = sdsnew(str.c_str());
    sm_.Add(i, ele, 0, &out_flags, &new_val);
    sdsfree(ele);
  }

  for (size_t i = 0; i < 10'000; i++) {
    if (i % 10 == 0)
      continue;
    auto str = build_str(i);
    sds ele = sdsnew(str.c_str());
    sm_.Delete(ele);
    sdsfree(ele);
  }

  mi_heap_collect(mi_heap_get_backing(), true);
  mi_heap_visit_blocks(mi_heap_get_backing(), false, count_waste, nullptr);
  size_t wasted_before = total_wasted_memory;

  ASSERT_TRUE(sm_.DefragIfNeeded(9));

  total_wasted_memory = 0;
  mi_heap_collect(mi_heap_get_backing(), true);
  mi_heap_visit_blocks(mi_heap_get_backing(), false, count_waste, nullptr);
  size_t wasted_after = total_wasted_memory;

  // Check we waste significanlty less now
  EXPECT_GT(wasted_before, wasted_after * 2);

  ASSERT_EQ(sm_.Size(), 1000);
  auto cb = [i = 0, build_str](sds ele, double score) mutable -> bool {
    EXPECT_EQ(std::string_view(ele), build_str(i * 10));
    EXPECT_EQ((size_t)score, i * 10);
    ++i;
    return true;
  };

  sm_.Iterate(0, 10000, false, cb);
}

}  // namespace dfly
