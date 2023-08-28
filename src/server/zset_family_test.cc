// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/zset_family.h"

#include "base/gtest.h"
#include "base/logging.h"
#include "facade/facade_test.h"
#include "server/command_registry.h"
#include "server/test_utils.h"

using namespace testing;
using namespace std;
using namespace util;

namespace dfly {

class ZSetFamilyTest : public BaseFamilyTest {
 protected:
};

TEST_F(ZSetFamilyTest, Add) {
  auto resp = Run({"zadd", "x", "1.1", "a"});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"zscore", "x", "a"});
  EXPECT_THAT(resp, "1.1");

  resp = Run({"zadd", "x", "2", "a"});
  EXPECT_THAT(resp, IntArg(0));
  resp = Run({"zscore", "x", "a"});
  EXPECT_THAT(resp, "2");

  resp = Run({"zadd", "x", "ch", "3", "a"});
  EXPECT_THAT(resp, IntArg(1));
  resp = Run({"zscore", "x", "a"});
  EXPECT_EQ(resp, "3");

  resp = Run({"zcard", "x"});
  EXPECT_THAT(resp, IntArg(1));

  EXPECT_THAT(Run({"zadd", "x", "", "a"}), ErrArg("not a valid float"));

  EXPECT_THAT(Run({"zadd", "ztmp", "xx", "10", "member"}), IntArg(0));

  const char kHighPrecision[] = "0.79028573343077946";

  Run({"zadd", "zs", kHighPrecision, "a"});
  EXPECT_EQ(Run({"zscore", "zs", "a"}), "0.7902857334307795");
  EXPECT_EQ(0.79028573343077946, 0.7902857334307795);
}

TEST_F(ZSetFamilyTest, ZRem) {
  auto resp = Run({"zadd", "x", "1.1", "b", "2.1", "a"});
  EXPECT_THAT(resp, IntArg(2));

  resp = Run({"zrem", "x", "b", "c"});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"zcard", "x"});
  EXPECT_THAT(resp, IntArg(1));
  EXPECT_THAT(Run({"zrange", "x", "0", "3", "byscore"}), "a");
  EXPECT_THAT(Run({"zrange", "x", "(-inf", "(+inf", "byscore"}), "a");
}

TEST_F(ZSetFamilyTest, ZMScore) {
  Run({"zadd", "zms", "3.14", "a"});
  Run({"zadd", "zms", "42", "another"});

  auto resp = Run({"zmscore", "zms", "another", "a", "nofield"});
  ASSERT_EQ(RespExpr::ARRAY, resp.type);
  EXPECT_THAT(resp.GetVec(), ElementsAre("42", "3.14", ArgType(RespExpr::NIL)));
}

TEST_F(ZSetFamilyTest, ZRangeRank) {
  Run({"zadd", "x", "1.1", "a", "2.1", "b"});
  EXPECT_THAT(Run({"zrangebyscore", "x", "0", "(1.1"}), ArrLen(0));
  EXPECT_THAT(Run({"zrangebyscore", "x", "-inf", "1.1", "limit", "0", "10"}), "a");

  auto resp = Run({"zrangebyscore", "x", "-inf", "1.1", "limit", "0", "10", "WITHSCORES"});
  ASSERT_THAT(resp, ArrLen(2));
  ASSERT_THAT(resp.GetVec(), ElementsAre("a", "1.1"));

  resp = Run({"zrangebyscore", "x", "-inf", "1.1", "WITHSCORES", "limit", "0", "10"});
  ASSERT_THAT(resp, ArrLen(2));
  ASSERT_THAT(resp.GetVec(), ElementsAre("a", "1.1"));

  resp = Run({"zrangebyscore", "x", "-inf", "+inf", "LIMIT", "0", "-1"});
  ASSERT_THAT(resp, ArrLen(2));
  ASSERT_THAT(resp.GetVec(), ElementsAre("a", "b"));

  resp = Run({"zrevrangebyscore", "x", "+inf", "-inf", "limit", "0", "5"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  ASSERT_THAT(resp.GetVec(), ElementsAre("b", "a"));

  EXPECT_EQ(2, CheckedInt({"zcount", "x", "1.1", "2.1"}));
  EXPECT_EQ(1, CheckedInt({"zcount", "x", "(1.1", "2.1"}));
  EXPECT_EQ(0, CheckedInt({"zcount", "y", "(1.1", "2.1"}));

  EXPECT_EQ(0, CheckedInt({"zrank", "x", "a"}));
  EXPECT_EQ(1, CheckedInt({"zrank", "x", "b"}));
  EXPECT_EQ(1, CheckedInt({"zrevrank", "x", "a"}));
  EXPECT_EQ(0, CheckedInt({"zrevrank", "x", "b"}));
  EXPECT_THAT(Run({"zrevrank", "x", "c"}), ArgType(RespExpr::NIL));
  EXPECT_THAT(Run({"zrank", "y", "c"}), ArgType(RespExpr::NIL));
}

TEST_F(ZSetFamilyTest, LargeSet) {
  for (int i = 0; i < 129; ++i) {
    auto resp = Run({"zadd", "key", absl::StrCat(i), absl::StrCat("element:", i)});
    EXPECT_THAT(resp, IntArg(1)) << i;
  }
  Run({"zadd", "key", "129", ""});

  EXPECT_THAT(Run({"zrangebyscore", "key", "(-inf", "(0.0"}), ArrLen(0));
  EXPECT_THAT(Run({"zrangebyscore", "key", "(5", "0.0"}), ArrLen(0));
  EXPECT_THAT(Run({"zrangebylex", "key", "-", "(element:0"}), ArrLen(0));
  EXPECT_EQ(2, CheckedInt({"zremrangebyscore", "key", "127", "(129"}));
}

TEST_F(ZSetFamilyTest, ZRemRangeRank) {
  Run({"zadd", "x", "1.1", "a", "2.1", "b"});
  EXPECT_THAT(Run({"ZREMRANGEBYRANK", "y", "0", "1"}), IntArg(0));
  EXPECT_THAT(Run({"ZREMRANGEBYRANK", "x", "0", "0"}), IntArg(1));
  EXPECT_EQ(Run({"zrange", "x", "0", "5"}), "b");
  EXPECT_THAT(Run({"ZREMRANGEBYRANK", "x", "0", "1"}), IntArg(1));
  EXPECT_EQ(Run({"type", "x"}), "none");
}

TEST_F(ZSetFamilyTest, ZRemRangeScore) {
  Run({"zadd", "x", "1.1", "a", "2.1", "b"});
  EXPECT_THAT(Run({"ZREMRANGEBYSCORE", "y", "0", "1"}), IntArg(0));
  EXPECT_THAT(Run({"ZREMRANGEBYSCORE", "x", "-inf", "1.1"}), IntArg(1));
  EXPECT_EQ(Run({"zrange", "x", "0", "5"}), "b");
  EXPECT_THAT(Run({"ZREMRANGEBYSCORE", "x", "(2.0", "+inf"}), IntArg(1));
  EXPECT_EQ(Run({"type", "x"}), "none");
  EXPECT_THAT(Run({"zremrangebyscore", "x", "1", "NaN"}), ErrArg("min or max is not a float"));
}

TEST_F(ZSetFamilyTest, IncrBy) {
  auto resp = Run({"zadd", "key", "xx", "incr", "2.1", "member"});
  EXPECT_THAT(resp, ArgType(RespExpr::NIL));

  resp = Run({"zadd", "key", "nx", "incr", "2.1", "member"});
  EXPECT_THAT(resp, "2.1");

  resp = Run({"zadd", "key", "nx", "incr", "4.9", "member"});
  EXPECT_THAT(resp, ArgType(RespExpr::NIL));
}

TEST_F(ZSetFamilyTest, ByLex) {
  Run({
      "zadd", "key",      "0", "alpha", "0", "bar",   "0", "cool", "0", "down",
      "0",    "elephant", "0", "foo",   "0", "great", "0", "hill", "0", "omega",
  });

  auto resp = Run({"zrangebylex", "key", "-", "[cool"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(resp.GetVec(), ElementsAre("alpha", "bar", "cool"));

  EXPECT_EQ(3, CheckedInt({"ZLEXCOUNT", "key", "(foo", "+"}));
  EXPECT_EQ(3, CheckedInt({"ZREMRANGEBYLEX", "key", "(foo", "+"}));

  resp = Run({"zrangebylex", "key", "[a", "+"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  ASSERT_THAT(resp.GetVec(), ElementsAre("alpha", "bar", "cool", "down", "elephant", "foo"));
}

TEST_F(ZSetFamilyTest, ZRevRangeByLex) {
  Run({
      "zadd", "key",      "0", "alpha", "0", "bar",   "0", "cool", "0", "down",
      "0",    "elephant", "0", "foo",   "0", "great", "0", "hill", "0", "omega",
  });

  auto resp = Run({"zrevrangebylex", "key", "[cool", "-"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(resp.GetVec(), ElementsAre("cool", "bar", "alpha"));

  EXPECT_EQ(3, CheckedInt({"ZLEXCOUNT", "key", "(foo", "+"}));
  EXPECT_EQ(3, CheckedInt({"ZREMRANGEBYLEX", "key", "(foo", "+"}));

  resp = Run({"zrevrangebylex", "key", "+", "[a"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  ASSERT_THAT(resp.GetVec(), ElementsAre("foo", "elephant", "down", "cool", "bar", "alpha"));
}

TEST_F(ZSetFamilyTest, ZRange) {
  Run({"zadd", "key", "0", "a", "1", "d", "1", "b", "2", "c", "4", "e"});

  auto resp = Run({"zrange", "key", "0", "2"});
  ASSERT_THAT(resp, ArrLen(3));
  ASSERT_THAT(resp.GetVec(), ElementsAre("a", "b", "d"));

  resp = Run({"zrange", "key", "1", "3", "WITHSCORES"});
  ASSERT_THAT(resp, ArrLen(6));
  ASSERT_THAT(resp.GetVec(), ElementsAre("b", "1", "d", "1", "c", "2"));

  resp = Run({"zrange", "key", "1", "3", "WITHSCORES", "REV"});
  ASSERT_THAT(resp, ArrLen(6));
  ASSERT_THAT(resp.GetVec(), ElementsAre("c", "2", "d", "1", "b", "1"));

  resp = Run({"zrange", "key", "(1", "4", "BYSCORE", "WITHSCORES"});
  ASSERT_THAT(resp, ArrLen(4));
  ASSERT_THAT(resp.GetVec(), ElementsAre("c", "2", "e", "4"));

  resp = Run({"zrange", "key", "-", "d", "BYLEX", "BYSCORE"});
  EXPECT_THAT(resp, ErrArg("BYSCORE and BYLEX options are not compatible"));

  resp = Run({"zrange", "key", "0", "-1", "LIMIT", "3", "-1"});
  ASSERT_THAT(resp, ArrLen(2));
  ASSERT_THAT(resp.GetVec(), ElementsAre("c", "e"));

  Run({"zremrangebyscore", "key", "0", "4"});

  Run({
      "zadd", "key",      "0", "alpha", "0", "bar",   "0", "cool", "0", "down",
      "0",    "elephant", "0", "foo",   "0", "great", "0", "hill", "0", "omega",
  });
  resp = Run({"zrange", "key", "-", "[cool", "BYLEX"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(resp.GetVec(), ElementsAre("alpha", "bar", "cool"));

  resp = Run({"zrange", "key", "[cool", "-", "REV", "BYLEX"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(resp.GetVec(), ElementsAre("cool", "bar", "alpha"));

  resp = Run({"zrange", "key", "+", "[cool", "REV", "BYLEX", "LIMIT", "2", "2"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(resp.GetVec(), ElementsAre("great", "foo"));

  resp = Run({"zrange", "key", "+", "[cool", "BYLEX", "LIMIT", "2", "2", "REV"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(resp.GetVec(), ElementsAre("great", "foo"));
}

TEST_F(ZSetFamilyTest, ZRevRange) {
  Run({"zadd", "key", "-inf", "a", "1", "b", "2", "c"});
  auto resp = Run({"zrevrangebyscore", "key", "2", "-inf"});
  ASSERT_THAT(resp, ArrLen(3));
  EXPECT_THAT(resp.GetVec(), ElementsAre("c", "b", "a"));

  resp = Run({"zrevrangebyscore", "key", "2", "-inf", "withscores"});
  ASSERT_THAT(resp, ArrLen(6));
  EXPECT_THAT(resp.GetVec(), ElementsAre("c", "2", "b", "1", "a", "-inf"));

  resp = Run({"zrevrange", "key", "0", "2"});
  ASSERT_THAT(resp, ArrLen(3));
  EXPECT_THAT(resp.GetVec(), ElementsAre("c", "b", "a"));

  resp = Run({"zrevrange", "key", "1", "2", "withscores"});
  ASSERT_THAT(resp, ArrLen(4));
  EXPECT_THAT(resp.GetVec(), ElementsAre("b", "1", "a", "-inf"));

  // Make sure that when using with upper case it works as well (see
  // https://github.com/dragonflydb/dragonfly/issues/326)
  resp = Run({"zrevrangebyscore", "key", "2", "-INF"});
  ASSERT_THAT(resp, ArrLen(3));
  EXPECT_THAT(resp.GetVec(), ElementsAre("c", "b", "a"));

  resp = Run({"zrevrangebyscore", "key", "2", "-INF", "withscores"});
  ASSERT_THAT(resp, ArrLen(6));
  EXPECT_THAT(resp.GetVec(), ElementsAre("c", "2", "b", "1", "a", "-inf"));
}

TEST_F(ZSetFamilyTest, ZScan) {
  string prefix(128, 'a');
  for (unsigned i = 0; i < 100; ++i) {
    Run({"zadd", "key", "1", absl::StrCat(prefix, i)});
  }

  EXPECT_EQ(100, CheckedInt({"zcard", "key"}));
  int64_t cursor = 0;
  size_t scan_len = 0;
  do {
    auto resp = Run({"zscan", "key", absl::StrCat(cursor)});
    ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
    ASSERT_THAT(resp.GetVec(), ElementsAre(ArgType(RespExpr::STRING), ArgType(RespExpr::ARRAY)));
    string_view token = ToSV(resp.GetVec()[0].GetBuf());
    ASSERT_TRUE(absl::SimpleAtoi(token, &cursor));
    auto sub_arr = resp.GetVec()[1].GetVec();
    scan_len += sub_arr.size();
  } while (cursor != 0);

  EXPECT_EQ(100 * 2, scan_len);

  // Check scan with count and match params
  scan_len = 0;
  do {
    auto resp = Run({"zscan", "key", absl::StrCat(cursor), "count", "5", "match", "*0"});
    ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
    ASSERT_THAT(resp.GetVec(), ElementsAre(ArgType(RespExpr::STRING), ArgType(RespExpr::ARRAY)));
    string_view token = ToSV(resp.GetVec()[0].GetBuf());
    ASSERT_TRUE(absl::SimpleAtoi(token, &cursor));
    auto sub_arr = resp.GetVec()[1].GetVec();
    scan_len += sub_arr.size();
  } while (cursor != 0);
  EXPECT_EQ(10 * 2, scan_len);  // expected members a0,a10,a20..,a90
}

TEST_F(ZSetFamilyTest, ZUnionError) {
  RespExpr resp;

  resp = Run({"zunion", "0"});
  EXPECT_THAT(resp, ErrArg("wrong number of arguments"));

  resp = Run({"zunion", "0", "myset"});
  EXPECT_THAT(resp, ErrArg("syntax error"));

  resp = Run({"zunion", "3", "z1", "z2", "z3", "weights", "1", "1", "k"});
  EXPECT_THAT(resp, ErrArg("weight value is not a float"));

  resp = Run({"zunion", "3", "z1", "z2", "z3", "weights", "1", "1", "2", "aggregate", "something"});
  EXPECT_THAT(resp, ErrArg("syntax error"));

  resp = Run({"zunion", "3", "z1", "z2", "z3", "weights", "1", "2", "aggregate", "something"});
  EXPECT_THAT(resp, ErrArg("weight value is not a float"));

  resp = Run({"zunion", "3", "z1", "z2", "z3", "aggregate", "sum", "somescore"});
  EXPECT_THAT(resp, ErrArg("syntax error"));

  resp = Run({"zunion", "3", "z1", "z2", "z3", "withscores", "someargs"});
  EXPECT_THAT(resp, ErrArg("syntax error"));

  resp = Run({"zunion", "1"});
  EXPECT_THAT(resp, ErrArg("wrong number of arguments"));

  resp = Run({"zunion", "2", "z1"});
  EXPECT_THAT(resp, ErrArg("syntax error"));

  resp = Run({"zunion", "2", "z1", "z2", "z3"});
  EXPECT_THAT(resp, ErrArg("syntax error"));

  resp = Run({"zunion", "2", "z1", "z2", "weights", "1", "2", "3"});
  EXPECT_THAT(resp, ErrArg("syntax error"));
}

TEST_F(ZSetFamilyTest, ZUnion) {
  RespExpr resp;

  EXPECT_EQ(2, CheckedInt({"zadd", "z1", "1", "a", "3", "b"}));
  EXPECT_EQ(2, CheckedInt({"zadd", "z2", "3", "c", "2", "b"}));
  EXPECT_EQ(2, CheckedInt({"zadd", "z3", "1", "c", "1", "d"}));

  resp = Run({"zunion", "3", "z1", "z2", "z3"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("a", "d", "c", "b"));

  resp = Run({"zunion", "3", "z1", "z2", "z3", "weights", "1", "1", "2"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("a", "d", "b", "c"));

  resp = Run({"zunion", "3", "z1", "z2", "z3", "weights", "1", "1", "2", "withscores"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("a", "1", "d", "2", "b", "5", "c", "5"));

  resp = Run({"zunion", "3", "z1", "z2", "z3", "weights", "1", "1", "2", "aggregate", "min",
              "withscores"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("a", "1", "b", "2", "c", "2", "d", "2"));

  resp = Run({"zunion", "3", "z1", "z2", "z3", "withscores", "weights", "1", "1", "2", "aggregate",
              "min"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("a", "1", "b", "2", "c", "2", "d", "2"));

  resp = Run({"zunion", "3", "none1", "none2", "z3", "withscores", "weights", "1", "1", "2"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("c", "2", "d", "2"));

  resp = Run({"zunion", "3", "z1", "z2", "z3", "weights", "1", "1", "2", "aggregate", "max",
              "withscores"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("a", "1", "d", "2", "b", "3", "c", "3"));

  resp = Run({"zunion", "1", "z1", "weights", "2", "aggregate", "max", "withscores"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("a", "2", "b", "6"));

  for (unsigned i = 0; i < 256; ++i) {
    Run({"zadd", "large1", "1000", absl::StrCat("aaaaaaaaaa", i)});
    Run({"zadd", "large2", "1000", absl::StrCat("bbbbbbbbbb", i)});
    Run({"zadd", "large2", "1000", absl::StrCat("aaaaaaaaaa", i)});
  }
  resp = Run({"zunion", "2", "large2", "large1"});
  EXPECT_THAT(resp, ArrLen(512));
}

TEST_F(ZSetFamilyTest, ZUnionStore) {
  RespExpr resp;

  resp = Run({"zunionstore", "key", "0"});
  EXPECT_THAT(resp, ErrArg("wrong number of arguments"));

  resp = Run({"zunionstore", "key", "0", "aggregate"});
  EXPECT_THAT(resp, ErrArg("syntax error"));

  resp = Run({"zunionstore", "key", "0", "aggregate", "sum"});
  EXPECT_THAT(resp, ErrArg("at least 1 input key is needed"));
  resp = Run({"zunionstore", "key", "-1", "aggregate", "sum"});
  EXPECT_THAT(resp, ErrArg("out of range"));
  resp = Run({"zunionstore", "key", "2", "foo", "bar", "weights", "1"});
  EXPECT_THAT(resp, ErrArg("syntax error"));

  EXPECT_EQ(2, CheckedInt({"zadd", "z1", "1", "a", "2", "b"}));
  EXPECT_EQ(2, CheckedInt({"zadd", "z2", "3", "c", "2", "b"}));

  resp = Run({"zunionstore", "key", "2", "z1", "z2"});
  EXPECT_THAT(resp, IntArg(3));
  resp = Run({"zrange", "key", "0", "-1", "withscores"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("a", "1", "c", "3", "b", "4"));

  resp = Run({"zunionstore", "z1", "1", "z1"});
  EXPECT_THAT(resp, IntArg(2));

  resp = Run({"zunionstore", "z1", "2", "z1", "z2"});
  EXPECT_THAT(resp, IntArg(3));
  resp = Run({"zrange", "z1", "0", "-1", "withscores"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("a", "1", "c", "3", "b", "4"));

  Run({"set", "foo", "bar"});
  resp = Run({"zunionstore", "foo", "1", "z2"});
  EXPECT_THAT(resp, IntArg(2));
  resp = Run({"zrange", "foo", "0", "-1", "withscores"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("b", "2", "c", "3"));
}

TEST_F(ZSetFamilyTest, ZUnionStoreOpts) {
  EXPECT_EQ(2, CheckedInt({"zadd", "z1", "1", "a", "2", "b"}));
  EXPECT_EQ(2, CheckedInt({"zadd", "z2", "3", "c", "2", "b"}));
  RespExpr resp;

  EXPECT_EQ(3, CheckedInt({"zunionstore", "a", "2", "z1", "z2", "weights", "1", "3"}));
  resp = Run({"zrange", "a", "0", "-1", "withscores"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("a", "1", "b", "8", "c", "9"));

  resp = Run({"zunionstore", "a", "2", "z1", "z2", "weights", "1"});
  EXPECT_THAT(resp, ErrArg("syntax error"));

  resp = Run({"zunionstore", "z1", "1", "z1", "weights", "2"});
  EXPECT_THAT(resp, IntArg(2));
  resp = Run({"zrange", "z1", "0", "-1", "withscores"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("a", "2", "b", "4"));

  resp = Run({"zunionstore", "max", "2", "z1", "z2", "weights", "1", "0", "aggregate", "max"});
  ASSERT_THAT(resp, IntArg(3));
  resp = Run({"zrange", "max", "0", "-1", "withscores"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("c", "0", "a", "2", "b", "4"));
}

TEST_F(ZSetFamilyTest, ZInterStore) {
  EXPECT_EQ(2, CheckedInt({"zadd", "z1", "1", "a", "2", "b"}));
  EXPECT_EQ(2, CheckedInt({"zadd", "z2", "3", "c", "2", "b"}));
  RespExpr resp;

  EXPECT_EQ(1, CheckedInt({"zinterstore", "a", "2", "z1", "z2"}));
  resp = Run({"zrange", "a", "0", "-1", "withscores"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("b", "4"));

  // support for sets
  EXPECT_EQ(2, CheckedInt({"sadd", "s2", "b", "c"}));
  EXPECT_EQ(1, CheckedInt({"zinterstore", "b", "2", "z1", "s2"}));
  resp = Run({"zrange", "b", "0", "-1", "withscores"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("b", "3"));
}

TEST_F(ZSetFamilyTest, ZInterCard) {
  EXPECT_EQ(3, CheckedInt({"zadd", "z1", "1", "a", "2", "b", "3", "c"}));
  EXPECT_EQ(3, CheckedInt({"zadd", "z2", "2", "b", "3", "c", "4", "d"}));
  RespExpr resp;

  EXPECT_EQ(2, CheckedInt({"zintercard", "2", "z1", "z2"}));
  EXPECT_EQ(1, CheckedInt({"zintercard", "2", "z1", "z2", "LIMIT", "1"}));

  resp = Run({"zintercard", "2", "z1", "z2", "LIM"});
  EXPECT_THAT(resp, ErrArg("syntax error"));
  resp = Run({"zintercard", "2", "z1", "z2", "LIMIT"});
  EXPECT_THAT(resp, ErrArg("syntax error"));
  resp = Run({"zintercard", "2", "z1", "z2", "LIMIT", "a"});
  EXPECT_THAT(resp, ErrArg("limit value is not a positive integer"));

  // support for sets
  EXPECT_EQ(3, CheckedInt({"sadd", "s2", "b", "c", "d"}));
  EXPECT_EQ(2, CheckedInt({"zintercard", "2", "z1", "s2"}));
}

TEST_F(ZSetFamilyTest, ZAddBug148) {
  auto resp = Run({"zadd", "key", "1", "9fe9f1eb"});
  EXPECT_THAT(resp, IntArg(1));
}

TEST_F(ZSetFamilyTest, ZPopMin) {
  auto resp = Run({"zadd", "key", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e", "6", "f"});
  EXPECT_THAT(resp, IntArg(6));

  resp = Run({"zpopmin", "key"});
  ASSERT_THAT(resp, ArrLen(2));
  EXPECT_THAT(resp.GetVec(), ElementsAre("a", "1"));

  resp = Run({"zpopmin", "key", "2"});
  ASSERT_THAT(resp, ArrLen(4));
  EXPECT_THAT(resp.GetVec(), ElementsAre("b", "2", "c", "3"));

  resp = Run({"zpopmin", "key", "-1"});
  ASSERT_THAT(resp, ErrArg("value is out of range, must be positive"));

  resp = Run({"zpopmin", "key", "1"});
  ASSERT_THAT(resp, ArrLen(2));
  EXPECT_THAT(resp.GetVec(), ElementsAre("d", "4"));

  resp = Run({"zpopmin", "key", "3"});
  ASSERT_THAT(resp, ArrLen(4));
  EXPECT_THAT(resp.GetVec(), ElementsAre("e", "5", "f", "6"));

  resp = Run({"zpopmin", "key", "1"});
  ASSERT_THAT(resp, ArrLen(0));
}

TEST_F(ZSetFamilyTest, ZPopMax) {
  auto resp = Run({"zadd", "key", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e", "6", "f"});
  EXPECT_THAT(resp, IntArg(6));

  resp = Run({"zpopmax", "key"});
  ASSERT_THAT(resp, ArrLen(2));
  EXPECT_THAT(resp.GetVec(), ElementsAre("f", "6"));

  resp = Run({"zpopmax", "key", "2"});
  ASSERT_THAT(resp, ArrLen(4));
  EXPECT_THAT(resp.GetVec(), ElementsAre("e", "5", "d", "4"));

  resp = Run({"zpopmax", "key", "-1"});
  ASSERT_THAT(resp, ErrArg("value is out of range, must be positive"));

  resp = Run({"zpopmax", "key", "1"});
  ASSERT_THAT(resp, ArrLen(2));
  EXPECT_THAT(resp.GetVec(), ElementsAre("c", "3"));

  resp = Run({"zpopmax", "key", "3"});
  ASSERT_THAT(resp, ArrLen(4));
  EXPECT_THAT(resp.GetVec(), ElementsAre("b", "2", "a", "1"));

  resp = Run({"zpopmax", "key", "1"});
  ASSERT_THAT(resp, ArrLen(0));
}

TEST_F(ZSetFamilyTest, ZAddPopCrash) {
  for (int i = 0; i < 129; ++i) {
    auto resp = Run({"zadd", "key", absl::StrCat(i), absl::StrCat("element:", i)});
    EXPECT_THAT(resp, IntArg(1)) << i;
  }

  auto resp = Run({"zpopmin", "key"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("element:0", "0"));
}

TEST_F(ZSetFamilyTest, Resp3) {
  Run({"hello", "3"});
  Run({"zadd", "x", "1", "a", "2", "b"});
  auto resp = Run({"zrange", "x", "0", "-1", "WITHSCORES"});
  ASSERT_THAT(resp, ArrLen(2));
  ASSERT_THAT(resp.GetVec()[0].GetVec(), ElementsAre("a", DoubleArg(1)));
  ASSERT_THAT(resp.GetVec()[1].GetVec(), ElementsAre("b", DoubleArg(2)));
}

TEST_F(ZSetFamilyTest, BlockingIsReleased) {
  // Inputs for ZSET store commands.
  Run({"ZADD", "A", "1", "x", "2", "b"});
  Run({"ZADD", "B", "1", "x", "3", "b"});
  Run({"ZADD", "C", "1", "x", "10", "a"});
  Run({"ZADD", "D", "1", "x", "5", "c"});

  vector<string> blocking_keys{"zset1", "zset2", "zset3"};
  for (const auto& key : blocking_keys) {
    vector<vector<string>> unblocking_commands;
    // All commands output the same set {2,x}.
    unblocking_commands.push_back({"ZADD", key, "2", "x", "10", "y"});
    unblocking_commands.push_back({"ZINCRBY", key, "2", "x"});
    unblocking_commands.push_back({"ZINTERSTORE", key, "2", "A", "B"});
    unblocking_commands.push_back({"ZUNIONSTORE", key, "2", "C", "D"});
    // unblocking_commands.push_back({"ZDIFFSTORE", key, "2", "A", "B"}); // unimplemented

    for (auto& cmd : unblocking_commands) {
      RespExpr resp0;
      auto fb0 = pp_->at(0)->LaunchFiber(Launch::dispatch, [&] {
        resp0 = Run({"BZPOPMIN", "zset1", "zset2", "zset3", "0"});
        LOG(INFO) << "BZPOPMIN";
      });

      pp_->at(1)->Await([&] { return Run({cmd.data(), cmd.size()}); });
      fb0.Join();

      ASSERT_THAT(resp0, ArrLen(3)) << cmd[0];
      EXPECT_THAT(resp0.GetVec(), ElementsAre(key, "x", "2")) << cmd[0];

      Run({"DEL", key});
    }
  }
}

TEST_F(ZSetFamilyTest, BlockingTimeout) {
  RespExpr resp0;

  auto start = absl::Now();
  auto fb0 = pp_->at(0)->LaunchFiber(Launch::dispatch, [&] {
    resp0 = Run({"BZPOPMIN", "zset1", "1"});
    LOG(INFO) << "BZPOPMIN";
  });
  fb0.Join();
  auto dur = absl::Now() - start;

  // Check that the timeout duration is not too crazy.
  EXPECT_LT(AbsDuration(dur - absl::Milliseconds(1000)), absl::Milliseconds(300));
  EXPECT_THAT(resp0, ArgType(RespExpr::NIL_ARRAY));
}

TEST_F(ZSetFamilyTest, ZDiffError) {
  RespExpr resp;

  resp = Run({"zdiff", "-1", "z1"});
  EXPECT_THAT(resp, ErrArg("value is not an integer or out of range"));

  resp = Run({"zdiff", "0"});
  EXPECT_THAT(resp, ErrArg("wrong number of arguments"));

  resp = Run({"zdiff", "0", "z1"});
  EXPECT_THAT(resp, ErrArg("value is not an integer or out of range"));

  resp = Run({"zdiff", "0", "z1", "z2"});
  EXPECT_THAT(resp, ErrArg("value is not an integer or out of range"));
}

TEST_F(ZSetFamilyTest, ZDiff) {
  RespExpr resp;

  EXPECT_EQ(4, CheckedInt({"zadd", "z1", "1", "one", "2", "two", "3", "three", "4", "four"}));
  EXPECT_EQ(2, CheckedInt({"zadd", "z2", "1", "one", "5", "five"}));
  EXPECT_EQ(2, CheckedInt({"zadd", "z3", "2", "two", "3", "three"}));
  EXPECT_EQ(1, CheckedInt({"zadd", "z4", "4", "four"}));

  resp = Run({"zdiff", "1", "z1"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("one", "two", "three", "four"));

  resp = Run({"zdiff", "2", "z1", "z1"});
  EXPECT_THAT(resp.GetVec().empty(), true);

  resp = Run({"zdiff", "2", "z1", "doesnt_exist"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("one", "two", "three", "four"));

  resp = Run({"zdiff", "2", "z1", "z2"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("two", "three", "four"));

  resp = Run({"zdiff", "2", "z1", "z3"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("one", "four"));

  resp = Run({"zdiff", "4", "z1", "z2", "z3", "z4"});
  EXPECT_THAT(resp.GetVec().empty(), true);

  resp = Run({"zdiff", "2", "doesnt_exist", "key1"});
  EXPECT_THAT(resp.GetVec().empty(), true);

  // WITHSCORES
  resp = Run({"zdiff", "1", "z1", "WITHSCORES"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("one", "1", "two", "2", "three", "3", "four", "4"));

  resp = Run({"zdiff", "2", "z1", "z2", "WITHSCORES"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("two", "2", "three", "3", "four", "4"));
}

TEST_F(ZSetFamilyTest, Count) {
  for (int i = 0; i < 129; ++i) {
    auto resp = Run({"zadd", "key", absl::StrCat(i), absl::StrCat("element:", i)});
    EXPECT_THAT(resp, IntArg(1)) << i;
  }

  EXPECT_THAT(CheckedInt({"zcount", "key", "-inf", "+inf"}), 129);
  EXPECT_THAT(CheckedInt({"zlexcount", "key", "-", "+"}), 129);
}

TEST_F(ZSetFamilyTest, GeoAdd) {
  EXPECT_EQ(2, CheckedInt({"geoadd", "Sicily", "13.361389", "38.115556", "Palermo", "15.087269",
                           "37.502669", "Catania"}));
  EXPECT_EQ(0, CheckedInt({"geoadd", "Sicily", "13.361389", "38.115556", "Palermo", "15.087269",
                           "37.502669", "Catania"}));
  auto resp = Run({"geohash", "Sicily", "Palermo", "Catania"});
  EXPECT_THAT(resp, RespArray(ElementsAre("sqc8b49rny0", "sqdtr74hyu0")));
}

TEST_F(ZSetFamilyTest, GeoAddOptions) {
  EXPECT_EQ(2, CheckedInt({"geoadd", "Sicily", "13.361389", "38.115556", "Palermo", "15.087269",
                           "37.502669", "Catania"}));

  // add 1 + update 1 + XX
  EXPECT_EQ(0, CheckedInt({"geoadd", "Sicily", "XX", "15.361389", "38.115556", "Palermo",
                           "15.554167", "38.193611", "Messina"}));
  auto resp = Run({"geopos", "Sicily", "Palermo", "Messina"});
  EXPECT_THAT(
      resp, RespArray(ElementsAre(RespArray(ElementsAre("15.361389219760895", "38.1155563954963")),
                                  ArgType(RespExpr::NIL))));

  // add 1 + update 1 + NX
  EXPECT_EQ(1, CheckedInt({"geoadd", "Sicily", "NX", "18.361389", "38.115556", "Palermo", "15.2875",
                           "37.069167", "Syracuse"}));
  resp = Run({"geopos", "Sicily", "Palermo", "Syracuse"});
  EXPECT_THAT(resp, RespArray(ElementsAre(
                        RespArray(ElementsAre("15.361389219760895", "38.1155563954963")),
                        RespArray(ElementsAre("15.287499725818634", "37.06916773705567")))));

  // add 1 + update 1 CH
  EXPECT_EQ(2, CheckedInt({"geoadd", "Sicily", "CH", "18.361389", "38.115556", "Palermo",
                           "12.434167", "37.798056", "Marsala"}));
  resp = Run({"geopos", "Sicily", "Palermo", "Marsala"});
  EXPECT_THAT(resp, RespArray(ElementsAre(
                        RespArray(ElementsAre("18.361386358737946", "38.1155563954963")),
                        RespArray(ElementsAre("12.43416577577591", "37.7980572230775")))));

  // update 1 + CH + XX
  EXPECT_EQ(1, CheckedInt({"geoadd", "Sicily", "CH", "XX", "10.361389", "38.115556", "Palermo"}));
  resp = Run({"geopos", "Sicily", "Palermo"});
  EXPECT_THAT(resp, RespArray(ElementsAre("10.361386835575104", "38.1155563954963")));

  // add 1 + CH + NX
  EXPECT_EQ(1, CheckedInt({"geoadd", "Sicily", "CH", "NX", "14.25", "37.066667", "Gela"}));
  resp = Run({"geopos", "Sicily", "Gela"});
  EXPECT_THAT(resp, RespArray(ElementsAre("14.249999821186066", "37.06666596727141")));

  // add 1 + XX + NX
  resp = Run({"geoadd", "Sicily", "XX", "NX", "14.75", "36.933333", "Ragusa"});
  EXPECT_THAT(resp, ErrArg("XX and NX options at the same time are not compatible"));

  // incorrect number of args
  resp = Run({"geoadd", "Sicily", "14.75", "36.933333", "Ragusa", "10.23"});
  EXPECT_THAT(resp, ErrArg("syntax error"));
}

TEST_F(ZSetFamilyTest, GeoPos) {
  EXPECT_EQ(1, CheckedInt({"geoadd", "Sicily", "13.361389", "38.115556", "Palermo"}));
  auto resp = Run({"geopos", "Sicily", "Palermo", "NonExisting"});
  EXPECT_THAT(
      resp, RespArray(ElementsAre(RespArray(ElementsAre("13.361389338970184", "38.1155563954963")),
                                  ArgType(RespExpr::NIL))));
}

TEST_F(ZSetFamilyTest, GeoPosWrongType) {
  Run({"set", "x", "value"});
  EXPECT_THAT(Run({"geopos", "x", "Sicily", "Palermo"}), ErrArg("WRONGTYPE"));
}

TEST_F(ZSetFamilyTest, GeoDist) {
  EXPECT_EQ(2, CheckedInt({"geoadd", "Sicily", "13.361389", "38.115556", "Palermo", "15.087269",
                           "37.502669", "Catania"}));
  auto resp = Run({"geodist", "Sicily", "Palermo", "Catania"});
  EXPECT_EQ(resp, "166274.15156960033");

  resp = Run({"geodist", "Sicily", "Palermo", "Catania", "km"});
  EXPECT_EQ(resp, "166.27415156960032");

  resp = Run({"geodist", "Sicily", "Palermo", "Catania", "MI"});
  EXPECT_EQ(resp, "103.31822459492733");

  resp = Run({"geodist", "Sicily", "Palermo", "Catania", "FT"});
  EXPECT_EQ(resp, "545518.8699790037");

  resp = Run({"geodist", "Sicily", "Foo", "Bar"});
  EXPECT_THAT(resp, ArgType(RespExpr::NIL));
}

}  // namespace dfly
