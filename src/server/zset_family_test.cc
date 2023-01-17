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
}

TEST_F(ZSetFamilyTest, ZUnion) {
  RespExpr resp;

  resp = Run({"zunion", "0"});
  EXPECT_THAT(resp, ErrArg("wrong number of arguments"));

  EXPECT_EQ(2, CheckedInt({"zadd", "z1", "1", "a", "3", "b"}));
  EXPECT_EQ(2, CheckedInt({"zadd", "z2", "3", "c", "2", "b"}));
  EXPECT_EQ(2, CheckedInt({"zadd", "z3", "1", "c", "1", "d"}));

  resp = Run({"zunion", "z1", "z2", "z3", "weights", "1", "1", "k"});
  EXPECT_THAT(resp, ErrArg("weight value is not a float"));

  resp = Run({"zunion", "z1", "z2", "z3", "weights", "1", "1", "2", "aggregate", "something"});
  EXPECT_THAT(resp, ErrArg("syntax error"));

  resp = Run({"zunion", "z1", "z2", "z3", "weights", "1", "2", "aggregate", "something"});
  EXPECT_THAT(resp, ErrArg("weight value is not a float"));

  resp = Run({"zunion", "z1", "z2", "z3", "aggregate", "sum", "somescore"});
  EXPECT_THAT(resp, ErrArg("syntax error"));

  resp = Run({"zunion", "z1", "z2", "z3", "withscores", "someargs"});
  EXPECT_THAT(resp, ErrArg("syntax error"));

  resp = Run({"zunion", "z1", "z2", "z3"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("a", "d", "c", "b"));

  resp = Run({"zunion", "z1", "z2", "z3", "weights", "1", "1", "2"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("a", "d", "b", "c"));

  resp = Run({"zunion", "z1", "z2", "z3", "weights", "1", "1", "2", "withscores"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("a", "1", "d", "2", "b", "5", "c", "5"));

  resp =
      Run({"zunion", "z1", "z2", "z3", "weights", "1", "1", "2", "aggregate", "min", "withscores"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("a", "1", "b", "2", "c", "2", "d", "2"));

  resp =
      Run({"zunion", "z1", "z2", "z3", "withscores", "weights", "1", "1", "2", "aggregate", "min"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("a", "1", "b", "2", "c", "2", "d", "2"));

  resp = Run({"zunion", "none1", "none2", "z3", "withscores", "weights", "1", "1", "2"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("c", "2", "d", "2"));

  resp =
      Run({"zunion", "z1", "z2", "z3", "weights", "1", "1", "2", "aggregate", "max", "withscores"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("a", "1", "d", "2", "b", "3", "c", "3"));
}

TEST_F(ZSetFamilyTest, ZUnionStore) {
  RespExpr resp;

  resp = Run({"zunionstore", "key", "0"});
  EXPECT_THAT(resp, ErrArg("wrong number of arguments"));

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
}

TEST_F(ZSetFamilyTest, ZAddBug148) {
  auto resp = Run({"zadd", "key", "1", "9fe9f1eb"});
  EXPECT_THAT(resp, IntArg(1));
}

TEST_F(ZSetFamilyTest, ZPopMin) {
  auto resp = Run({"zadd", "key", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e"});
  EXPECT_THAT(resp, IntArg(5));

  resp = Run({"zpopmin", "key", "2"});
  ASSERT_THAT(resp, ArrLen(2));
  EXPECT_THAT(resp.GetVec(), ElementsAre("a", "b"));

  resp = Run({"zpopmin", "key", "-1"});
  ASSERT_THAT(resp, ErrArg("value is out of range, must be positive"));

  resp = Run({"zpopmin", "key", "1"});
  ASSERT_THAT(resp, "c");

  resp = Run({"zpopmin", "key", "3"});
  ASSERT_THAT(resp, ArrLen(2));
  EXPECT_THAT(resp.GetVec(), ElementsAre("d", "e"));

  resp = Run({"zpopmin", "key", "1"});
  ASSERT_THAT(resp, ArrLen(0));
}

TEST_F(ZSetFamilyTest, ZPopMax) {
  auto resp = Run({"zadd", "key", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e"});
  EXPECT_THAT(resp, IntArg(5));

  resp = Run({"zpopmax", "key", "2"});
  ASSERT_THAT(resp, ArrLen(2));
  EXPECT_THAT(resp.GetVec(), ElementsAre("e", "d"));

  resp = Run({"zpopmax", "key", "-1"});
  ASSERT_THAT(resp, ErrArg("value is out of range, must be positive"));

  resp = Run({"zpopmax", "key", "1"});
  ASSERT_THAT(resp, "c");

  resp = Run({"zpopmax", "key", "3"});
  ASSERT_THAT(resp, ArrLen(2));
  EXPECT_THAT(resp.GetVec(), ElementsAre("b", "a"));

  resp = Run({"zpopmax", "key", "1"});
  ASSERT_THAT(resp, ArrLen(0));
}
}  // namespace dfly
