// Copyright 2022, Roman Gershman.  All rights reserved.
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
using namespace boost;

namespace dfly {

class ZSetFamilyTest : public BaseFamilyTest {
 protected:
};

TEST_F(ZSetFamilyTest, Add) {
  auto resp = Run({"zadd", "x", "1.1", "a"});
  EXPECT_THAT(resp[0], IntArg(1));

  resp = Run({"zscore", "x", "a"});
  EXPECT_THAT(resp[0], StrArg("1.1"));

  resp = Run({"zadd", "x", "2", "a"});
  EXPECT_THAT(resp[0], IntArg(0));
  resp = Run({"zscore", "x", "a"});
  EXPECT_THAT(resp[0], StrArg("2"));

  resp = Run({"zadd", "x", "ch", "3", "a"});
  EXPECT_THAT(resp[0], IntArg(1));
  resp = Run({"zscore", "x", "a"});
  EXPECT_THAT(resp[0], StrArg("3"));

  resp = Run({"zcard", "x"});
  EXPECT_THAT(resp[0], IntArg(1));
}

TEST_F(ZSetFamilyTest, ZRem) {
  auto resp = Run({"zadd", "x", "1.1", "b", "2.1", "a"});
  EXPECT_THAT(resp[0], IntArg(2));

  resp = Run({"zrem", "x", "b", "c"});
  EXPECT_THAT(resp[0], IntArg(1));

  resp = Run({"zcard", "x"});
  EXPECT_THAT(resp[0], IntArg(1));
  EXPECT_THAT(Run({"zrange", "x", "0", "3", "byscore"}), ElementsAre("a"));
  EXPECT_THAT(Run({"zrange", "x", "(-inf", "(+inf", "byscore"}), ElementsAre("a"));
}

TEST_F(ZSetFamilyTest, ZRange) {
  Run({"zadd", "x", "1.1", "a", "2.1", "b"});
  EXPECT_THAT(Run({"zrangebyscore", "x", "0", "(1.1"}), ElementsAre(ArrLen(0)));
  EXPECT_THAT(Run({"zrangebyscore", "x", "-inf", "1.1"}), ElementsAre("a"));
}

TEST_F(ZSetFamilyTest, ZRemRangeRank) {
  Run({"zadd", "x", "1.1", "a", "2.1", "b"});
  EXPECT_THAT(Run({"ZREMRANGEBYRANK", "y", "0", "1"}), ElementsAre(IntArg(0)));
  EXPECT_THAT(Run({"ZREMRANGEBYRANK", "x", "0", "0"}), ElementsAre(IntArg(1)));
  EXPECT_THAT(Run({"zrange", "x", "0", "5"}), ElementsAre("b"));
  EXPECT_THAT(Run({"ZREMRANGEBYRANK", "x", "0", "1"}), ElementsAre(IntArg(1)));
  EXPECT_THAT(Run({"type", "x"}), ElementsAre("none"));
}

TEST_F(ZSetFamilyTest, ZRemRangeScore) {
  Run({"zadd", "x", "1.1", "a", "2.1", "b"});
  EXPECT_THAT(Run({"ZREMRANGEBYSCORE", "y", "0", "1"}), ElementsAre(IntArg(0)));
  EXPECT_THAT(Run({"ZREMRANGEBYSCORE", "x", "-inf", "1.1"}), ElementsAre(IntArg(1)));
  EXPECT_THAT(Run({"zrange", "x", "0", "5"}), ElementsAre("b"));
  EXPECT_THAT(Run({"ZREMRANGEBYSCORE", "x", "(2.0", "+inf"}), ElementsAre(IntArg(1)));
  EXPECT_THAT(Run({"type", "x"}), ElementsAre("none"));
}

TEST_F(ZSetFamilyTest, IncrBy) {
  auto resp = Run({"zadd", "key", "xx", "incr", "2.1", "member"});
  EXPECT_THAT(resp[0], ArgType(RespExpr::NIL));

  resp = Run({"zadd", "key", "nx", "incr", "2.1", "member"});
  EXPECT_THAT(resp[0], "2.1");

  resp = Run({"zadd", "key", "nx", "incr", "4.9", "member"});
  EXPECT_THAT(resp[0], ArgType(RespExpr::NIL));
}

}  // namespace dfly
