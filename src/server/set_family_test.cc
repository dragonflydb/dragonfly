// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/set_family.h"

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

class SetFamilyTest : public BaseFamilyTest {
 protected:
};

TEST_F(SetFamilyTest, SAdd) {
  auto resp = Run({"sadd", "x", "1", "2", "3"});
  EXPECT_THAT(resp[0], IntArg(3));
  resp = Run({"sadd", "x", "2", "3"});
  EXPECT_THAT(resp[0], IntArg(0));
  Run({"set", "a", "foo"});
  resp = Run({"sadd", "a", "b"});
  EXPECT_THAT(resp[0], ErrArg("WRONGTYPE "));
  resp = Run({"type", "x"});
  EXPECT_THAT(resp, RespEq("set"));
}

TEST_F(SetFamilyTest, IntConv) {
  auto resp = Run({"sadd", "x", "134"});
  EXPECT_THAT(resp[0], IntArg(1));
  resp = Run({"sadd", "x", "abc"});
  EXPECT_THAT(resp[0], IntArg(1));
  resp = Run({"sadd", "x", "134"});
  EXPECT_THAT(resp[0], IntArg(0));
}

TEST_F(SetFamilyTest, SUnionStore) {
  auto resp = Run({"sadd", "b", "1", "2", "3"});
  Run({"sadd", "c", "10", "11"});
  Run({"set", "a", "foo"});
  resp = Run({"sunionstore", "a", "b", "c"});
  EXPECT_THAT(resp[0], IntArg(5));
  resp = Run({"type", "a"});
  ASSERT_THAT(resp, RespEq("set"));

  resp = Run({"smembers", "a"});
  EXPECT_THAT(resp, UnorderedElementsAre("11", "10", "1", "2", "3"));
}

TEST_F(SetFamilyTest, SDiff) {
  auto resp = Run({"sadd", "b", "1", "2", "3"});
  Run({"sadd", "c", "10", "11"});
  Run({"set", "a", "foo"});
  resp = Run({"sdiff", "b", "c"});
  EXPECT_THAT(resp, UnorderedElementsAre("1", "2", "3"));
  resp = Run({"sdiffstore", "a", "b", "c"});
  EXPECT_THAT(resp[0], IntArg(3));

  Run({"sadd", "bar", "x", "a", "b", "c"});
  Run({"sadd", "foo", "c"});
  Run({"sadd", "car", "a", "d"});
  EXPECT_EQ(2, CheckedInt({"SDIFFSTORE", "tar", "bar", "foo", "car"}));
}

TEST_F(SetFamilyTest, SInter) {
  auto resp = Run({"sadd", "a", "1", "2", "3", "4"});
  Run({"sadd", "b", "3", "5", "6", "2"});
  resp = Run({"sinterstore", "d", "a", "b"});
  EXPECT_THAT(resp[0], IntArg(2));
  resp = Run({"smembers", "d"});
  EXPECT_THAT(resp, UnorderedElementsAre("3", "2"));
}

TEST_F(SetFamilyTest, SMove) {
  auto resp = Run({"sadd", "a", "1", "2", "3", "4"});
  Run({"sadd", "b", "3", "5", "6", "2"});
  resp = Run({"smove", "a", "b", "1"});
  EXPECT_THAT(resp[0], IntArg(1));

  Run({"sadd", "x", "a", "b", "c"});
  Run({"sadd", "y", "c"});
  EXPECT_THAT(Run({"smove", "x", "y", "c"}), ElementsAre(IntArg(1)));
}

TEST_F(SetFamilyTest, SPop) {
  auto resp = Run({"sadd", "x", "1", "2", "3"});
  resp = Run({"spop", "x", "3"});
  EXPECT_THAT(resp, UnorderedElementsAre("1", "2", "3"));
  resp = Run({"type", "x"});
  EXPECT_THAT(resp, RespEq("none"));

  Run({"sadd", "x", "1", "2", "3"});
  resp = Run({"spop", "x", "2"});
  EXPECT_THAT(resp, IsSubsetOf({"1", "2", "3"}));
  EXPECT_EQ(2, resp.size());

  resp = Run({"scard", "x"});
  EXPECT_THAT(resp[0], IntArg(1));

  Run({"sadd", "y", "a", "b", "c"});
  resp = Run({"spop", "y", "1"});
  EXPECT_THAT(resp, IsSubsetOf({"a", "b", "c"}));
  EXPECT_EQ(1, resp.size());

  resp = Run({"smembers", "y"});
  EXPECT_THAT(resp, IsSubsetOf({"a", "b", "c"}));
  EXPECT_EQ(2, resp.size());
}

}  // namespace dfly
