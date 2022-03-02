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
}


}  // namespace dfly
