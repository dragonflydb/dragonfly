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

}  // namespace dfly
