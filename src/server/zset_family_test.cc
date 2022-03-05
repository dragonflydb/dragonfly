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
  resp = Run({"zadd", "x", "2", "a"});
  EXPECT_THAT(resp[0], IntArg(0));
  resp = Run({"zadd", "x", "ch", "3", "a"});
  EXPECT_THAT(resp[0], IntArg(1));
}


}  // namespace dfly
