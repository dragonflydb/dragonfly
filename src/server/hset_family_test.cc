// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/hset_family.h"

#include "base/gtest.h"
#include "base/logging.h"
#include "server/test_utils.h"

using namespace testing;
using namespace std;
using namespace util;
using namespace boost;

namespace dfly {

class HSetFamilyTest : public BaseFamilyTest {
 protected:
};

TEST_F(HSetFamilyTest, HSet) {
  auto resp = Run({"hset", "x", "a"});
  EXPECT_THAT(resp[0],  ErrArg("wrong number"));

  resp = Run({"hset", "x", "a", "b"});
  EXPECT_THAT(resp[0],  IntArg(1));
  resp = Run({"hset", "x", "a", "b"});
  EXPECT_THAT(resp[0],  IntArg(0));
  resp = Run({"hset", "x", "a", "c"});
  EXPECT_THAT(resp[0],  IntArg(0));
  resp = Run({"hset", "y", "a", "c", "d", "e"});
  EXPECT_THAT(resp[0],  IntArg(2));
}

}  // namespace dfly
