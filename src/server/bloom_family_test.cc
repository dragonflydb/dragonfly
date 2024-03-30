// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/bloom_family.h"

#include "facade/facade_test.h"
#include "server/test_utils.h"

namespace dfly {

class BloomFamilyTest : public BaseFamilyTest {
 protected:
};

TEST_F(BloomFamilyTest, Basic) {
  auto resp = Run({"bf.reserve", "b1", "0.1", "32"});
  EXPECT_EQ(resp, "OK");
  resp = Run({"type", "b1"});
  EXPECT_EQ(resp, "MBbloom--");
}

}  // namespace dfly
