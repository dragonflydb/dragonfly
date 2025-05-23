// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <absl/flags/flag.h>

#include "base/gtest.h"
#include "base/logging.h"
#include "facade/facade_test.h"
#include "server/tdigest_family.h"
#include "server/test_utils.h"

using namespace testing;
using namespace util;

namespace dfly {

class CuckooFilterFamilyTest : public BaseFamilyTest {
 protected:
};

TEST_F(CuckooFilterFamilyTest, Simple) {
  auto resp = Run({"CF.RESERVE", "my_filter", "100"});
  EXPECT_EQ(resp, "OK");

  resp = Run({"CF.ADD", "my_filter", "foo"});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"CF.EXISTS", "my_filter", "foo"});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"CF.ADD", "my_filter", "foo"});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"CF.EXISTS", "my_filter", "foo"});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"CF.COUNT", "my_filter", "foo"});
  EXPECT_THAT(resp, IntArg(2));

  resp = Run({"CF.DEL", "my_filter", "foo"});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"CF.EXISTS", "my_filter", "foo"});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"CF.COUNT", "my_filter", "foo"});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"CF.DEL", "my_filter", "foo"});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"CF.EXISTS", "my_filter", "foo"});
  EXPECT_THAT(resp, IntArg(0));

  resp = Run({"CF.COUNT", "my_filter", "foo"});
  EXPECT_THAT(resp, IntArg(0));

  resp = Run({"CF.DEL", "my_filter", "foo"});
  EXPECT_THAT(resp, IntArg(0));
}

}  // namespace dfly
