// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/tdigest_family.h"

#include <absl/flags/flag.h>

#include "base/gtest.h"
#include "base/logging.h"
#include "facade/facade_test.h"
#include "server/test_utils.h"

using namespace testing;
using namespace util;

namespace dfly {

class TDigestFamilyTest : public BaseFamilyTest {
 protected:
};

TEST_F(TDigestFamilyTest, TDigestBasic) {
  auto resp = Run({"TDIGEST.CREATE", "k1"});
  EXPECT_EQ(resp, "OK");

  resp = Run({"TDIGEST.CREATE", "k1", "COMPRESSION", "200"});
  ASSERT_THAT(resp, ErrArg("ERR key already exists"));

  resp = Run({"TDIGEST.CREATE", "k2", "COMPRESSION", "200"});
  EXPECT_EQ(resp, "OK");
}

TEST_F(TDigestFamilyTest, TDigestMerge) {
  auto resp = Run({"TDIGEST.CREATE", "k1"});
  resp = Run({"TDIGEST.CREATE", "k2"});

  resp = Run({"TDIGEST.ADD", "k1", "10.0", "20.0"});
  EXPECT_EQ(resp, "OK");
  resp = Run({"TDIGEST.ADD", "k2", "30.0", "40.0"});
  EXPECT_EQ(resp, "OK");

  resp = Run({"TDIGEST.MERGE", "res", "2", "k1", "k2"});
  EXPECT_EQ(resp, "OK");

  resp = Run({"TDIGEST.BYRANK", "res", "0", "1", "2", "3", "4"});
  auto results = resp.GetVec();
  ASSERT_THAT(results, ElementsAre(DoubleArg(10), DoubleArg(20), DoubleArg(30), DoubleArg(40),
                                   DoubleArg(INFINITY)));

  resp = Run({"TDIGEST.INFO", "res"});
  // TODO add this
}

}  // namespace dfly
