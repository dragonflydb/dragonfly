// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/stream_family.h"

#include "base/gtest.h"
#include "base/logging.h"
#include "facade/facade_test.h"
#include "server/command_registry.h"
#include "server/test_utils.h"

using namespace testing;
using namespace std;
using namespace util;

namespace dfly {

class StreamFamilyTest : public BaseFamilyTest {
 protected:
};

TEST_F(StreamFamilyTest, Add) {
  auto resp = Run({"xadd", "key", "*", "field", "value"});
  ASSERT_THAT(resp, ArgType(RespExpr::STRING));
  string id = string(ToSV(resp.GetBuf()));
  EXPECT_THAT(id, EndsWith("-0"));

  resp = Run({"xrange", "null", "-", "+"});
  EXPECT_THAT(resp, ArrLen(0));

  resp = Run({"xrange", "key", "-", "+"});
  EXPECT_THAT(resp, ArrLen(2));
  auto sub_arr = resp.GetVec();
  EXPECT_THAT(sub_arr, ElementsAre(id, ArrLen(2)));

  resp = Run({"xlen", "key"});
  EXPECT_THAT(resp, IntArg(1));
}

TEST_F(StreamFamilyTest, AddExtended) {
  auto resp1 = Run({"xadd", "key", "maxlen", "1", "*", "field1", "val1"});
  Run({"xadd", "key", "maxlen", "1", "*", "field2", "val2"});
  string id1 = string(ToSV(resp1.GetBuf()));

  EXPECT_THAT(Run({"xlen", "key"}), IntArg(1));
  EXPECT_THAT(Run({"xrange", "key", id1, id1}), ArrLen(0));
}

}  // namespace dfly