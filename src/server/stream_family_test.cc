// Copyright 2022, DragonflyDB authors.  All rights reserved.
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

  resp = Run({"xadd", "key", "badid", "f1", "val1"});
  EXPECT_THAT(resp, ErrArg("Invalid stream ID"));
}

TEST_F(StreamFamilyTest, AddExtended) {
  auto resp0 = Run({"xadd", "key", "5", "f1", "v1", "f2", "v2"});
  EXPECT_EQ(resp0, "5-0");
  resp0 = Run({"xrange", "key", "5-0", "5-0"});
  EXPECT_THAT(resp0, ArrLen(2));
  auto sub_arr = resp0.GetVec();
  EXPECT_THAT(sub_arr, ElementsAre("5-0", ArrLen(4)));
  sub_arr = sub_arr[1].GetVec();
  EXPECT_THAT(sub_arr, ElementsAre("f1", "v1", "f2", "v2"));

  auto resp1 = Run({"xadd", "key", "maxlen", "1", "*", "field1", "val1"});
  string id1 = string(ToSV(resp1.GetBuf()));

  auto resp2 = Run({"xadd", "key", "maxlen", "1", "*", "field2", "val2"});
  string id2 = string(ToSV(resp2.GetBuf()));

  EXPECT_THAT(Run({"xlen", "key"}), IntArg(1));
  EXPECT_THAT(Run({"xrange", "key", id1, id1}), ArrLen(0));

  auto resp3 = Run({"xadd", "key", id2, "f1", "val1"});
  EXPECT_THAT(resp3, ErrArg("equal or smaller than"));
}

TEST_F(StreamFamilyTest, Range) {
  Run({"xadd", "key", "1-*", "f1", "v1"});
  Run({"xadd", "key", "1-*", "f2", "v2"});
  auto resp = Run({"xrange", "key", "-", "+"});
  EXPECT_THAT(resp, ArrLen(2));
  auto sub_arr = resp.GetVec();
  EXPECT_THAT(sub_arr, ElementsAre(ArrLen(2), ArrLen(2)));
  auto sub0 = sub_arr[0].GetVec();
  auto sub1 = sub_arr[1].GetVec();
  EXPECT_THAT(sub0, ElementsAre("1-0", ArrLen(2)));
  EXPECT_THAT(sub1, ElementsAre("1-1", ArrLen(2)));

  resp = Run({"xrevrange", "key", "-", "+"});
  sub_arr = resp.GetVec();
  sub0 = sub_arr[0].GetVec();
  sub1 = sub_arr[1].GetVec();
  EXPECT_THAT(sub0, ElementsAre("1-1", ArrLen(2)));
  EXPECT_THAT(sub1, ElementsAre("1-0", ArrLen(2)));
}

}  // namespace dfly
