// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/topk_family.h"

#include <absl/flags/flag.h>

#include "base/gtest.h"
#include "base/logging.h"
#include "facade/facade_test.h"
#include "server/test_utils.h"

using namespace testing;
using namespace util;

namespace dfly {

class TopKFamilyTest : public BaseFamilyTest {
 protected:
};

TEST_F(TopKFamilyTest, Basic) {
  // errors
  std::string err = "ERR wrong number of arguments for 'topk.reserve' command";
  auto resp = Run({"TOPK.RESERVE", "k1"});
  ASSERT_THAT(resp, ErrArg(err));

  resp = Run({"TOPK.RESERVE", "k1", "12"});
  EXPECT_EQ(resp, "OK");

  err = "ERR wrong number of arguments for 'topk.info' command";
  resp = Run({"TOPK.INFO"});
  ASSERT_THAT(resp, ErrArg(err));

  resp = Run({"TOPK.INFO", "k1"});
  auto v = resp.GetVec();
  ASSERT_THAT(v, ElementsAre("k", 12, "width", 3, "depth", 4, "decay", DoubleArg(1.08)));

  err = "ERR wrong number of arguments for 'topk.add' command";
  resp = Run({"TOPK.ADD", "k1"});
  ASSERT_THAT(resp, ErrArg(err));

  resp = Run({"TOPK.ADD", "k1", "foo", "bar", "fooz"});
  v = resp.GetVec();
  // TODO fix reply of command when an element of a cell is replaced with the one added here
  ASSERT_THAT(v,
              ElementsAre(ArgType(RespExpr::NIL), ArgType(RespExpr::NIL), ArgType(RespExpr::NIL)));
  // First time nothing is added
  resp = Run({"TOPK.QUERY", "k1", "foo", "bar", "fooz"});
  v = resp.GetVec();
  ASSERT_THAT(v, ElementsAre(0, 0, 0));

  // Second time elements are added
  resp = Run({"TOPK.ADD", "k1", "foo", "bar", "fooz"});
  v = resp.GetVec();
  ASSERT_THAT(v,
              ElementsAre(ArgType(RespExpr::NIL), ArgType(RespExpr::NIL), ArgType(RespExpr::NIL)));

  resp = Run({"TOPK.QUERY", "k1", "foo", "bar", "fooz"});
  v = resp.GetVec();
  ASSERT_THAT(v, ElementsAre(1, 1, 1));

  resp = Run({"TOPK.LIST", "k1"});
  v = resp.GetVec();
  ASSERT_THAT(v, UnorderedElementsAre("foo", "bar", "fooz"));

  // TODO add TOPK.INCRBY
}

}  // namespace dfly
