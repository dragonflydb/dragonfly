// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/hset_family.h"

extern "C" {
#include "redis/listpack.h"
#include "redis/object.h"
#include "redis/sds.h"
}

#include "base/gtest.h"
#include "base/logging.h"
#include "facade/facade_test.h"
#include "server/test_utils.h"

using namespace testing;
using namespace std;
using namespace util;
using namespace boost;
using namespace facade;

namespace dfly {

class HSetFamilyTest : public BaseFamilyTest {
 protected:
};

TEST_F(HSetFamilyTest, Hash) {
  robj* obj = createHashObject();
  sds field = sdsnew("field");
  sds val = sdsnew("value");
  hashTypeSet(obj, field, val, 0);
  sdsfree(field);
  sdsfree(val);
  decrRefCount(obj);
}

TEST_F(HSetFamilyTest, Basic) {
  auto resp = Run({"hset", "x", "a"});
  EXPECT_THAT(resp[0], ErrArg("wrong number"));

  EXPECT_THAT(Run({"HSET", "hs", "key1", "val1", "key2"}), ElementsAre(ErrArg("wrong number")));

  EXPECT_EQ(1, CheckedInt({"hset", "x", "a", "b"}));
  EXPECT_EQ(1, CheckedInt({"hlen", "x"}));

  EXPECT_EQ(1, CheckedInt({"hexists", "x", "a"}));
  EXPECT_EQ(0, CheckedInt({"hexists", "x", "b"}));
  EXPECT_EQ(0, CheckedInt({"hexists", "y", "a"}));

  EXPECT_EQ(0, CheckedInt({"hset", "x", "a", "b"}));
  EXPECT_EQ(0, CheckedInt({"hset", "x", "a", "c"}));
  EXPECT_EQ(0, CheckedInt({"hset", "x", "a", ""}));

  EXPECT_EQ(2, CheckedInt({"hset", "y", "a", "c", "d", "e"}));
  EXPECT_EQ(2, CheckedInt({"hdel", "y", "a", "d"}));
}

TEST_F(HSetFamilyTest, HSetLarge) {
  string val(1024, 'b');

  auto resp = Run({"hset", "x", "a", val});
  EXPECT_THAT(resp[0], IntArg(1));
  resp = Run({"hlen", "x"});
  EXPECT_THAT(resp[0], IntArg(1));
}

TEST_F(HSetFamilyTest, Get) {
  auto resp = Run({"hset", "x", "a", "1", "b", "2", "c", "3"});
  EXPECT_THAT(resp[0], IntArg(3));

  resp = Run({"hmget", "unkwn", "a", "c"});
  EXPECT_THAT(resp, ElementsAre(ArgType(RespExpr::NIL), ArgType(RespExpr::NIL)));

  resp = Run({"hkeys", "x"});
  EXPECT_THAT(resp, UnorderedElementsAre("a", "b", "c"));

  resp = Run({"hvals", "x"});
  EXPECT_THAT(resp, UnorderedElementsAre("1", "2", "3"));

  resp = Run({"hmget", "x", "a", "c", "d"});
  EXPECT_THAT(resp, ElementsAre("1", "3", ArgType(RespExpr::NIL)));

  resp = Run({"hgetall", "x"});
  EXPECT_THAT(resp, ElementsAre("a", "1", "b", "2", "c", "3"));
}

TEST_F(HSetFamilyTest, HSetNx) {
  EXPECT_EQ(1, CheckedInt({"hsetnx", "key", "field", "val"}));
  EXPECT_THAT(Run({"hget", "key", "field"}), RespEq("val"));

  EXPECT_EQ(0, CheckedInt({"hsetnx", "key", "field", "val2"}));
  EXPECT_THAT(Run({"hget", "key", "field"}), RespEq("val"));

  EXPECT_EQ(1, CheckedInt({"hsetnx", "key", "field2", "val2"}));
  EXPECT_THAT(Run({"hget", "key", "field2"}), RespEq("val2"));

  // check dict path
  EXPECT_EQ(0, CheckedInt({"hsetnx", "key", "field2", string(512, 'a')}));
  EXPECT_THAT(Run({"hget", "key", "field2"}), RespEq("val2"));
}

}  // namespace dfly
