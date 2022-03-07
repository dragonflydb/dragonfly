// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/hset_family.h"

extern "C" {
#include "redis/listpack.h"
#include "redis/object.h"
#include "redis/sds.h"
#include "redis/zmalloc.h"
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
  static void SetUpTestSuite() {
    init_zmalloc_threadlocal();
  }
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

  resp = Run({"hset", "x", "a", "b"});
  EXPECT_THAT(resp[0], IntArg(1));
  resp = Run({"hlen", "x"});
  EXPECT_THAT(resp[0], IntArg(1));

  resp = Run({"hexists", "x", "a"});
  EXPECT_THAT(resp[0], IntArg(1));

  resp = Run({"hexists", "x", "b"});
  EXPECT_THAT(resp[0], IntArg(0));

  resp = Run({"hexists", "y", "a"});
  EXPECT_THAT(resp[0], IntArg(0));

  resp = Run({"hset", "x", "a", "b"});
  EXPECT_THAT(resp[0], IntArg(0));

  resp = Run({"hset", "x", "a", "c"});
  EXPECT_THAT(resp[0], IntArg(0));

  resp = Run({"hset", "y", "a", "c", "d", "e"});
  EXPECT_THAT(resp[0], IntArg(2));

  resp = Run({"hdel", "y", "a", "d"});
  EXPECT_THAT(resp[0], IntArg(2));
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

  resp = Run({"hkeys", "x"});
  EXPECT_THAT(resp, UnorderedElementsAre("a", "b", "c"));

  resp = Run({"hvals", "x"});
  EXPECT_THAT(resp, UnorderedElementsAre("1", "2", "3"));

  resp = Run({"hmget", "x", "a", "c", "d"});
  EXPECT_THAT(resp, ElementsAre("1", "3", ArgType(RespExpr::NIL)));

  resp = Run({"hgetall", "x"});
  EXPECT_THAT(resp, ElementsAre("a", "1", "b", "2", "c", "3"));
}

}  // namespace dfly
