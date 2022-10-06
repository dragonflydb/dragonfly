// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/string_family.h"

#include "base/gtest.h"
#include "base/logging.h"
#include "facade/facade_test.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/engine_shard_set.h"
#include "server/error.h"
#include "server/test_utils.h"
#include "server/transaction.h"

using namespace testing;
using namespace std;
using namespace util;
using absl::StrCat;

namespace dfly {

class StringFamilyTest : public BaseFamilyTest {
 protected:
};

vector<int64_t> ToIntArr(const RespExpr& e) {
  vector<int64_t> res;
  CHECK_EQ(e.type, RespExpr::ARRAY);
  const RespVec* vec = get<RespVec*>(e.u);
  for (auto a : *vec) {
    int64_t val;
    std::string_view s = ToSV(a.GetBuf());
    CHECK(absl::SimpleAtoi(s, &val)) << s;
    res.push_back(val);
  }

  return res;
}

TEST_F(StringFamilyTest, SetGet) {
  auto resp = Run({"set", "key", "val"});

  EXPECT_EQ(resp, "OK");
  EXPECT_EQ(Run({"get", "key"}), "val");
  EXPECT_EQ(Run({"set", "key1", "1"}), "OK");
  EXPECT_EQ(Run({"get", "key1"}), "1");
  EXPECT_EQ(Run({"set", "key", "2"}), "OK");
  EXPECT_EQ(Run({"get", "key"}), "2");
}

TEST_F(StringFamilyTest, Incr) {
  ASSERT_EQ(Run({"set", "key", "0"}), "OK");
  ASSERT_THAT(Run({"incr", "key"}), IntArg(1));

  ASSERT_EQ(Run({"set", "key1", "123456789"}), "OK");
  ASSERT_THAT(Run({"incrby", "key1", "0"}), IntArg(123456789));

  ASSERT_EQ(Run({"set", "key1", "-123456789"}), "OK");
  ASSERT_THAT(Run({"incrby", "key1", "0"}), IntArg(-123456789));

  ASSERT_EQ(Run({"set", "key1", "   -123  "}), "OK");
  ASSERT_THAT(Run({"incrby", "key1", "1"}), ErrArg("ERR value is not an integer"));

  ASSERT_THAT(Run({"incrby", "ne", "0"}), IntArg(0));
  ASSERT_THAT(Run({"decrby", "a", "-9223372036854775808"}), ErrArg("overflow"));
}

TEST_F(StringFamilyTest, Append) {
  Run({"setex", "key", "100", "val"});
  EXPECT_THAT(Run({"ttl", "key"}), IntArg(100));

  EXPECT_THAT(Run({"append", "key", "bar"}), IntArg(6));
  EXPECT_THAT(Run({"ttl", "key"}), IntArg(100));
}

TEST_F(StringFamilyTest, Expire) {
  ASSERT_EQ(Run({"set", "key", "val", "PX", "20"}), "OK");

  AdvanceTime(10);
  EXPECT_EQ(Run({"get", "key"}), "val");

  AdvanceTime(10);

  EXPECT_THAT(Run({"get", "key"}), ArgType(RespExpr::NIL));

  ASSERT_THAT(Run({"set", "i", "1", "PX", "10"}), "OK");
  ASSERT_THAT(Run({"incr", "i"}), IntArg(2));

  AdvanceTime(10);
  ASSERT_THAT(Run({"incr", "i"}), IntArg(1));
}

TEST_F(StringFamilyTest, Set) {
  auto resp = Run({"set", "foo", "bar", "XX"});
  EXPECT_THAT(resp, ArgType(RespExpr::NIL));

  resp = Run({"set", "foo", "bar", "NX"});
  ASSERT_THAT(resp, "OK");
  resp = Run({"set", "foo", "bar", "NX"});
  EXPECT_THAT(resp, ArgType(RespExpr::NIL));

  resp = Run({"set", "foo", "bar", "xx"});
  ASSERT_THAT(resp, "OK");

  resp = Run({"set", "foo", "bar", "ex", "abc"});
  ASSERT_THAT(resp, ErrArg(kInvalidIntErr));

  resp = Run({"set", "foo", "bar", "ex", "-1"});
  ASSERT_THAT(resp, ErrArg("invalid expire time"));

  resp = Run({"set", "foo", "bar", "ex", "1"});
  ASSERT_THAT(resp, "OK");
}

TEST_F(StringFamilyTest, SetHugeKey) {
  const string key(36000000, 'b');
  auto resp = Run({"set", key, "1"});
  ASSERT_THAT(resp, "OK");
  Run({"del", key});
}

TEST_F(StringFamilyTest, MGetSet) {
  Run({"mset", "z", "0"});         // single key
  auto resp = Run({"mget", "z"});  // single key
  EXPECT_THAT(resp, "0");

  Run({"mset", "x", "0", "b", "0"});

  ASSERT_EQ(2, GetDebugInfo("IO0").shards_count);

  auto mget_fb = pp_->at(0)->LaunchFiber([&] {
    for (size_t i = 0; i < 1000; ++i) {
      RespExpr resp = Run({"mget", "b", "x"});
      ASSERT_EQ(RespExpr::ARRAY, resp.type);
      auto ivec = ToIntArr(resp);

      ASSERT_GE(ivec[1], ivec[0]);
    }
  });

  auto set_fb = pp_->at(1)->LaunchFiber([&] {
    for (size_t i = 1; i < 2000; ++i) {
      Run({"set", "x", StrCat(i)});
      Run({"set", "b", StrCat(i)});
    }
  });

  mget_fb.join();
  set_fb.join();
}

TEST_F(StringFamilyTest, MSetGet) {
  Run({"mset", "x", "0", "y", "0", "a", "0", "b", "0"});
  ASSERT_EQ(2, GetDebugInfo().shards_count);

  Run({"mset", "x", "0", "y", "0"});
  ASSERT_EQ(1, GetDebugInfo().shards_count);

  Run({"mset", "x", "1", "b", "5", "x", "0"});
  ASSERT_EQ(2, GetDebugInfo().shards_count);

  int64_t val = CheckedInt({"get", "x"});
  EXPECT_EQ(0, val);

  val = CheckedInt({"get", "b"});
  EXPECT_EQ(5, val);

  auto mset_fb = pp_->at(0)->LaunchFiber([&] {
    for (size_t i = 0; i < 1000; ++i) {
      RespExpr resp = Run({"mset", "x", StrCat(i), "b", StrCat(i)});
      ASSERT_EQ(resp, "OK") << i;
    }
  });

  // A problematic order when mset is not atomic: set x, get x, get b (old), set b
  auto get_fb = pp_->at(2)->LaunchFiber([&] {
    for (size_t i = 0; i < 1000; ++i) {
      int64_t x = CheckedInt({"get", "x"});
      int64_t z = CheckedInt({"get", "b"});

      ASSERT_LE(x, z) << "Inconsistency at " << i;
    }
  });

  mset_fb.join();
  get_fb.join();
}

TEST_F(StringFamilyTest, MSetDel) {
  auto mset_fb = pp_->at(0)->LaunchFiber([&] {
    for (size_t i = 0; i < 1000; ++i) {
      Run({"mset", "x", "0", "z", "0"});
    }
  });

  auto del_fb = pp_->at(2)->LaunchFiber([&] {
    for (size_t i = 0; i < 1000; ++i) {
      CheckedInt({"del", "x", "z"});
    }
  });

  mset_fb.join();
  del_fb.join();
}

TEST_F(StringFamilyTest, IntKey) {
  Run({"mset", "1", "1", "-1000", "-1000"});
  auto resp = Run({"get", "1"});
  ASSERT_THAT(resp, "1");
}

TEST_F(StringFamilyTest, SingleShard) {
  Run({"mset", "x", "1", "y", "1"});
  ASSERT_EQ(1, GetDebugInfo("IO0").shards_count);

  Run({"mget", "x", "y", "b"});
  ASSERT_EQ(2, GetDebugInfo("IO0").shards_count);

  auto resp = Run({"mget", "x", "y"});
  ASSERT_EQ(1, GetDebugInfo("IO0").shards_count);
  ASSERT_THAT(ToIntArr(resp), ElementsAre(1, 1));

  auto mset_fb = pp_->at(0)->LaunchFiber([&] {
    for (size_t i = 0; i < 100; ++i) {
      Run({"mset", "x", "0", "y", "0"});
    }
  });

  // Specially multiple shards to avoid fast-path.
  auto mget_fb = pp_->at(1)->LaunchFiber([&] {
    for (size_t i = 0; i < 100; ++i) {
      Run({"mget", "x", "b", "y"});
    }
  });
  mset_fb.join();
  mget_fb.join();
}

TEST_F(StringFamilyTest, MSetIncr) {
  /*  serializable orders
   init: x=z=0

   mset x=z=1
   mset, incr x, incr z = 2, 2
   incr x, mset, incr z = 1, 2
   incr x, incr z, mset = 1, 1
*/

  /* unserializable scenario when mset is not atomic with respect to incr x
      set x, incr x, incr z, set z = 2, 1
    */

  Run({"mset", "a", "0", "b", "0", "c", "0"});
  ASSERT_EQ(2, GetDebugInfo("IO0").shards_count);

  auto mset_fb = pp_->at(0)->LaunchFiber([&] {
    for (size_t i = 1; i < 1000; ++i) {
      string base = StrCat(i * 900);
      auto resp = Run({"mset", "b", base, "a", base, "c", base});
      ASSERT_EQ(resp, "OK");
    }
  });

  auto get_fb = pp_->at(1)->LaunchFiber([&] {
    for (unsigned j = 0; j < 900; ++j) {
      int64_t a = CheckedInt({"incr", "a"});
      int64_t b = CheckedInt({"incr", "b"});
      ASSERT_LE(a, b);

      int64_t c = CheckedInt({"incr", "c"});
      if (a > c) {
        LOG(ERROR) << "Consistency error ";
      }
      ASSERT_LE(a, c);
    }
  });
  mset_fb.join();
  get_fb.join();
}

TEST_F(StringFamilyTest, SetEx) {
  ASSERT_EQ(Run({"setex", "key", "1", "val"}), "OK");
  ASSERT_EQ(Run({"setex", "key", "10", "val"}), "OK");
  ASSERT_THAT(Run({"ttl", "key"}), IntArg(10));
  ASSERT_THAT(Run({"setex", "key", "0", "val"}), ErrArg("invalid expire time"));
}

TEST_F(StringFamilyTest, Range) {
  Run({"set", "key1", "Hello World"});
  EXPECT_EQ(Run({"getrange", "key1", "5", "3"}), "");

  Run({"SETRANGE", "key1", "6", "Earth"});
  EXPECT_EQ(Run({"get", "key1"}), "Hello Earth");

  Run({"SETRANGE", "key2", "2", "Earth"});
  EXPECT_EQ(Run({"get", "key2"}), string_view("\000\000Earth", 7));

  Run({"SETRANGE", "key3", "0", ""});
  EXPECT_EQ(0, CheckedInt({"exists", "key3"}));

  Run({"SETRANGE", "key3", "0", "abc"});
  EXPECT_EQ(1, CheckedInt({"exists", "key3"}));

  Run({"SET", "key3", "123"});
  EXPECT_EQ(Run({"getrange", "key3", "2", "3"}), "3");
  EXPECT_EQ(Run({"getrange", "key3", "3", "3"}), "");
  EXPECT_EQ(Run({"getrange", "key3", "4", "5"}), "");

  Run({"SET", "num", "1234"});
  EXPECT_EQ(Run({"getrange", "num", "3", "5000"}), "4");
  EXPECT_EQ(Run({"getrange", "num", "-5000", "10000"}), "1234");
}

TEST_F(StringFamilyTest, IncrByFloat) {
  Run({"SET", "nonum", "  11"});
  auto resp = Run({"INCRBYFLOAT", "nonum", "1.0"});
  EXPECT_THAT(resp, ErrArg("not a valid float"));

  Run({"SET", "nonum", "11 "});
  resp = Run({"INCRBYFLOAT", "nonum", "1.0"});
  EXPECT_THAT(resp, ErrArg("not a valid float"));

  Run({"SET", "num", "2.566"});
  resp = Run({"INCRBYFLOAT", "num", "1.0"});
  EXPECT_EQ(resp, "3.566");
}

TEST_F(StringFamilyTest, SetNx) {
  // Make sure that we "screen out" invalid parameters for this command
  // this is important as it uses similar path as the "normal" set
  auto resp = Run({"setnx", "foo", "bar", "XX"});
  EXPECT_THAT(resp, ErrArg("wrong number of arguments"));

  resp = Run({"setnx", "foo", "bar", "NX"});
  ASSERT_THAT(resp, ErrArg("wrong number of arguments"));

  resp = Run({"setnx", "foo", "bar", "xx"});
  ASSERT_THAT(resp, ErrArg("wrong number of arguments"));

  resp = Run({"setnx", "foo", "bar", "ex", "abc"});
  ASSERT_THAT(resp, ErrArg("wrong number of arguments"));

  resp = Run({"setnx", "foo", "bar", "ex", "-1"});
  ASSERT_THAT(resp, ErrArg("wrong number of arguments"));

  resp = Run({"setnx", "foo", "bar", "ex", "1"});
  ASSERT_THAT(resp, ErrArg("wrong number of arguments"));

  // now let see how it goes for the valid parameters
  EXPECT_EQ(1, CheckedInt({"setnx", "foo", "bar"}));
  EXPECT_EQ(Run({"get", "foo"}), "bar");
  // second call to the same key should return 0 as we have it
  EXPECT_EQ(0, CheckedInt({"setnx", "foo", "hello"}));
  EXPECT_EQ(Run({"get", "foo"}), "bar");  // the value was not changed
}

TEST_F(StringFamilyTest, SetPxAtExAt) {
  using std::chrono::duration_cast;
  using std::chrono::milliseconds;
  using std::chrono::seconds;
  using std::chrono::system_clock;

  // Expiration time as set at unix time
  auto resp = Run({"set", "foo", "bar", "EXAT", "-1"});
  ASSERT_THAT(resp, ErrArg("invalid expire time"));
  resp = Run({"set", "foo", "bar", "EXAT", std::to_string(time(nullptr) - 1)});
  ASSERT_THAT(resp, "OK");  // it would return OK but will not set the value - expiration time is 0
                            // (checked with Redis)
  EXPECT_EQ(Run({"get", "foo"}).type, facade::RespExpr::NIL);

  resp = Run({"set", "foo", "bar", "PXAT", "-1"});
  ASSERT_THAT(resp, ErrArg("invalid expire time"));

  auto now = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
  resp = Run({"set", "foo", "bar", "PXAT", std::to_string(now - 23)});
  ASSERT_THAT(resp, "OK");  // it would return OK but will not set the value (checked with Redis)
  EXPECT_EQ(Run({"get", "foo"}).type, facade::RespExpr::NIL);

  resp = Run({"set", "foo", "bar", "EXAT", std::to_string(time(nullptr) + 1)});
  ASSERT_THAT(resp, "OK");  // valid expiration time
  EXPECT_EQ(Run({"get", "foo"}), "bar");

  resp = Run({"set", "foo2", "abc", "PXAT", std::to_string(now + 300)});
  ASSERT_THAT(resp, "OK");
  EXPECT_EQ(Run({"get", "foo2"}), "abc");
}

TEST_F(StringFamilyTest, GetDel) {
  auto resp = Run({"set", "foo", "bar"});
  EXPECT_THAT(resp, "OK");

  resp = Run({"getdel", "foo"});
  // foo's value
  ASSERT_THAT(resp, ArgType(RespExpr::STRING));

  resp = Run({"get", "foo"});
  ASSERT_THAT(resp, ArgType(RespExpr::NIL));
}

}  // namespace dfly
