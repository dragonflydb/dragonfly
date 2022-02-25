// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/string_family.h"

#include "base/gtest.h"
#include "base/logging.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/engine_shard_set.h"
#include "server/error.h"
#include "server/test_utils.h"
#include "server/transaction.h"
#include "util/uring/uring_pool.h"

using namespace testing;
using namespace std;
using namespace util;
using absl::StrCat;

namespace dfly {

class StringFamilyTest : public BaseFamilyTest {
 protected:
};

TEST_F(StringFamilyTest, SetGet) {
  auto resp = Run({"set", "key", "val"});

  EXPECT_THAT(resp, RespEq("OK"));
  EXPECT_THAT(Run({"get", "key"}), RespEq("val"));
  EXPECT_THAT(Run({"set", "key1", "1"}), RespEq("OK"));
  EXPECT_THAT(Run({"get", "key1"}), RespEq("1"));
  EXPECT_THAT(Run({"set", "key", "2"}), RespEq("OK"));
  EXPECT_THAT(Run({"get", "key"}), RespEq("2"));
}

TEST_F(StringFamilyTest, Incr) {
  ASSERT_THAT(Run({"set", "key", "0"}), RespEq("OK"));
  ASSERT_THAT(Run({"incr", "key"}), ElementsAre(IntArg(1)));

  ASSERT_THAT(Run({"set", "key1", "123456789"}), RespEq("OK"));
  ASSERT_THAT(Run({"incrby", "key1", "0"}), ElementsAre(IntArg(123456789)));

  ASSERT_THAT(Run({"set", "key1", "-123456789"}), RespEq("OK"));
  ASSERT_THAT(Run({"incrby", "key1", "0"}), ElementsAre(IntArg(-123456789)));

  ASSERT_THAT(Run({"set", "key1", "   -123  "}), RespEq("OK"));
  ASSERT_THAT(Run({"incrby", "key1", "1"}), ElementsAre(ErrArg("ERR value is not an integer")));

  ASSERT_THAT(Run({"incrby", "ne", "0"}), ElementsAre(IntArg(0)));
}

TEST_F(StringFamilyTest, Expire) {
  constexpr uint64_t kNow = 232279092000;

  UpdateTime(kNow);
  ASSERT_THAT(Run({"set", "key", "val", "PX", "20"}), RespEq("OK"));

  UpdateTime(kNow + 10);
  EXPECT_THAT(Run({"get", "key"}), RespEq("val"));

  UpdateTime(kNow + 20);

  EXPECT_THAT(Run({"get", "key"}), ElementsAre(ArgType(RespExpr::NIL)));

  ASSERT_THAT(Run({"set", "i", "1", "PX", "10"}), RespEq("OK"));
  ASSERT_THAT(Run({"incr", "i"}), ElementsAre(IntArg(2)));

  UpdateTime(kNow + 30);
  ASSERT_THAT(Run({"incr", "i"}), ElementsAre(IntArg(1)));
}

TEST_F(StringFamilyTest, Set) {
  auto resp = Run({"set", "foo", "bar", "XX"});
  EXPECT_THAT(resp, ElementsAre(ArgType(RespExpr::NIL)));

  resp = Run({"set", "foo", "bar", "NX"});
  ASSERT_THAT(resp, RespEq("OK"));
  resp = Run({"set", "foo", "bar", "NX"});
  EXPECT_THAT(resp, ElementsAre(ArgType(RespExpr::NIL)));

  resp = Run({"set", "foo", "bar", "xx"});
  ASSERT_THAT(resp, RespEq("OK"));

  resp = Run({"set", "foo", "bar", "ex", "abc"});
  ASSERT_THAT(resp, ElementsAre(ErrArg(kInvalidIntErr)));

  resp = Run({"set", "foo", "bar", "ex", "-1"});
  ASSERT_THAT(resp, ElementsAre(ErrArg("invalid expire time in set")));

  resp = Run({"set", "foo", "bar", "ex", "1"});
  ASSERT_THAT(resp, RespEq("OK"));
}

TEST_F(StringFamilyTest, MGetSet) {
  Run({"mset", "x", "0", "b", "0"});

  ASSERT_EQ(2, GetDebugInfo("IO0").shards_count);

  auto mget_fb = pp_->at(0)->LaunchFiber([&] {
    for (size_t i = 0; i < 1000; ++i) {
      auto resp = Run({"mget", "b", "x"});
      ASSERT_EQ(2, resp.size());
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
      RespVec resp = Run({"mset", "x", StrCat(i), "b", StrCat(i)});
      ASSERT_THAT(resp, RespEq("OK")) << i;
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
  ASSERT_THAT(resp, RespEq("1"));
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
  /*  serialzable orders
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
      ASSERT_THAT(resp, RespEq("OK"));
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

}  // namespace dfly
