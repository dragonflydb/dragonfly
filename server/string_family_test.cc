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

TEST_F(StringFamilyTest, Expire) {
  constexpr uint64_t kNow = 232279092000;

  UpdateTime(kNow);
  ASSERT_THAT(Run({"set", "key", "val", "PX", "20"}), RespEq("OK"));

  UpdateTime(kNow + 10);
  EXPECT_THAT(Run({"get", "key"}), RespEq("val"));

  UpdateTime(kNow + 20);

  EXPECT_THAT(Run({"get", "key"}), ElementsAre(ArgType(RespExpr::NIL)));

  ASSERT_THAT(Run({"set", "i", "1", "PX", "10"}), RespEq("OK"));
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

}  // namespace dfly
