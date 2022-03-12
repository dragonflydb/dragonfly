// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/generic_family.h"

#include "base/gtest.h"
#include "base/logging.h"
#include "facade/facade_test.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/engine_shard_set.h"
#include "server/string_family.h"
#include "server/test_utils.h"
#include "server/transaction.h"
#include "util/uring/uring_pool.h"

using namespace testing;
using namespace std;
using namespace util;
using namespace boost;
using absl::StrCat;

namespace dfly {

class GenericFamilyTest : public BaseFamilyTest {
};

TEST_F(GenericFamilyTest, Expire) {
  Run({"set", "key", "val"});
  auto resp = Run({"expire", "key", "1"});

  EXPECT_THAT(resp[0], IntArg(1));
  UpdateTime(expire_now_ + 1000);
  resp = Run({"get", "key"});
  EXPECT_THAT(resp, ElementsAre(ArgType(RespExpr::NIL)));

  Run({"set", "key", "val"});
  resp = Run({"pexpireat", "key", absl::StrCat(expire_now_ + 2000)});
  EXPECT_THAT(resp[0], IntArg(1));

  // override
  resp = Run({"pexpireat", "key", absl::StrCat(expire_now_ + 3000)});
  EXPECT_THAT(resp[0], IntArg(1));

  UpdateTime(expire_now_ + 2999);
  resp = Run({"get", "key"});
  EXPECT_THAT(resp[0], "val");

  UpdateTime(expire_now_ + 3000);
  resp = Run({"get", "key"});
  EXPECT_THAT(resp[0], ArgType(RespExpr::NIL));
}


TEST_F(GenericFamilyTest, Del) {
  for (size_t i = 0; i < 1000; ++i) {
    Run({"set", StrCat("foo", i), "1"});
    Run({"set", StrCat("bar", i), "1"});
  }

  ASSERT_EQ(2000, CheckedInt({"dbsize"}));

  auto exist_fb = pp_->at(0)->LaunchFiber([&] {
    for (size_t i = 0; i < 1000; ++i) {
      int64_t resp = CheckedInt({"exists", StrCat("foo", i), StrCat("bar", i)});
      ASSERT_TRUE(2 == resp || resp == 0) << resp << " " << i;
    }
  });

  auto del_fb = pp_->at(2)->LaunchFiber([&] {
    for (size_t i = 0; i < 1000; ++i) {
      auto resp = CheckedInt({"del", StrCat("foo", i), StrCat("bar", i)});
      ASSERT_EQ(2, resp);
    }
  });

  exist_fb.join();
  del_fb.join();
}

TEST_F(GenericFamilyTest, Exists) {
  Run({"mset", "x", "0", "y", "1"});
  auto resp = Run({"exists", "x", "y", "x"});
  EXPECT_THAT(resp[0], IntArg(3));
}


TEST_F(GenericFamilyTest, Rename) {
  RespVec resp = Run({"mset", "x", "0", "b", "1"});
  ASSERT_THAT(resp, RespEq("OK"));
  ASSERT_EQ(2, last_cmd_dbg_info_.shards_count);

  resp = Run({"rename", "z", "b"});
  ASSERT_THAT(resp[0], ErrArg("no such key"));

  resp = Run({"rename", "x", "b"});
  ASSERT_THAT(resp, RespEq("OK"));

  int64_t val = CheckedInt({"get", "x"});
  ASSERT_EQ(kint64min, val);  // does not exist

  val = CheckedInt({"get", "b"});
  ASSERT_EQ(0, val);  // it has value of x.

  const char* keys[2] = {"b", "x"};
  auto ren_fb = pp_->at(0)->LaunchFiber([&] {
    for (size_t i = 0; i < 200; ++i) {
      int j = i % 2;
      auto resp = Run({"rename", keys[j], keys[1 - j]});
      ASSERT_THAT(resp, RespEq("OK"));
    }
  });

  auto exist_fb = pp_->at(2)->LaunchFiber([&] {
    for (size_t i = 0; i < 300; ++i) {
      int64_t resp = CheckedInt({"exists", "x", "b"});
      ASSERT_EQ(1, resp);
    }
  });

  ren_fb.join();
  exist_fb.join();
}

}  // namespace dfly
