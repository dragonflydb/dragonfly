// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/generic_family.h"

#include "base/gtest.h"
#include "base/logging.h"
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
  constexpr uint64_t kNow = 1636070340000;
  UpdateTime(kNow);

  Run({"set", "key", "val"});
  auto resp = Run({"expire", "key", "1"});
  EXPECT_THAT(resp[0], IntArg(1));
  UpdateTime(kNow + 1000);
  resp = Run({"get", "key"});
  EXPECT_THAT(resp, ElementsAre(ArgType(RespExpr::NIL)));

  Run({"set", "key", "val"});
  resp = Run({"expireat", "key", absl::StrCat((kNow + 2000) / 1000)});
  EXPECT_THAT(resp[0], IntArg(1));
  resp = Run({"expireat", "key", absl::StrCat((kNow + 3000) / 1000)});
  EXPECT_THAT(resp[0], IntArg(1));

  UpdateTime(kNow + 2999);
  resp = Run({"get", "key"});
  EXPECT_THAT(resp[0], "val");

  UpdateTime(kNow + 3000);
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

}  // namespace dfly