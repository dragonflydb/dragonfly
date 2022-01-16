// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/list_family.h"

#include <absl/strings/match.h>

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
namespace this_fiber = ::boost::this_fiber;
namespace fibers = ::boost::fibers;

namespace dfly {

class ListFamilyTest : public BaseFamilyTest {
 protected:
  ListFamilyTest() {
    num_threads_ = 4;
  }
};

const char kKey1[] = "x";
const char kKey2[] = "b";
const char kKey3[] = "c";

TEST_F(ListFamilyTest, Basic) {
  auto resp = Run({"lpush", kKey1, "1"});
  EXPECT_THAT(resp[0], IntArg(1));
  resp = Run({"lpush", kKey2, "2"});
  ASSERT_THAT(resp[0], IntArg(1));
  resp = Run({"llen", kKey1});
  ASSERT_THAT(resp[0], IntArg(1));
}

TEST_F(ListFamilyTest, Expire) {
  auto resp = Run({"lpush", kKey1, "1"});
  EXPECT_THAT(resp[0], IntArg(1));

  constexpr uint64_t kNow = 232279092000;
  UpdateTime(kNow);
  resp = Run({"expire", kKey1, "1"});
  EXPECT_THAT(resp[0], IntArg(1));

  UpdateTime(kNow + 1000);
  resp = Run({"lpush", kKey1, "1"});
  EXPECT_THAT(resp[0], IntArg(1));
}


TEST_F(ListFamilyTest, BLPopUnblocking) {
  auto resp = Run({"lpush", kKey1, "1"});
  EXPECT_THAT(resp[0], IntArg(1));
  resp = Run({"lpush", kKey2, "2"});
  ASSERT_THAT(resp, ElementsAre(IntArg(1)));

  resp = Run({"blpop", kKey1, kKey2});  // missing "0" delimiter.
  ASSERT_THAT(resp[0], ErrArg("timeout is not a float"));

  resp = Run({"blpop", kKey1, kKey2, "0"});
  ASSERT_EQ(2, GetDebugInfo().shards_count);
  EXPECT_THAT(resp, ElementsAre(kKey1, "1"));

  resp = Run({"blpop", kKey1, kKey2, "0"});
  EXPECT_THAT(resp, ElementsAre(kKey2, "2"));

  Run({"set", "z", "1"});

  resp = Run({"blpop", "z", "0"});
  ASSERT_THAT(resp[0], ErrArg("WRONGTYPE "));

  ASSERT_FALSE(IsLocked(0, "x"));
  ASSERT_FALSE(IsLocked(0, "y"));
  ASSERT_FALSE(IsLocked(0, "z"));
}

TEST_F(ListFamilyTest, BLPopBlocking) {
  RespVec resp0, resp1;

  // Run the fiber at creation.
  auto fb0 = pp_->at(0)->LaunchFiber(fibers::launch::dispatch, [&] {
    resp0 = Run({"blpop", "x", "0"});
    LOG(INFO) << "pop0";
  });

  this_fiber::sleep_for(50us);
  auto fb1 = pp_->at(1)->LaunchFiber([&] {
    resp1 = Run({"blpop", "x", "0"});
    LOG(INFO) << "pop1";
  });
  this_fiber::sleep_for(30us);
  pp_->at(1)->Await([&] { Run({"lpush", "x", "2", "1"}); });

  fb0.join();
  fb1.join();

  // fb0 should start first and be the first transaction blocked. Therefore, it should pop '1'.
  // sometimes order is switched, need to think how to fix it.
  int64_t epoch0 = GetDebugInfo("IO0").clock;
  int64_t epoch1 = GetDebugInfo("IO1").clock;
  ASSERT_LT(epoch0, epoch1);

  EXPECT_THAT(resp0, ElementsAre("x", "1"));
  ASSERT_FALSE(IsLocked(0, "x"));
}

TEST_F(ListFamilyTest, BLPopMultiple) {
  RespVec resp0, resp1;

  resp0 = Run({"blpop", kKey1, kKey2, "0.01"});  // timeout
  EXPECT_THAT(resp0, ElementsAre(ArgType(RespExpr::NIL_ARRAY)));
  ASSERT_EQ(2, GetDebugInfo().shards_count);

  ASSERT_FALSE(IsLocked(0, kKey1));
  ASSERT_FALSE(IsLocked(0, kKey2));

  auto fb1 = pp_->at(0)->LaunchFiber(fibers::launch::dispatch, [&] {
    resp0 = Run({"blpop", kKey1, kKey2, "0"});
  });

  pp_->at(1)->Await([&] { Run({"lpush", kKey1, "1", "2", "3"}); });
  fb1.join();
  EXPECT_THAT(resp0, ElementsAre(StrArg(kKey1), StrArg("3")));
  ASSERT_FALSE(IsLocked(0, kKey1));
  ASSERT_FALSE(IsLocked(0, kKey2));
  ess_->RunBriefInParallel([](EngineShard* es) { ASSERT_FALSE(es->HasAwakedTransaction()); });
}

TEST_F(ListFamilyTest, BLPopTimeout) {
  RespVec resp = Run({"blpop", kKey1, kKey2, kKey3, "0.01"});
  EXPECT_THAT(resp[0], ArgType(RespExpr::NIL_ARRAY));
  EXPECT_EQ(3, GetDebugInfo().shards_count);
  ASSERT_FALSE(service_->IsLocked(0, kKey1));

  // Under Multi
  resp = Run({"multi"});
  ASSERT_THAT(resp, RespEq("OK"));

  Run({"blpop", kKey1, "0"});
  resp = Run({"exec"});

  EXPECT_THAT(resp, ElementsAre(ArgType(RespExpr::NIL_ARRAY)));
  ASSERT_FALSE(service_->IsLocked(0, kKey1));
}

TEST_F(ListFamilyTest, BLPopSerialize) {
  RespVec blpop_resp;

  auto pop_fb = pp_->at(0)->LaunchFiber(fibers::launch::dispatch, [&] {
    blpop_resp = Run({"blpop", kKey1, kKey2, kKey3, "0"});
  });

  do {
    this_fiber::sleep_for(30us);
  } while (!IsLocked(0, kKey1));

  LOG(INFO) << "Starting multi";

  TxClock cl1, cl2;
  unsigned key1_len1 = 0, key1_len2 = 0;

  auto p1_fb = pp_->at(1)->LaunchFiber([&] {
    auto resp = Run({"multi"});  // We use multi to assign ts to lpush.
    ASSERT_THAT(resp, RespEq("OK"));
    Run({"lpush", kKey1, "A"});
    resp = Run({"exec"});

    // Either this lpush has run first or the one below.
    // In any case it must be that between 2 invocations of lpush (wrapped in multi)
    // blpop will be triggerred and it will empty the list again. Hence, in any case
    // lpush kKey1 here and below should return 1.
    EXPECT_THAT(resp, ElementsAre(IntArg(1)));
    key1_len1 = get<int64_t>(resp[0].u);
    cl1 = GetDebugInfo("IO1").clock;
    LOG(INFO) << "push1 ts: " << cl1;
  });

  auto p2_fb = pp_->at(2)->LaunchFiber([&] {
    auto resp = Run({"multi"});  // We use multi to assign ts to lpush.
    ASSERT_THAT(resp, RespEq("OK"));
    Run({"lpush", kKey1, "B"});
    Run({"lpush", kKey2, "C"});
    resp = Run({"exec"});
    EXPECT_THAT(resp, ElementsAre(IntArg(1), IntArg(1)));
    key1_len2 = get<int64_t>(resp[0].u);
    cl2 = GetDebugInfo("IO2").clock;
    LOG(INFO) << "push2 ts: " << cl2;
  });

  p1_fb.join();
  p2_fb.join();

  pop_fb.join();
  EXPECT_THAT(blpop_resp, ElementsAre(StrArg(kKey1), ArgType(RespExpr::STRING)));

  if (cl2 < cl1) {
    EXPECT_EQ(blpop_resp[1], "B");
  } else {
    EXPECT_EQ(blpop_resp[1], "A");
  }
}

}  // namespace dfly
