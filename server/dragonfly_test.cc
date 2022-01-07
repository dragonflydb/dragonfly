// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <absl/strings/str_join.h>
#include <absl/strings/strip.h>
#include <gmock/gmock.h>

#include "base/gtest.h"
#include "base/logging.h"
#include "server/conn_context.h"
#include "server/main_service.h"
#include "server/redis_parser.h"
#include "server/test_utils.h"
#include "util/uring/uring_pool.h"

namespace dfly {

using namespace absl;
using namespace boost;
using namespace std;
using namespace util;
using ::io::Result;
using testing::ElementsAre;
using testing::HasSubstr;

namespace {

constexpr unsigned kPoolThreadCount = 4;

const char kKey1[] = "x";
const char kKey2[] = "b";
const char kKey3[] = "c";
const char kKey4[] = "y";

}  // namespace

// This test is responsible for server and main service
// (connection, transaction etc) families.
class DflyEngineTest : public BaseFamilyTest {
 protected:
  DflyEngineTest() : BaseFamilyTest() {
    num_threads_ = kPoolThreadCount;
  }
};

TEST_F(DflyEngineTest, Multi) {
  RespVec resp = Run({"multi"});
  ASSERT_THAT(resp, RespEq("OK"));

  resp = Run({"get", kKey1});
  ASSERT_THAT(resp, RespEq("QUEUED"));

  resp = Run({"get", kKey4});
  ASSERT_THAT(resp, RespEq("QUEUED"));

  resp = Run({"exec"});
  ASSERT_THAT(resp, ElementsAre(ArgType(RespExpr::NIL), ArgType(RespExpr::NIL)));

  atomic_bool tx_empty = true;

  ess_->RunBriefInParallel([&](EngineShard* shard) {
    if (!shard->txq()->Empty())
      tx_empty.store(false);
  });
  EXPECT_TRUE(tx_empty);

  resp = Run({"get", kKey4});
  ASSERT_THAT(resp, ElementsAre(ArgType(RespExpr::NIL)));

  ASSERT_FALSE(service_->IsLocked(0, kKey1));
  ASSERT_FALSE(service_->IsLocked(0, kKey4));
}


TEST_F(DflyEngineTest, MultiEmpty) {
  RespVec resp = Run({"multi"});
  ASSERT_THAT(resp, RespEq("OK"));
  resp = Run({"exec"});

  ASSERT_THAT(resp[0], ArrLen(0));
}

TEST_F(DflyEngineTest, MultiSeq) {
  RespVec resp = Run({"multi"});
  ASSERT_THAT(resp, RespEq("OK"));

  resp = Run({"set", kKey1, absl::StrCat(1)});
  ASSERT_THAT(resp, RespEq("QUEUED"));
  resp = Run({"get", kKey1});
  ASSERT_THAT(resp, RespEq("QUEUED"));
  resp = Run({"mget", kKey1, kKey4});
  ASSERT_THAT(resp, RespEq("QUEUED"));
  resp = Run({"exec"});

  ASSERT_FALSE(service_->IsLocked(0, kKey1));
  ASSERT_FALSE(service_->IsLocked(0, kKey4));

  EXPECT_THAT(resp, ElementsAre(StrArg("OK"), StrArg("1"), ArrLen(2)));
  const RespExpr::Vec& arr = *get<RespVec*>(resp[2].u);
  ASSERT_THAT(arr, ElementsAre("1", ArgType(RespExpr::NIL)));
}

TEST_F(DflyEngineTest, MultiConsistent) {
  auto mset_fb = pp_->at(0)->LaunchFiber([&] {
    for (size_t i = 1; i < 10; ++i) {
      string base = StrCat(i * 900);
      RespVec resp = Run({"mset", kKey1, base, kKey4, base});
      ASSERT_THAT(resp, RespEq("OK"));
    }
  });

  auto fb = pp_->at(1)->LaunchFiber([&] {
    RespVec resp = Run({"multi"});
    ASSERT_THAT(resp, RespEq("OK"));
    this_fiber::sleep_for(1ms);

    resp = Run({"get", kKey1});
    ASSERT_THAT(resp, RespEq("QUEUED"));

    resp = Run({"get", kKey4});
    ASSERT_THAT(resp, RespEq("QUEUED"));

    resp = Run({"mget", kKey4, kKey1});
    ASSERT_THAT(resp, RespEq("QUEUED"));

    resp = Run({"exec"});

    EXPECT_THAT(resp, ElementsAre(ArgType(RespExpr::STRING), ArgType(RespExpr::STRING),
                                  ArgType(RespExpr::ARRAY)));
    ASSERT_EQ(resp[0].GetBuf(), resp[1].GetBuf());
    const RespVec& arr = *get<RespVec*>(resp[2].u);
    EXPECT_THAT(arr, ElementsAre(ArgType(RespExpr::STRING), ArgType(RespExpr::STRING)));
    EXPECT_EQ(arr[0].GetBuf(), arr[1].GetBuf());
    EXPECT_EQ(arr[0].GetBuf(), resp[0].GetBuf());
  });

  mset_fb.join();
  fb.join();
  ASSERT_FALSE(service_->IsLocked(0, kKey1));
  ASSERT_FALSE(service_->IsLocked(0, kKey4));
}

TEST_F(DflyEngineTest, MultiRename) {
  RespVec resp = Run({"multi"});
  ASSERT_THAT(resp, RespEq("OK"));
  Run({"set", kKey1, "1"});

  resp = Run({"rename", kKey1, kKey4});
  ASSERT_THAT(resp, RespEq("QUEUED"));
  resp = Run({"exec"});

  EXPECT_THAT(resp, ElementsAre(StrArg("OK"), StrArg("OK")));
  ASSERT_FALSE(service_->IsLocked(0, kKey1));
  ASSERT_FALSE(service_->IsLocked(0, kKey4));
}

}  // namespace dfly
