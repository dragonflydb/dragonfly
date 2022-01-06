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

  resp = Run({"get", "x"});
  ASSERT_THAT(resp, RespEq("QUEUED"));

  resp = Run({"get", "y"});
  ASSERT_THAT(resp, RespEq("QUEUED"));

  resp = Run({"exec"});
  ASSERT_THAT(resp, ElementsAre(ArgType(RespExpr::NIL), ArgType(RespExpr::NIL)));

  atomic_bool tx_empty = true;

  ess_->RunBriefInParallel([&](EngineShard* shard) {
    if (!shard->txq()->Empty())
      tx_empty.store(false);
  });
  EXPECT_TRUE(tx_empty);

  resp = Run({"get", "y"});
  ASSERT_THAT(resp, ElementsAre(ArgType(RespExpr::NIL)));

  ASSERT_FALSE(service_->IsLocked(0, "x"));
  ASSERT_FALSE(service_->IsLocked(0, "y"));
}

}  // namespace dfly
