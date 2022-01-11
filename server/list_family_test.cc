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
using namespace boost;

namespace dfly {

class ListFamilyTest : public BaseFamilyTest {
 protected:
};

const char* kKey1 = "x";
const char* kKey2 = "b";

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

}  // namespace dfly
