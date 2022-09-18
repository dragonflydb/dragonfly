// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/bitops_family.h"

#include "base/gtest.h"
#include "base/logging.h"
#include "facade/facade_test.h"
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

class BitOpsFamilyTest : public BaseFamilyTest {
 protected:
};

const long EXPECTED_VALUE_SETBIT[] = {0, 1, 1, 0, 0, 0,
                                      0, 1, 0, 1, 1, 0};  // taken from running this on redis
const int32_t ITERATIONS = sizeof(EXPECTED_VALUE_SETBIT) / sizeof(EXPECTED_VALUE_SETBIT[0]);

TEST_F(BitOpsFamilyTest, GetBit) {
  auto resp = Run({"set", "foo", "abc"});

  EXPECT_EQ(resp, "OK");

  for (int32_t i = 0; i < ITERATIONS; i++) {
    EXPECT_EQ(EXPECTED_VALUE_SETBIT[i], CheckedInt({"getbit", "foo", std::to_string(i)}));
  }

  // make sure that when accessing bit that is not in the range its working and we are
  // getting 0
  EXPECT_EQ(0, CheckedInt({"getbit", "foo", std::to_string(strlen("abc") + 5)}));
}

TEST_F(BitOpsFamilyTest, SetBitExistingKey) {
  // this test would test when we have the value in place and
  // we are overriding and existing key
  // so there are no allocations of keys
  auto resp = Run({"set", "foo", "abc"});

  EXPECT_EQ(resp, "OK");

  // we are setting all to 1s first, we are expecting to get the old values
  for (int32_t i = 0; i < ITERATIONS; i++) {
    EXPECT_EQ(EXPECTED_VALUE_SETBIT[i], CheckedInt({"setbit", "foo", std::to_string(i), "1"}));
  }

  for (int32_t i = 0; i < ITERATIONS; i++) {
    EXPECT_EQ(1, CheckedInt({"getbit", "foo", std::to_string(i)}));
  }
}

TEST_F(BitOpsFamilyTest, SetBitMissingKey) {
  // This test would run without pre-allocated existing key
  // so we need to allocate the key as part of setting the values
  for (int32_t i = 0; i < ITERATIONS; i++) {  // we are setting all to 1s first, we are expecting
    // get 0s since we didn't have this key before
    EXPECT_EQ(0, CheckedInt({"setbit", "foo", std::to_string(i), "1"}));
  }

  // now all that we set are at 1s
  for (int32_t i = 0; i < ITERATIONS; i++) {
    EXPECT_EQ(1, CheckedInt({"getbit", "foo", std::to_string(i)}));
  }
}

}  // end of namespace dfly
