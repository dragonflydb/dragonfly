// Copyright 2022, DragonflyDB authors.  All rights reserved.
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

const int32_t EXPECTED_VALUES_BYTES_BIT_COUNT[] = {  // got this from redis 0 as start index
    4, 7, 11, 14, 17, 21, 21, 21, 21};

const int32_t BYTES_EXPECTED_VALUE_LEN =
    sizeof(EXPECTED_VALUES_BYTES_BIT_COUNT) / sizeof(EXPECTED_VALUES_BYTES_BIT_COUNT[0]);

TEST_F(BitOpsFamilyTest, BitCountByte) {
  // This would run without the bit flag - meaning it count on bytes boundaries
  auto resp = Run({"set", "foo", "farbar"});
  EXPECT_EQ(resp, "OK");
  EXPECT_EQ(0, CheckedInt({"bitcount", "foo2"}));  // on none existing key we are expecting 0

  for (int32_t i = 0; i < BYTES_EXPECTED_VALUE_LEN; i++) {
    EXPECT_EQ(EXPECTED_VALUES_BYTES_BIT_COUNT[i],
              CheckedInt({"bitcount", "foo", "0", std::to_string(i)}));
  }
  EXPECT_EQ(21, CheckedInt({"bitcount", "foo"}));  // the total number of bits in this value
}

TEST_F(BitOpsFamilyTest, BitCountByteSubRange) {
  // This test test using some sub ranges of bit count on bytes
  auto resp = Run({"set", "foo", "farbar"});
  EXPECT_EQ(resp, "OK");
  EXPECT_EQ(3, CheckedInt({"bitcount", "foo", "1", "1"}));
  EXPECT_EQ(7, CheckedInt({"bitcount", "foo", "1", "2"}));
  EXPECT_EQ(4, CheckedInt({"bitcount", "foo", "2", "2"}));
  EXPECT_EQ(0, CheckedInt({"bitcount", "foo", "3", "2"}));  // illegal range
  EXPECT_EQ(10, CheckedInt({"bitcount", "foo", "-3", "-1"}));
  EXPECT_EQ(13, CheckedInt({"bitcount", "foo", "-5", "-2"}));
  EXPECT_EQ(0, CheckedInt({"bitcount", "foo", "-1", "-2"}));  // illegal range
}

TEST_F(BitOpsFamilyTest, BitCountByteBitSubRange) {
  // This test test using some sub ranges of bit count on bytes
  auto resp = Run({"set", "foo", "abcdef"});
  EXPECT_EQ(resp, "OK");
  resp = Run({"bitcount", "foo", "bar", "BIT"});
  ASSERT_THAT(resp, ErrArg("value is not an integer or out of range"));

  EXPECT_EQ(1, CheckedInt({"bitcount", "foo", "1", "1", "BIT"}));
  EXPECT_EQ(2, CheckedInt({"bitcount", "foo", "1", "2", "BIT"}));
  EXPECT_EQ(1, CheckedInt({"bitcount", "foo", "2", "2", "BIT"}));
  EXPECT_EQ(0, CheckedInt({"bitcount", "foo", "3", "2", "bit"}));  // illegal range
  EXPECT_EQ(2, CheckedInt({"bitcount", "foo", "-3", "-1", "bit"}));
  EXPECT_EQ(2, CheckedInt({"bitcount", "foo", "-5", "-2", "bit"}));
  EXPECT_EQ(4, CheckedInt({"bitcount", "foo", "1", "9", "bit"}));
  EXPECT_EQ(7, CheckedInt({"bitcount", "foo", "2", "19", "bit"}));
  EXPECT_EQ(0, CheckedInt({"bitcount", "foo", "-1", "-2", "bit"}));  // illegal range
}

}  // end of namespace dfly
