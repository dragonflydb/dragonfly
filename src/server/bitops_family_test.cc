// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/bitops_family.h"

#include <bitset>
#include <iomanip>
#include <iostream>
#include <string>
#include <string_view>

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

class Bytes {
  using char_t = std::uint8_t;
  using string_type = std::basic_string<char_t>;

 public:
  enum State { GOOD, ERROR, NIL };

  Bytes(std::initializer_list<std::uint8_t> bytes) : data_(bytes.size(), 0) {
    // note - we want this to be like its would be used in redis where most significate bit is to
    // the "left"
    std::copy(rbegin(bytes), rend(bytes), data_.begin());
  }

  explicit Bytes(unsigned long long n) : data_(sizeof(n), 0) {
    FromNumber(n);
  }

  static Bytes From(unsigned long long x) {
    return Bytes(x);
  }

  explicit Bytes(State state) : state_{state} {
  }

  Bytes(const char_t* ch, std::size_t len) : data_(ch, len) {
  }

  Bytes(const char* ch, std::size_t len) : Bytes(reinterpret_cast<const char_t*>(ch), len) {
  }

  explicit Bytes(std::string_view from) : Bytes(from.data(), from.size()) {
  }

  static Bytes From(RespExpr&& r);

  std::size_t Size() const {
    return data_.size();
  }

  operator std::string_view() const {
    return std::string_view(reinterpret_cast<const char*>(data_.data()), Size());
  }

  std::ostream& Print(std::ostream& os) const;

  std::ostream& PrintHex(std::ostream& os) const;

 private:
  template <typename T> void FromNumber(T num) {
    // note - we want this to be like its would be used in redis where most significate bit is to
    // the "left"
    std::size_t i = 0;
    for (const char_t* s = reinterpret_cast<const char_t*>(&num); i < sizeof(T); s++, i++) {
      data_[i] = *s;
    }
  }

  string_type data_;
  State state_ = GOOD;
};

Bytes Bytes::From(RespExpr&& r) {
  if (r.type == RespExpr::STRING) {
    return Bytes(ToSV(r.GetBuf()));
  } else {
    if (r.type == RespExpr::NIL || r.type == RespExpr::NIL_ARRAY) {
      return Bytes{Bytes::NIL};
    } else {
      return Bytes(Bytes::ERROR);
    }
  }
}

std::ostream& Bytes::Print(std::ostream& os) const {
  if (state_ == GOOD) {
    for (auto c : data_) {
      std::bitset<8> b{c};
      os << b << ":";
    }
  } else {
    if (state_ == NIL) {
      os << "nil";
    } else {
      os << "error";
    }
  }
  return os;
}

std::ostream& Bytes::PrintHex(std::ostream& os) const {
  if (state_ == GOOD) {
    for (auto c : data_) {
      os << std::hex << std::setfill('0') << std::setw(2) << (std::uint16_t)c << ":";
    }
  } else {
    if (state_ == NIL) {
      os << "nil";
    } else {
      os << "error";
    }
  }
  return os;
}

inline bool operator==(const Bytes& left, const Bytes& right) {
  return static_cast<const std::string_view&>(left) == static_cast<const std::string_view&>(right);
}

inline bool operator!=(const Bytes& left, const Bytes& right) {
  return !(left == right);
}

inline Bytes operator"" _b(unsigned long long x) {
  return Bytes::From(x);
}

inline Bytes operator"" _b(const char* x, std::size_t s) {
  return Bytes{x, s};
}

inline Bytes operator"" _b(const char* x) {
  return Bytes{x, std::strlen(x)};
}

inline std::ostream& operator<<(std::ostream& os, const Bytes& bs) {
  return bs.PrintHex(os);
}

class BitOpsFamilyTest : public BaseFamilyTest {
 protected:
  // only for bitop XOR, OR, AND tests
  void BitOpSetKeys();
};

// for the bitop tests we need to test with multiple keys as the issue
// is that we need to make sure that accessing multiple shards creates
// the correct result
// Since this is bit operations, we are using the bytes data type
// that makes the verification more ergonomics.
const std::pair<std::string_view, Bytes> KEY_VALUES_BIT_OP[] = {
    {"first_key", 0xFFAACC01_b},
    {"key_second", {0x1, 0xBB}},
    {"_this_is_the_third_key", {0x01, 0x05, 0x15, 0x20, 0xAA, 0xCC}},
    {"the_last_key_we_have", 0xAACC_b}};

// For the bitop XOR OR and AND we are setting these keys/value pairs
void BitOpsFamilyTest::BitOpSetKeys() {
  auto resp = Run({"set", KEY_VALUES_BIT_OP[0].first, KEY_VALUES_BIT_OP[0].second});
  EXPECT_EQ(resp, "OK");
  resp = Run({"set", KEY_VALUES_BIT_OP[1].first, KEY_VALUES_BIT_OP[1].second});
  EXPECT_EQ(resp, "OK");
  resp = Run({"set", KEY_VALUES_BIT_OP[2].first, KEY_VALUES_BIT_OP[2].second});
  EXPECT_EQ(resp, "OK");
  resp = Run({"set", KEY_VALUES_BIT_OP[3].first, KEY_VALUES_BIT_OP[3].second});
  EXPECT_EQ(resp, "OK");
}

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

// ------------------------- BITOP tests

const auto EXPECTED_LEN_BITOP =
    std::max(KEY_VALUES_BIT_OP[0].second.Size(), KEY_VALUES_BIT_OP[1].second.Size());
const auto EXPECTED_LEN_BITOP2 = std::max(EXPECTED_LEN_BITOP, KEY_VALUES_BIT_OP[2].second.Size());
const auto EXPECTED_LEN_BITOP3 = std::max(EXPECTED_LEN_BITOP2, KEY_VALUES_BIT_OP[3].second.Size());

TEST_F(BitOpsFamilyTest, BitOpsAnd) {
  BitOpSetKeys();
  auto resp = Run({"bitop", "foo", "bar", "abc"});  // should failed this is illegal operation
  ASSERT_THAT(resp, ErrArg("syntax error"));
  // run with none existing keys, should return 0
  EXPECT_EQ(0, CheckedInt({"bitop", "and", "dest_key", "1", "2", "3"}));

  // bitop AND single key
  EXPECT_EQ(KEY_VALUES_BIT_OP[0].second.Size(),
            CheckedInt({"bitop", "and", "foo_out", KEY_VALUES_BIT_OP[0].first}));

  auto res = Bytes::From(Run({"get", "foo_out"}));
  EXPECT_EQ(res, KEY_VALUES_BIT_OP[0].second);

  // this will 0 all values other than one bit it would end with result with length ==
  //     FOO_KEY_VALUE && value == BAR_KEY_VALUE
  EXPECT_EQ(EXPECTED_LEN_BITOP, CheckedInt({"bitop", "and", "foo-out", KEY_VALUES_BIT_OP[0].first,
                                            KEY_VALUES_BIT_OP[1].first}));
  const auto EXPECTED_RESULT = Bytes((0xffaacc01 & 0x1BB));  // first and second values
  res = Bytes::From(Run({"get", "foo-out"}));
  EXPECT_EQ(res, EXPECTED_RESULT);

  // test bitop AND with 3 keys
  EXPECT_EQ(EXPECTED_LEN_BITOP2,
            CheckedInt({"bitop", "and", "foo-out", KEY_VALUES_BIT_OP[0].first,
                        KEY_VALUES_BIT_OP[1].first, KEY_VALUES_BIT_OP[2].first}));
  const auto EXPECTED_RES2 = Bytes((0xffaacc01 & 0x1BB & 0x01051520AACC));
  res = Bytes::From(Run({"get", "foo-out"}));
  EXPECT_EQ(EXPECTED_RES2, res);

  // test bitop AND with 4 parameters
  const auto EXPECTED_RES3 = Bytes((0xffaacc01 & 0x1BB & 0x01051520AACC & 0xAACC));
  EXPECT_EQ(EXPECTED_LEN_BITOP3, CheckedInt({"bitop", "and", "foo-out", KEY_VALUES_BIT_OP[0].first,
                                             KEY_VALUES_BIT_OP[1].first, KEY_VALUES_BIT_OP[2].first,
                                             KEY_VALUES_BIT_OP[3].first}));
  res = Bytes::From(Run({"get", "foo-out"}));
  EXPECT_EQ(EXPECTED_RES3, res);
}

TEST_F(BitOpsFamilyTest, BitOpsOr) {
  BitOpSetKeys();

  EXPECT_EQ(0, CheckedInt({"bitop", "or", "dest_key", "1", "2", "3"}));

  // bitop or single key
  EXPECT_EQ(KEY_VALUES_BIT_OP[0].second.Size(),
            CheckedInt({"bitop", "or", "foo_out", KEY_VALUES_BIT_OP[0].first}));

  auto res = Bytes::From(Run({"get", "foo_out"}));
  EXPECT_EQ(res, KEY_VALUES_BIT_OP[0].second);

  // bitop OR 2 keys
  EXPECT_EQ(EXPECTED_LEN_BITOP, CheckedInt({"bitop", "or", "foo-out", KEY_VALUES_BIT_OP[0].first,
                                            KEY_VALUES_BIT_OP[1].first}));
  const auto EXPECTED_RESULT = Bytes((0xffaacc01 | 0x1BB));  // first or second values
  res = Bytes::From(Run({"get", "foo-out"}));
  EXPECT_EQ(res, EXPECTED_RESULT);

  // bitop OR with 3 keys
  EXPECT_EQ(EXPECTED_LEN_BITOP2,
            CheckedInt({"bitop", "or", "foo-out", KEY_VALUES_BIT_OP[0].first,
                        KEY_VALUES_BIT_OP[1].first, KEY_VALUES_BIT_OP[2].first}));
  const auto EXPECTED_RES2 = Bytes((0xffaacc01 | 0x1BB | 0x01051520AACC));
  res = Bytes::From(Run({"get", "foo-out"}));
  EXPECT_EQ(EXPECTED_RES2, res);

  // bitop OR with 4 keys
  const auto EXPECTED_RES3 = Bytes((0xffaacc01 | 0x1BB | 0x01051520AACC | 0xAACC));
  EXPECT_EQ(EXPECTED_LEN_BITOP3, CheckedInt({"bitop", "or", "foo-out", KEY_VALUES_BIT_OP[0].first,
                                             KEY_VALUES_BIT_OP[1].first, KEY_VALUES_BIT_OP[2].first,
                                             KEY_VALUES_BIT_OP[3].first}));
  res = Bytes::From(Run({"get", "foo-out"}));
  EXPECT_EQ(EXPECTED_RES3, res);
}

TEST_F(BitOpsFamilyTest, BitOpsXor) {
  BitOpSetKeys();

  EXPECT_EQ(0, CheckedInt({"bitop", "or", "dest_key", "1", "2", "3"}));

  // bitop XOR on single key
  EXPECT_EQ(KEY_VALUES_BIT_OP[0].second.Size(),
            CheckedInt({"bitop", "xor", "foo_out", KEY_VALUES_BIT_OP[0].first}));
  auto res = Bytes::From(Run({"get", "foo_out"}));
  EXPECT_EQ(res, KEY_VALUES_BIT_OP[0].second);

  // bitop on XOR with two keys
  EXPECT_EQ(EXPECTED_LEN_BITOP, CheckedInt({"bitop", "xor", "foo-out", KEY_VALUES_BIT_OP[0].first,
                                            KEY_VALUES_BIT_OP[1].first}));
  const auto EXPECTED_RESULT = Bytes((0xffaacc01 ^ 0x1BB));  // first xor second values
  res = Bytes::From(Run({"get", "foo-out"}));
  EXPECT_EQ(res, EXPECTED_RESULT);

  // bitop XOR with 3 keys
  EXPECT_EQ(EXPECTED_LEN_BITOP2,
            CheckedInt({"bitop", "xor", "foo-out", KEY_VALUES_BIT_OP[0].first,
                        KEY_VALUES_BIT_OP[1].first, KEY_VALUES_BIT_OP[2].first}));
  const auto EXPECTED_RES2 = Bytes((0xffaacc01 ^ 0x1BB ^ 0x01051520AACC));
  res = Bytes::From(Run({"get", "foo-out"}));
  EXPECT_EQ(EXPECTED_RES2, res);

  // bitop XOR with 4 keys
  const auto EXPECTED_RES3 = Bytes((0xffaacc01 ^ 0x1BB ^ 0x01051520AACC ^ 0xAACC));
  EXPECT_EQ(EXPECTED_LEN_BITOP3, CheckedInt({"bitop", "xor", "foo-out", KEY_VALUES_BIT_OP[0].first,
                                             KEY_VALUES_BIT_OP[1].first, KEY_VALUES_BIT_OP[2].first,
                                             KEY_VALUES_BIT_OP[3].first}));
  res = Bytes::From(Run({"get", "foo-out"}));
  EXPECT_EQ(EXPECTED_RES3, res);
}

TEST_F(BitOpsFamilyTest, BitOpsNot) {
  // should failed this is illegal number of args
  auto resp = Run({"bitop", "not", "bar", "abc", "efg"});
  ASSERT_THAT(resp, ErrArg("syntax error"));

  // Make sure that this works with none existing key as well
  EXPECT_EQ(0, CheckedInt({"bitop", "NOT", "bit-op-not-none-existing-key-results",
                           "this-key-do-not-exists"}));
  EXPECT_EQ(Run({"get", "bit-op-not-none-existing-key-results"}), "");

  // test bitop not
  resp = Run({"set", KEY_VALUES_BIT_OP[0].first, KEY_VALUES_BIT_OP[0].second});
  EXPECT_EQ(KEY_VALUES_BIT_OP[0].second.Size(),
            CheckedInt({"bitop", "not", "foo_out", KEY_VALUES_BIT_OP[0].first}));
  auto res = Bytes::From(Run({"get", "foo_out"}));

  const auto NOT_RESULTS = Bytes(~0xFFAACC01ull);
  EXPECT_EQ(res, NOT_RESULTS);
}

}  // end of namespace dfly
