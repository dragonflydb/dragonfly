// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/bitops_family.h"

#include <bitset>
#include <iomanip>
#include <iostream>
#include <limits>
#include <string>
#include <string_view>

#include "absl/strings/str_cat.h"
#include "base/gtest.h"
#include "base/logging.h"
#include "facade/facade_test.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/engine_shard_set.h"
#include "server/error.h"
#include "server/test_utils.h"
#include "server/transaction.h"

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

TEST_F(BitOpsFamilyTest, SetBitIncorrectValues) {
  EXPECT_EQ(0, CheckedInt({"setbit", "foo", "0", "1"}));
  EXPECT_THAT(Run({"setbit", "foo", "1", "-1"}),
              ErrArg("ERR value is not an integer or out of range"));
  EXPECT_THAT(Run({"setbit", "foo", "2", "11"}),
              ErrArg("ERR value is not an integer or out of range"));
  EXPECT_THAT(Run({"setbit", "foo", "3", "a"}),
              ErrArg("ERR value is not an integer or out of range"));
  EXPECT_THAT(Run({"setbit", "foo", "4", "O"}),
              ErrArg("ERR value is not an integer or out of range"));
  EXPECT_EQ(1, CheckedInt({"getbit", "foo", "0"}));
  EXPECT_EQ(0, CheckedInt({"getbit", "foo", "1"}));
  EXPECT_EQ(0, CheckedInt({"getbit", "foo", "2"}));
  EXPECT_EQ(0, CheckedInt({"getbit", "foo", "3"}));
  EXPECT_EQ(0, CheckedInt({"getbit", "foo", "4"}));
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
  EXPECT_EQ(0, CheckedInt({"bitcount", "foo", "1", "0"}));    // illegal range
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
  ASSERT_THAT(Run({"get", "bit-op-not-none-existing-key-results"}), ArgType(RespExpr::Type::NIL));

  EXPECT_EQ(Run({"set", "foo", "bar"}), "OK");
  EXPECT_EQ(0, CheckedInt({"bitop", "NOT", "foo", "this-key-do-not-exists"}));
  ASSERT_THAT(Run({"get", "foo"}), ArgType(RespExpr::Type::NIL));

  // Change the type of foo. Bitops is similar to set command. It's a blind update.
  ASSERT_THAT(Run({"hset", "foo", "bar", "val"}), IntArg(1));
  EXPECT_EQ(0, CheckedInt({"bitop", "NOT", "foo", "this-key-do-not-exists"}));
  ASSERT_THAT(Run({"get", "foo"}), ArgType(RespExpr::Type::NIL));

  // test bitop not
  resp = Run({"set", KEY_VALUES_BIT_OP[0].first, KEY_VALUES_BIT_OP[0].second});
  EXPECT_EQ(KEY_VALUES_BIT_OP[0].second.Size(),
            CheckedInt({"bitop", "not", "foo_out", KEY_VALUES_BIT_OP[0].first}));
  auto res = Bytes::From(Run({"get", "foo_out"}));

  const auto NOT_RESULTS = Bytes(~0xFFAACC01ull);
  EXPECT_EQ(res, NOT_RESULTS);
}

TEST_F(BitOpsFamilyTest, BitPos) {
  ASSERT_EQ(Run({"set", "a", "\x00\x00\x06\xff\xf0"_b}), "OK");

  // Find clear bits
  EXPECT_EQ(0, CheckedInt({"bitpos", "a", "0"}));
  EXPECT_EQ(8, CheckedInt({"bitpos", "a", "0", "1"}));
  EXPECT_EQ(16, CheckedInt({"bitpos", "a", "0", "2"}));
  EXPECT_EQ(-1, CheckedInt({"bitpos", "a", "0", "100"}));
  EXPECT_EQ(-1, CheckedInt({"bitpos", "a", "0", "100", "103"}));
  EXPECT_EQ(-1, CheckedInt({"bitpos", "a", "0", "100", "0"}));
  EXPECT_EQ(0, CheckedInt({"bitpos", "a", "0", "0", "100"}));
  EXPECT_EQ(8, CheckedInt({"bitpos", "a", "0", "1", "100"}));
  EXPECT_EQ(0, CheckedInt({"bitpos", "a", "0", "0", "-3"}));
  EXPECT_EQ(8, CheckedInt({"bitpos", "a", "0", "1", "-2"}));
  EXPECT_EQ(36, CheckedInt({"bitpos", "a", "0", "3"}));
  EXPECT_EQ(36, CheckedInt({"bitpos", "a", "0", "4"}));
  EXPECT_EQ(36, CheckedInt({"bitpos", "a", "0", "-2"}));
  EXPECT_EQ(36, CheckedInt({"bitpos", "a", "0", "-2", "-1"}));
  EXPECT_EQ(36, CheckedInt({"bitpos", "a", "0", "-1"}));
  EXPECT_EQ(0, CheckedInt({"bitpos", "a", "0", "-100"}));

  // Find clear bits, explicitly mention "BYTE"
  EXPECT_EQ(-1, CheckedInt({"bitpos", "a", "0", "100", "103", "BYTE"}));
  EXPECT_EQ(-1, CheckedInt({"bitpos", "a", "0", "100", "0", "BYTE"}));
  EXPECT_EQ(0, CheckedInt({"bitpos", "a", "0", "0", "100", "BYTE"}));
  EXPECT_EQ(8, CheckedInt({"bitpos", "a", "0", "1", "100", "BYTE"}));
  EXPECT_EQ(0, CheckedInt({"bitpos", "a", "0", "0", "-3", "BYTE"}));
  EXPECT_EQ(8, CheckedInt({"bitpos", "a", "0", "1", "-2", "BYTE"}));
  EXPECT_EQ(36, CheckedInt({"bitpos", "a", "0", "-2", "-1", "BYTE"}));

  // Find clear bits using "BIT"
  EXPECT_EQ(-1, CheckedInt({"bitpos", "a", "0", "100", "103", "BIT"}));
  EXPECT_EQ(-1, CheckedInt({"bitpos", "a", "0", "100", "0", "BIT"}));
  EXPECT_EQ(0, CheckedInt({"bitpos", "a", "0", "0", "100", "BIT"}));
  EXPECT_EQ(1, CheckedInt({"bitpos", "a", "0", "1", "100", "BIT"}));
  EXPECT_EQ(2, CheckedInt({"bitpos", "a", "0", "2", "100", "BIT"}));
  EXPECT_EQ(16, CheckedInt({"bitpos", "a", "0", "16", "100", "BIT"}));
  EXPECT_EQ(23, CheckedInt({"bitpos", "a", "0", "21", "100", "BIT"}));
  EXPECT_EQ(36, CheckedInt({"bitpos", "a", "0", "24", "100", "BIT"}));
  EXPECT_EQ(0, CheckedInt({"bitpos", "a", "0", "0", "-3", "BIT"}));
  EXPECT_EQ(1, CheckedInt({"bitpos", "a", "0", "1", "-2", "BIT"}));
  EXPECT_EQ(38, CheckedInt({"bitpos", "a", "0", "-2", "-1", "BIT"}));

  // Find set bits
  EXPECT_EQ(21, CheckedInt({"bitpos", "a", "1"}));
  EXPECT_EQ(21, CheckedInt({"bitpos", "a", "1", "0"}));
  EXPECT_EQ(21, CheckedInt({"bitpos", "a", "1", "1"}));
  EXPECT_EQ(21, CheckedInt({"bitpos", "a", "1", "2"}));
  EXPECT_EQ(24, CheckedInt({"bitpos", "a", "1", "3"}));
  EXPECT_EQ(32, CheckedInt({"bitpos", "a", "1", "4"}));
  EXPECT_EQ(32, CheckedInt({"bitpos", "a", "1", "-1"}));
  EXPECT_EQ(24, CheckedInt({"bitpos", "a", "1", "-2"}));
  EXPECT_EQ(21, CheckedInt({"bitpos", "a", "1", "-3"}));
  EXPECT_EQ(21, CheckedInt({"bitpos", "a", "1", "-4"}));
  EXPECT_EQ(21, CheckedInt({"bitpos", "a", "1", "-5"}));
  EXPECT_EQ(21, CheckedInt({"bitpos", "a", "1", "-6"}));
  EXPECT_EQ(21, CheckedInt({"bitpos", "a", "1", "-100"}));
  EXPECT_EQ(-1, CheckedInt({"bitpos", "a", "1", "0", "0"}));
  EXPECT_EQ(-1, CheckedInt({"bitpos", "a", "1", "0", "1"}));
  EXPECT_EQ(21, CheckedInt({"bitpos", "a", "1", "0", "3"}));
  EXPECT_EQ(21, CheckedInt({"bitpos", "a", "1", "0", "100"}));
  EXPECT_EQ(21, CheckedInt({"bitpos", "a", "1", "2", "2"}));
  EXPECT_EQ(21, CheckedInt({"bitpos", "a", "1", "2", "3"}));
  EXPECT_EQ(32, CheckedInt({"bitpos", "a", "1", "-1", "-1"}));
  EXPECT_EQ(24, CheckedInt({"bitpos", "a", "1", "-2", "-1"}));
  EXPECT_EQ(-1, CheckedInt({"bitpos", "a", "1", "-1", "-2"}));

  // Find set bits, explicitly mention "BYTE"
  EXPECT_EQ(-1, CheckedInt({"bitpos", "a", "1", "0", "0", "BYTE"}));
  EXPECT_EQ(-1, CheckedInt({"bitpos", "a", "1", "0", "1", "BYTE"}));
  EXPECT_EQ(21, CheckedInt({"bitpos", "a", "1", "0", "3", "BYTE"}));
  EXPECT_EQ(21, CheckedInt({"bitpos", "a", "1", "0", "100", "BYTE"}));
  EXPECT_EQ(21, CheckedInt({"bitpos", "a", "1", "2", "2", "BYTE"}));
  EXPECT_EQ(21, CheckedInt({"bitpos", "a", "1", "2", "3", "BYTE"}));
  EXPECT_EQ(32, CheckedInt({"bitpos", "a", "1", "-1", "-1", "BYTE"}));
  EXPECT_EQ(24, CheckedInt({"bitpos", "a", "1", "-2", "-1", "BYTE"}));
  EXPECT_EQ(-1, CheckedInt({"bitpos", "a", "1", "-1", "-2", "BYTE"}));

  // Find set bits using "BIT"
  EXPECT_EQ(-1, CheckedInt({"bitpos", "a", "1", "0", "0", "BIT"}));
  EXPECT_EQ(-1, CheckedInt({"bitpos", "a", "1", "0", "1", "BIT"}));
  EXPECT_EQ(21, CheckedInt({"bitpos", "a", "1", "0", "21", "BIT"}));
  EXPECT_EQ(21, CheckedInt({"bitpos", "a", "1", "21", "21", "BIT"}));
  EXPECT_EQ(21, CheckedInt({"bitpos", "a", "1", "21", "100", "BIT"}));
  EXPECT_EQ(21, CheckedInt({"bitpos", "a", "1", "0", "100", "BIT"}));
  EXPECT_EQ(-1, CheckedInt({"bitpos", "a", "1", "-1", "-1", "BIT"}));
  EXPECT_EQ(-1, CheckedInt({"bitpos", "a", "1", "-4", "-1", "BIT"}));
  EXPECT_EQ(35, CheckedInt({"bitpos", "a", "1", "-5", "-1", "BIT"}));
  EXPECT_EQ(34, CheckedInt({"bitpos", "a", "1", "-6", "-1", "BIT"}));

  // Make sure we behave like Redis does when looking for clear bits in an all-set string.
  ASSERT_EQ(Run({"set", "b", "\xff\xff\xff"_b}), "OK");
  EXPECT_EQ(24, CheckedInt({"bitpos", "b", "0"}));
  EXPECT_EQ(24, CheckedInt({"bitpos", "b", "0", "0"}));
  EXPECT_EQ(24, CheckedInt({"bitpos", "b", "0", "1"}));
  EXPECT_EQ(24, CheckedInt({"bitpos", "b", "0", "2"}));
  EXPECT_EQ(-1, CheckedInt({"bitpos", "b", "0", "3"}));
  EXPECT_EQ(-1, CheckedInt({"bitpos", "b", "0", "0", "1"}));
  EXPECT_EQ(-1, CheckedInt({"bitpos", "b", "0", "0", "1", "BYTE"}));
  EXPECT_EQ(-1, CheckedInt({"bitpos", "b", "0", "0", "3"}));
  EXPECT_EQ(-1, CheckedInt({"bitpos", "b", "0", "0", "3", "BYTE"}));

  ASSERT_EQ(Run({"set", "empty", ""_b}), "OK");
  EXPECT_EQ(-1, CheckedInt({"bitpos", "empty", "0"}));
  EXPECT_EQ(-1, CheckedInt({"bitpos", "empty", "0", "1"}));

  // Non-existent key should be treated like padded with zeros string.
  EXPECT_EQ(-1, CheckedInt({"bitpos", "d", "1"}));
  EXPECT_EQ(0, CheckedInt({"bitpos", "d", "0"}));

  // Make sure we accept only 0 and 1 for the bit mode arguement.
  const auto argument_must_be_0_or_1_error = ErrArg("ERR The bit argument must be 1 or 0");
  ASSERT_THAT(Run({"bitpos", "d", "2"}), argument_must_be_0_or_1_error);
  ASSERT_THAT(Run({"bitpos", "d", "42"}), argument_must_be_0_or_1_error);
  ASSERT_THAT(Run({"bitpos", "d", "-1"}), argument_must_be_0_or_1_error);
}

TEST_F(BitOpsFamilyTest, BitFieldParsing) {
  const auto syntax_error = ErrArg("ERR syntax error");
  // Parsing Errors
  ASSERT_THAT(Run({"bitfield", "foo", "set", "u1"}), syntax_error);
  ASSERT_THAT(Run({"bitfield", "foo", "set", "u1", "0"}), syntax_error);
  ASSERT_THAT(Run({"bitfield", "foo", "set", "u1", "0", "0", "55"}), syntax_error);
  ASSERT_THAT(Run({"bitfield", "foo", "set", "u1", "0", "0", "get", "u1"}), syntax_error);
  ASSERT_THAT(Run({"bitfield", "foo", "incrby", "u1"}), syntax_error);
  ASSERT_THAT(Run({"bitfield", "foo", "incrby", "u1", "0"}), syntax_error);
  ASSERT_THAT(Run({"bitfield", "foo", "get", "u1", "0", "15"}), syntax_error);
  ASSERT_THAT(Run({"bitfield", "foo", "get"}), syntax_error);
  ASSERT_THAT(Run({"bitfield", "foo", "set", "u1", "0", "0", "set"}), syntax_error);
  ASSERT_THAT(Run({"bitfield", "foo", "overflow"}), syntax_error);
  ASSERT_THAT(Run({"bitfield", "foo", "overflow", "nonsense"}), syntax_error);

  // Range errors
  auto expected_error = ErrArg(
      "ERR invalid bitfield type. use something like i16 u8. note that u64 is not supported but "
      "i64 is.");

  ASSERT_THAT(Run({"bitfield", "foo", "set", "u0", "0", "0"}), expected_error);
  ASSERT_THAT(Run({"bitfield", "foo", "set", "u0", "0", "0"}), expected_error);
  ASSERT_THAT(Run({"bitfield", "foo", "set", "u64", "0", "0"}), expected_error);
  ASSERT_THAT(Run({"bitfield", "foo", "set", "u65", "0", "0"}), expected_error);
  ASSERT_THAT(Run({"bitfield", "foo", "set", "i65", "0", "0"}), expected_error);

  expected_error = ErrArg("BITFIELD_RO only supports the GET subcommand");
  ASSERT_THAT(Run({"bitfield_ro", "foo", "set", "u1", "0", "0"}), expected_error);
  ASSERT_THAT(Run({"bitfield_ro", "foo", "incrby", "i64", "0", "15"}), expected_error);
}

TEST_F(BitOpsFamilyTest, BitFieldCreate) {
  // check that SET, INCR create the key when it does not exist
  ASSERT_THAT(Run({"bitfield", "foo", "set", "u1", "0", "1"}), IntArg(0));
  ASSERT_THAT(Run({"bitfield", "foo", "get", "u1", "0"}), IntArg(1));
  ASSERT_THAT(Run({"bitfield", "foo", "incrby", "u1", "1", "1"}), IntArg(1));
  ASSERT_THAT(Run({"bitfield", "foo", "get", "u1", "1"}), IntArg(1));
}

TEST_F(BitOpsFamilyTest, BitFieldOverflowUnderflow) {
  Run({"bitfield", "foo", "set", "u2", "0", "2"});

  // unsigned 1bit
  ASSERT_THAT(Run({"bitfield", "foo", "set", "u1", "0", "2"}), IntArg(1));
  ASSERT_THAT(Run({"bitfield", "foo", "get", "u1", "0"}), IntArg(0));
  ASSERT_THAT(Run({"bitfield", "foo", "incrby", "u1", "1", "2"}), IntArg(0));
  ASSERT_THAT(Run({"bitfield", "foo", "get", "u1", "1"}), IntArg(0));

  // unsigned 63bit
  int64_t max = std::numeric_limits<int64_t>::max();
  Run({"bitfield", "foo", "set", "i64", "0", absl::StrCat(max)});
  ASSERT_THAT(Run({"bitfield", "foo", "incrby", "i64", "0", "1"}), IntArg(-max - 1));

  // signed 1 bit
  Run({"bitfield", "foo", "set", "i1", "0", "-2"});
  ASSERT_THAT(Run({"bitfield", "foo", "get", "i1", "0"}), IntArg(0));
  ASSERT_THAT(Run({"bitfield", "foo", "incrby", "i1", "0", "-1"}), IntArg(-1));
  ASSERT_THAT(Run({"bitfield", "foo", "incrby", "i1", "0", "-1"}), IntArg(0));
  ASSERT_THAT(Run({"bitfield", "foo", "incrby", "i1", "0", "-3"}), IntArg(-1));

  int64_t min = std::numeric_limits<int64_t>::min();
  Run({"bitfield", "foo", "set", "i8", "0", absl::StrCat(min)});
  ASSERT_THAT(Run({"bitfield", "foo", "get", "i8", "0"}), IntArg(0));

  // signed 64 bit
  Run({"bitfield", "foo", "set", "i64", "0", absl::StrCat(min)});
  ASSERT_THAT(Run({"bitfield", "foo", "incrby", "i64", "0", "-1"}), IntArg(max));

  // overflow sat
  // unsigned 8 bit
  Run({"bitfield", "foo", "set", "u1", "0", "0"});
  ASSERT_THAT(Run({"bitfield", "foo", "overflow", "sat", "incrby", "u8", "0", "300"}), IntArg(255));
  ASSERT_THAT(Run({"bitfield", "foo", "overflow", "sat", "incrby", "u8", "0", "10"}), IntArg(255));
  ASSERT_THAT(Run({"bitfield", "foo", "get", "u8", "0"}), IntArg(255));

  // unsigned 63 bit
  Run({"bitfield", "foo", "set", "u63", "0", "0"});
  ASSERT_THAT(Run({"bitfield", "foo", "overflow", "sat", "set", "u63", "0", absl::StrCat(max)}),
              IntArg(0));
  ASSERT_THAT(Run({"bitfield", "foo", "overflow", "sat", "incrby", "u63", "0", "10"}), IntArg(max));

  // signed 8 bit
  Run({"bitfield", "foo", "set", "u8", "0", "0"});
  ASSERT_THAT(Run({"bitfield", "foo", "overflow", "sat", "set", "i8", "0", "300"}), IntArg(0));
  ASSERT_THAT(Run({"bitfield", "foo", "overflow", "sat", "incrby", "i8", "0", "-127"}), IntArg(0));
  ASSERT_THAT(Run({"bitfield", "foo", "overflow", "sat", "incrby", "i8", "0", "-255"}),
              IntArg(-128));

  // signed 64 bit
  Run({"bitfield", "foo", "set", "i64", "0", "0"});
  ASSERT_THAT(Run({"bitfield", "foo", "overflow", "sat", "set", "i64", "0", absl::StrCat(max)}),
              IntArg(0));
  ASSERT_THAT(Run({"bitfield", "foo", "overflow", "sat", "incrby", "i64", "0", "100"}),
              IntArg(max));
  ASSERT_THAT(Run({"bitfield", "foo", "get", "i64", "0"}), IntArg(max));
  ASSERT_THAT(Run({"bitfield", "foo", "overflow", "sat", "set", "i64", "0", absl::StrCat(min)}),
              IntArg(max));
  ASSERT_THAT(Run({"bitfield", "foo", "overflow", "sat", "incrby", "i64", "0", "-100"}),
              IntArg(min));

  // overflow fail
  // unsigned
  ASSERT_THAT(Run({"bitfield", "foo", "overflow", "fail", "set", "u8", "0", "300"}),
              ArgType(RespExpr::Type::NIL));
  ASSERT_THAT(Run({"bitfield", "foo", "overflow", "fail", "incrby", "u1", "0", "10"}),
              ArgType(RespExpr::Type::NIL));
  ASSERT_THAT(Run({"bitfield", "foo", "overflow", "fail", "incrby", "u1", "0", "-10"}),
              ArgType(RespExpr::Type::NIL));

  // signed
  ASSERT_THAT(Run({"bitfield", "foo", "overflow", "fail", "incrby", "i8", "0", "300"}),
              ArgType(RespExpr::Type::NIL));
  ASSERT_THAT(Run({"bitfield", "foo", "overflow", "fail", "incrby", "i1", "0", "10"}),
              ArgType(RespExpr::Type::NIL));
  ASSERT_THAT(Run({"bitfield", "foo", "overflow", "fail", "incrby", "i1", "0", "-10"}),
              ArgType(RespExpr::Type::NIL));

  // stickiness of overflow among operations in a chain
  ASSERT_THAT(Run({"bitfield", "foo", "overflow", "fail", "set", "u8", "0", "300", "set", "u1", "0",
                   "400"}),
              RespArray(ElementsAre(ArgType(RespExpr::NIL), ArgType(RespExpr::NIL))));
}

TEST_F(BitOpsFamilyTest, BitFieldOperations) {
  // alligned offset reads/writes unsigned
  Run({"bitfield", "foo", "set", "u32", "0", "0"});
  // Set the bit battern 01111000 00000001 00000001 00001010
  ASSERT_THAT(Run({"bitfield", "foo", "set", "u8", "0", "120"}), IntArg(0));
  ASSERT_THAT(Run({"bitfield", "foo", "get", "u8", "0"}), IntArg(120));

  ASSERT_THAT(Run({"bitfield", "foo", "set", "u8", "8", "1"}), IntArg(0));
  ASSERT_THAT(Run({"bitfield", "foo", "get", "u8", "8"}), IntArg(1));

  ASSERT_THAT(Run({"bitfield", "foo", "set", "u8", "16", "1"}), IntArg(0));
  ASSERT_THAT(Run({"bitfield", "foo", "get", "u8", "16"}), IntArg(1));

  ASSERT_THAT(Run({"bitfield", "foo", "set", "u8", "24", "10"}), IntArg(0));
  ASSERT_THAT(Run({"bitfield", "foo", "get", "u8", "24"}), IntArg(10));

  ASSERT_THAT(Run({"bitfield", "foo", "get", "u32", "0"}), IntArg(2013331722));

  ASSERT_THAT(Run({"bitfield", "foo", "incrby", "u8", "0", "120"}), IntArg(240));
  ASSERT_THAT(Run({"bitfield", "foo", "get", "u8", "0"}), IntArg(240));

  ASSERT_THAT(Run({"bitfield", "foo", "incrby", "u16", "0", "120"}), IntArg(61561));
  ASSERT_THAT(Run({"bitfield", "foo", "get", "u16", "0"}), IntArg(61561));

  // alligned offset reads/writes signed
  Run({"bitfield", "foo", "set", "u32", "0", "0"});
  // Set the bit battern 10001000 11111111 11111111 11110110
  ASSERT_THAT(Run({"bitfield", "foo", "set", "i8", "0", "-120"}), IntArg(0));
  ASSERT_THAT(Run({"bitfield", "foo", "get", "i8", "0"}), IntArg(-120));

  ASSERT_THAT(Run({"bitfield", "foo", "set", "i8", "8", "-1"}), IntArg(0));
  ASSERT_THAT(Run({"bitfield", "foo", "get", "i8", "8"}), IntArg(-1));

  ASSERT_THAT(Run({"bitfield", "foo", "set", "i8", "16", "-1"}), IntArg(0));
  ASSERT_THAT(Run({"bitfield", "foo", "get", "i8", "16"}), IntArg(-1));

  ASSERT_THAT(Run({"bitfield", "foo", "set", "i8", "24", "-10"}), IntArg(0));
  ASSERT_THAT(Run({"bitfield", "foo", "get", "i8", "24"}), IntArg(-10));

  ASSERT_THAT(Run({"bitfield", "foo", "get", "i32", "0"}), IntArg(-1996488714));

  ASSERT_THAT(Run({"bitfield", "foo", "incrby", "i8", "0", "-8"}), IntArg(-128));

  // nonalligned offset reads/writes unsigned
  Run({"bitfield", "foo", "set", "i64", "0", "0"});
  // Set the bit battern 00000000 10000000 10000000 10000000 10000000
  ASSERT_THAT(Run({"bitfield", "foo", "set", "u8", "1", "1"}), IntArg(0));
  ASSERT_THAT(Run({"bitfield", "foo", "get", "u8", "1"}), IntArg(1));

  ASSERT_THAT(Run({"bitfield", "foo", "set", "u8", "9", "1"}), IntArg(0));
  ASSERT_THAT(Run({"bitfield", "foo", "get", "u8", "9"}), IntArg(1));

  ASSERT_THAT(Run({"bitfield", "foo", "set", "u8", "17", "1"}), IntArg(0));
  ASSERT_THAT(Run({"bitfield", "foo", "get", "u8", "17"}), IntArg(1));

  ASSERT_THAT(Run({"bitfield", "foo", "set", "u8", "25", "1"}), IntArg(0));
  ASSERT_THAT(Run({"bitfield", "foo", "get", "u8", "25"}), IntArg(1));

  ASSERT_THAT(Run({"bitfield", "foo", "get", "u8", "0"}), IntArg(0));
  ASSERT_THAT(Run({"bitfield", "foo", "get", "u1", "8"}), IntArg(1));
  ASSERT_THAT(Run({"bitfield", "foo", "get", "u1", "16"}), IntArg(1));
  ASSERT_THAT(Run({"bitfield", "foo", "get", "u1", "24"}), IntArg(1));
  ASSERT_THAT(Run({"bitfield", "foo", "get", "u1", "32"}), IntArg(1));

  ASSERT_THAT(Run({"bitfield", "foo", "get", "u33", "0"}), IntArg(16843009));

  // nonalligned offset reads/writes signed
  Run({"bitfield", "foo", "set", "i64", "0", "0"});
  // Set the bit battern 1111111 11111111 0000000 000000001
  ASSERT_THAT(Run({"bitfield", "foo", "set", "i8", "1", "-1"}), IntArg(0));
  ASSERT_THAT(Run({"bitfield", "foo", "get", "i8", "1"}), IntArg(-1));

  ASSERT_THAT(Run({"bitfield", "foo", "set", "i8", "9", "-1"}), IntArg(0));
  ASSERT_THAT(Run({"bitfield", "foo", "get", "i8", "9"}), IntArg(-1));

  ASSERT_THAT(Run({"bitfield", "foo", "set", "i8", "17", "0"}), IntArg(0));
  ASSERT_THAT(Run({"bitfield", "foo", "get", "i8", "17"}), IntArg(0));

  ASSERT_THAT(Run({"bitfield", "foo", "set", "i8", "25", "1"}), IntArg(0));
  ASSERT_THAT(Run({"bitfield", "foo", "get", "i8", "25"}), IntArg(1));

  ASSERT_THAT(Run({"bitfield", "foo", "get", "i32", "1"}), IntArg(-65535));

  // chaining
  Run({
      "bitfield", "foo", "set", "u1", "0", "1", "set", "u1", "1", "1", "set", "u1",
      "2",        "1",   "set", "u1", "3", "1", "set", "u1", "4", "1", "set", "u1",
      "5",        "1",   "set", "u1", "6", "1", "set", "u1", "7", "1",
  });

  ASSERT_THAT(Run({"bitfield", "foo", "get", "u8", "0"}), IntArg(255));

  ASSERT_THAT(Run({
                  "bitfield",
                  "foo",
                  "set",
                  "u1",
                  "0",
                  "0",
                  "incrby",
                  "u1",
                  "0",
                  "1",
                  "get",
                  "u1",
                  "0",
              }),
              RespArray(ElementsAre(IntArg(1), IntArg(1), IntArg(1))));

  // check for positional offsets
  Run({"bitfield", "foo", "set", "u8", "#0", "1", "set", "u8", "#1", "1", "set", "u8", "#2", "1"});

  ASSERT_THAT(Run({"bitfield", "foo", "get", "u1", "7"}), IntArg(1));
  ASSERT_THAT(Run({"bitfield", "foo", "get", "u1", "15"}), IntArg(1));
}

}  // end of namespace dfly
