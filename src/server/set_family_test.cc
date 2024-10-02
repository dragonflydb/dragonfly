// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/set_family.h"

#include "base/gtest.h"
#include "base/logging.h"
#include "facade/facade_test.h"
#include "server/command_registry.h"
#include "server/test_utils.h"
extern "C" {
#include "redis/intset.h"
#include "redis/zmalloc.h"
}

using namespace testing;
using namespace std;
using namespace util;
using namespace boost;

namespace dfly {

class SetFamilyTest : public BaseFamilyTest {
 protected:
};

MATCHER_P(ConsistsOfMatcher, elements, "") {
  auto vec = arg.GetVec();
  for (const auto& x : vec) {
    if (elements.find(x.GetString()) == elements.end()) {
      return false;
    }
  }
  return true;
}

auto ConsistsOf(std::initializer_list<std::string> elements) {
  return ConsistsOfMatcher(std::unordered_set<std::string>{elements});
}

TEST_F(SetFamilyTest, SAdd) {
  auto resp = Run({"sadd", "x", "1", "2", "3"});
  EXPECT_THAT(resp, IntArg(3));
  resp = Run({"sadd", "x", "2", "3"});
  EXPECT_THAT(resp, IntArg(0));
  Run({"set", "a", "foo"});
  resp = Run({"sadd", "a", "b"});
  EXPECT_THAT(resp, ErrArg("WRONGTYPE "));
  resp = Run({"type", "x"});
  EXPECT_EQ(resp, "set");
}

TEST_F(SetFamilyTest, IntConv) {
  auto resp = Run({"sadd", "x", "134"});
  EXPECT_THAT(resp, IntArg(1));
  resp = Run({"sadd", "x", "abc"});
  EXPECT_THAT(resp, IntArg(1));
  resp = Run({"sadd", "x", "134"});
  EXPECT_THAT(resp, IntArg(0));
}

TEST_F(SetFamilyTest, SUnionStore) {
  auto resp = Run({"sadd", "b", "1", "2", "3"});
  Run({"sadd", "c", "10", "11"});
  Run({"set", "a", "foo"});
  resp = Run({"sunionstore", "a", "b", "c"});

  EXPECT_THAT(resp, IntArg(5));
  resp = Run({"type", "a"});
  ASSERT_EQ(resp, "set");

  resp = Run({"smembers", "a"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(resp.GetVec(), UnorderedElementsAre("11", "10", "1", "2", "3"));
}

TEST_F(SetFamilyTest, SDiff) {
  auto resp = Run({"sadd", "b", "1", "2", "3"});
  Run({"sadd", "c", "10", "11"});
  Run({"set", "a", "foo"});

  resp = Run({"sdiff", "b", "c"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(resp.GetVec(), UnorderedElementsAre("1", "2", "3"));

  resp = Run({"sdiffstore", "a", "b", "c"});
  EXPECT_THAT(resp, IntArg(3));

  Run({"set", "str", "foo"});
  EXPECT_THAT(Run({"sdiff", "b", "str"}), ErrArg("WRONGTYPE "));

  Run({"sadd", "bar", "x", "a", "b", "c"});
  Run({"sadd", "foo", "c"});
  Run({"sadd", "car", "a", "d"});
  EXPECT_EQ(2, CheckedInt({"SDIFFSTORE", "tar", "bar", "foo", "car"}));
}

TEST_F(SetFamilyTest, SInter) {
  auto resp = Run({"sadd", "a", "1", "2", "3", "4"});
  Run({"sadd", "b", "3", "5", "6", "2"});
  resp = Run({"sinterstore", "d", "a", "b"});
  EXPECT_THAT(resp, IntArg(2));
  resp = Run({"smembers", "d"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(resp.GetVec(), UnorderedElementsAre("3", "2"));

  Run({"set", "y", ""});
  resp = Run({"sinter", "x", "y"});
  ASSERT_EQ(1, GetDebugInfo("IO0").shards_count);
  EXPECT_THAT(resp, ErrArg("WRONGTYPE Operation against a key"));
  resp = Run({"sinterstore", "none1", "none2"});
  EXPECT_THAT(resp, IntArg(0));
}

TEST_F(SetFamilyTest, SInterCard) {
  Run({"sadd", "s1", "2", "b", "1", "a"});
  Run({"sadd", "s2", "3", "c", "2", "b"});
  Run({"sadd", "s3", "2", "b", "3", "c"});

  EXPECT_EQ(2, CheckedInt({"sintercard", "2", "s1", "s2"}));
  EXPECT_EQ(0, CheckedInt({"sintercard", "2", "s1", "s4"}));
  EXPECT_EQ(2, CheckedInt({"sintercard", "2", "s2", "s3", "LIMIT", "2"}));
  EXPECT_EQ(4, CheckedInt({"sintercard", "1", "s1"}));

  auto resp = Run({"sintercard", "a", "s1", "s2"});
  // redis does not throw this message, but SimpleAtoi does
  EXPECT_THAT(resp, ErrArg("value is not an integer or out of range"));
  resp = Run({"sintercard", "2", "s1", "s2", "LIMIT"});
  EXPECT_THAT(resp, ErrArg("syntax error"));
  resp = Run({"sintercard", "2", "s1", "s2", "LIMIT", "a"});
  EXPECT_THAT(resp, ErrArg("limit can't be negative"));
  resp = Run({"sintercard", "2", "s1", "s2", "LIMIT", "-1"});
  EXPECT_THAT(resp, ErrArg("limit can't be negative"));
  resp = Run({"sintercard", "2", "s1"});
  EXPECT_THAT(resp, ErrArg("syntax error"));
  resp = Run({"sintercard", "-1", "s1"});
  EXPECT_THAT(resp, ErrArg("value is not an integer or out of range"));
}

TEST_F(SetFamilyTest, SMove) {
  auto resp = Run({"sadd", "a", "1", "2", "3", "4"});
  Run({"sadd", "b", "3", "5", "6", "2"});
  resp = Run({"smove", "a", "b", "1"});
  EXPECT_THAT(resp, IntArg(1));

  Run({"sadd", "x", "a", "b", "c"});
  Run({"sadd", "y", "c"});
  EXPECT_THAT(Run({"smove", "x", "y", "c"}), IntArg(1));
}

TEST_F(SetFamilyTest, SPop) {
  auto resp = Run({"sadd", "x", "1", "2", "3"});
  resp = Run({"spop", "x", "3"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(resp.GetVec(), UnorderedElementsAre("1", "2", "3"));
  resp = Run({"type", "x"});
  EXPECT_EQ(resp, "none");

  Run({"sadd", "x", "1", "2", "3"});
  resp = Run({"spop", "x", "2"});

  ASSERT_THAT(resp, ArrLen(2));
  EXPECT_THAT(resp.GetVec(), IsSubsetOf({"1", "2", "3"}));

  resp = Run({"scard", "x"});
  EXPECT_THAT(resp, IntArg(1));

  Run({"sadd", "y", "a", "b", "c"});
  resp = Run({"spop", "y", "1"});
  EXPECT_THAT(resp, ArgType(RespExpr::STRING));
  EXPECT_THAT(resp, testing::AnyOf("a", "b", "c"));

  resp = Run({"smembers", "y"});
  ASSERT_THAT(resp, ArrLen(2));
  EXPECT_THAT(resp.GetVec(), IsSubsetOf({"a", "b", "c"}));
}

TEST_F(SetFamilyTest, SRandMember) {
  // Test IntSet
  Run({"sadd", "x", "1", "2", "3"});

  // Test if count > 0 (IntSet)
  auto resp = Run({"SRandMember", "x"});
  ASSERT_THAT(resp, ArgType(RespExpr::STRING));
  EXPECT_THAT(resp, AnyOf("1", "2", "3"));

  resp = Run({"SRandMember", "x", "1"});
  ASSERT_THAT(resp, ArgType(RespExpr::STRING));
  EXPECT_THAT(resp, AnyOf("1", "2", "3"));

  resp = Run({"SRandMember", "x", "2"});
  ASSERT_THAT(resp, ArrLen(2));
  EXPECT_THAT(resp.GetVec(), IsSubsetOf({"1", "2", "3"}));

  resp = Run({"SRandMember", "x", "3"});
  ASSERT_THAT(resp, ArrLen(3));
  EXPECT_THAT(resp.GetVec(), UnorderedElementsAre("1", "2", "3"));

  // Test if count is larger than the size of the IntSet
  resp = Run({"SRandMember", "x", "25"});
  ASSERT_THAT(resp, ArrLen(3));
  EXPECT_THAT(resp.GetVec(), UnorderedElementsAre("1", "2", "3"));

  // Test if count < 0 (IntSet)
  resp = Run({"SRandMember", "x", "-1"});
  ASSERT_THAT(resp, ArgType(RespExpr::STRING));
  EXPECT_THAT(resp, AnyOf("1", "2", "3"));

  resp = Run({"SRandMember", "x", "-2"});
  ASSERT_THAT(resp, ArrLen(2));
  EXPECT_THAT(resp, ConsistsOf({"1", "2", "3"}));

  resp = Run({"SRandMember", "x", "-3"});
  ASSERT_THAT(resp, ArrLen(3));
  EXPECT_THAT(resp, ConsistsOf({"1", "2", "3"}));

  // Test if count < 0, but the absolute value is larger than the size of the IntSet
  resp = Run({"SRandMember", "x", "-25"});
  ASSERT_THAT(resp, ArrLen(25));
  EXPECT_THAT(resp, ConsistsOf({"1", "2", "3"}));

  // Test StrSet
  Run({"sadd", "y", "a", "b", "c"});

  // Test if count > 0 (StrSet)
  resp = Run({"SRandMember", "y"});
  ASSERT_THAT(resp, ArgType(RespExpr::STRING));
  EXPECT_THAT(resp, AnyOf("a", "b", "c"));

  resp = Run({"SRandMember", "y", "1"});
  ASSERT_THAT(resp, ArgType(RespExpr::STRING));
  EXPECT_THAT(resp, AnyOf("a", "b", "c"));

  resp = Run({"SRandMember", "y", "2"});
  ASSERT_THAT(resp, ArrLen(2));
  EXPECT_THAT(resp.GetVec(), IsSubsetOf({"a", "b", "c"}));

  resp = Run({"SRandMember", "y", "3"});
  ASSERT_THAT(resp, ArrLen(3));
  EXPECT_THAT(resp.GetVec(), UnorderedElementsAre("a", "b", "c"));

  // Test if count is larger than the size of the StrSet
  resp = Run({"SRandMember", "y", "25"});
  ASSERT_THAT(resp, ArrLen(3));
  EXPECT_THAT(resp.GetVec(), UnorderedElementsAre("a", "b", "c"));

  // Test if count < 0 (StrSet)
  resp = Run({"SRandMember", "y", "-1"});
  ASSERT_THAT(resp, ArgType(RespExpr::STRING));
  EXPECT_THAT(resp, AnyOf("a", "b", "c"));

  resp = Run({"SRandMember", "y", "-2"});
  ASSERT_THAT(resp, ArrLen(2));
  EXPECT_THAT(resp, ConsistsOf({"a", "b", "c"}));

  resp = Run({"SRandMember", "y", "-3"});
  ASSERT_THAT(resp, ArrLen(3));
  EXPECT_THAT(resp, ConsistsOf({"a", "b", "c"}));

  // Test if count < 0, but the absolute value is larger than the size of the StrSet
  resp = Run({"SRandMember", "y", "-25"});
  ASSERT_THAT(resp, ArrLen(25));
  EXPECT_THAT(resp, ConsistsOf({"a", "b", "c"}));

  // Test if count is 0
  ASSERT_THAT(Run({"SRandMember", "x", "0"}), ArrLen(0));

  // Test if set is empty
  EXPECT_THAT(Run({"SAdd", "empty::set", "1"}), IntArg(1));
  EXPECT_THAT(Run({"SRem", "empty::set", "1"}), IntArg(1));
  ASSERT_THAT(Run({"SRandMember", "empty::set", "0"}), ArrLen(0));
  ASSERT_THAT(Run({"SRandMember", "empty::set", "3"}), ArrLen(0));
  ASSERT_THAT(Run({"SRandMember", "empty::set", "-4"}), ArrLen(0));

  // Test if key does not exist
  ASSERT_THAT(Run({"SRandMember", "unknown::set"}), ArgType(RespExpr::NIL));
  ASSERT_THAT(Run({"SRandMember", "unknown::set", "0"}), ArrLen(0));

  // Test wrong arguments
  resp = Run({"SRandMember", "x", "5", "3"});
  EXPECT_THAT(resp, ErrArg("wrong number of arguments"));
}

TEST_F(SetFamilyTest, SMIsMember) {
  Run({"sadd", "foo", "a"});
  Run({"sadd", "foo", "b"});

  auto resp = Run({"smismember", "foo"});
  EXPECT_THAT(resp, ErrArg("wrong number of arguments"));

  resp = Run({"smismember", "foo1", "a", "b"});
  EXPECT_THAT(resp, RespArray(ElementsAre(IntArg(0), IntArg(0))));

  resp = Run({"smismember", "foo", "a", "c"});
  EXPECT_THAT(resp, RespArray(ElementsAre(IntArg(1), IntArg(0))));

  resp = Run({"smismember", "foo", "a", "b"});
  EXPECT_THAT(resp, RespArray(ElementsAre(IntArg(1), IntArg(1))));

  resp = Run({"smismember", "foo", "d", "e"});
  EXPECT_THAT(resp, RespArray(ElementsAre(IntArg(0), IntArg(0))));

  resp = Run({"smismember", "foo", "b"});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"smismember", "foo", "x"});
  EXPECT_THAT(resp, IntArg(0));
}

TEST_F(SetFamilyTest, Empty) {
  auto resp = Run({"smembers", "x"});
  ASSERT_THAT(resp, ArrLen(0));
}

TEST_F(SetFamilyTest, SScan) {
  // Test for int set
  for (int i = 0; i < 15; i++) {
    Run({"sadd", "myintset", absl::StrCat(i)});
  }

  // Note that even though this limit by 4, it would return more because
  // all fields are on intlist
  auto resp = Run({"sscan", "myintset", "0", "count", "4"});
  auto vec = StrArray(resp.GetVec()[1]);
  EXPECT_THAT(vec.size(), 15);

  resp = Run({"sscan", "myintset", "0", "match", "1*"});
  vec = StrArray(resp.GetVec()[1]);
  EXPECT_THAT(vec, UnorderedElementsAre("1", "10", "11", "12", "13", "14"));

  // test string set
  for (int i = 0; i < 15; i++) {
    Run({"sadd", "mystrset", absl::StrCat("str-", i)});
  }

  resp = Run({"sscan", "mystrset", "0", "count", "5"});
  vec = StrArray(resp.GetVec()[1]);
  EXPECT_THAT(vec.size(), 5);

  resp = Run({"sscan", "mystrset", "0", "match", "str-1*"});
  vec = StrArray(resp.GetVec()[1]);
  EXPECT_THAT(vec, UnorderedElementsAre("str-1", "str-10", "str-11", "str-12", "str-13", "str-14"));

  resp = Run({"sscan", "mystrset", "0", "match", "str-1*", "count", "3"});
  vec = StrArray(resp.GetVec()[1]);
  EXPECT_THAT(vec, IsSubsetOf({"str-1", "str-10", "str-11", "str-12", "str-13", "str-14"}));
  EXPECT_EQ(vec.size(), 3);

  // nothing should match this
  resp = Run({"sscan", "mystrset", "0", "match", "1*"});
  vec = StrArray(resp.GetVec()[1]);
  EXPECT_THAT(vec.size(), 0);
}

TEST_F(SetFamilyTest, IntSetMemcpy) {
  // This logic is used in CompactObject::DefragIntSet
  intset* original = intsetNew();
  uint8_t success = 0;
  for (int i = 0; i < 250; ++i) {
    original = intsetAdd(original, i, &success);
    ASSERT_THAT(success, 1);
  }
  const size_t blob_len = intsetBlobLen(original);
  intset* replacement = (intset*)zmalloc(blob_len);
  memcpy(replacement, original, blob_len);

  ASSERT_THAT(original->encoding, replacement->encoding);
  ASSERT_THAT(original->length, replacement->length);

  for (int i = 0; i < 250; ++i) {
    int64_t value;
    ASSERT_THAT(intsetGet(replacement, i, &value), 1);
    ASSERT_THAT(value, i);
  }

  zfree(original);
  zfree(replacement);
}

}  // namespace dfly
