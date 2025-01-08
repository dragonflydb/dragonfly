// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/zset_family.h"

#include "base/gtest.h"
#include "base/logging.h"
#include "facade/facade_test.h"
#include "server/command_registry.h"
#include "server/test_utils.h"

using namespace testing;
using namespace std;
using namespace util;

namespace dfly {

class ZSetFamilyTest : public BaseFamilyTest {
 protected:
};

using ScoredElement = std::pair<std::string, std::string>;

template <typename Array> auto ParseToScoredArray(Array arr) {
  std::vector<ScoredElement> scored_elements;
  for (std::size_t i = 1; i < arr.size(); i += 2) {
    scored_elements.emplace_back(arr[i - 1].GetString(), arr[i].GetString());
  }
  return scored_elements;
}

MATCHER_P(ConsistsOfMatcher, elements, "") {
  auto vec = arg.GetVec();
  for (const auto& x : vec) {
    if (elements.find(x.GetString()) == elements.end()) {
      return false;
    }
  }
  return true;
}

MATCHER_P(ConsistsOfScoredElementsMatcher, elements, "") {
  auto vec = arg.GetVec();
  if (vec.size() % 2) {
    return false;
  }

  auto scored_vec = ParseToScoredArray(vec);
  for (const auto& scored_element : scored_vec) {
    if (elements.find(scored_element) == elements.end()) {
      return false;
    }
  }
  return true;
}

MATCHER_P(IsScoredSubsetOfMatcher, elements_list, "") {
  auto vec = arg.GetVec();
  if (vec.size() % 2) {
    return false;
  }

  auto scored_vec = ParseToScoredArray(vec);
  std::vector<ScoredElement> elements{elements_list};

  std::sort(scored_vec.begin(), scored_vec.end());
  std::sort(elements.begin(), elements.end());

  return std::includes(elements.begin(), elements.end(), scored_vec.begin(), scored_vec.end());
}

MATCHER_P(UnorderedScoredElementsAreMatcher, elements_list, "") {
  auto vec = arg.GetVec();
  if (vec.size() % 2) {
    return false;
  }

  auto scored_vec = ParseToScoredArray(vec);
  return std::is_permutation(scored_vec.begin(), scored_vec.end(), elements_list.begin(),
                             elements_list.end());
}

MATCHER_P2(ContainsLabeledScoredArrayMatcher, label, elements, "") {
  auto label_vec = arg.GetVec();
  if (label_vec.size() != 2) {
    *result_listener << "Labeled Scored Array does no contain two elements.";
    return false;
  }

  if (!ExplainMatchResult(Eq(label), label_vec[0].GetString(), result_listener)) {
    return false;
  }

  auto value_pairs_vec = label_vec[1].GetVec();
  std::set<std::pair<std::string, std::string>> actual_elements;
  for (const auto& scored_element : value_pairs_vec) {
    actual_elements.insert(std::make_pair(scored_element.GetVec()[0].GetString(),
                                          scored_element.GetVec()[1].GetString()));
  }
  if (actual_elements != elements) {
    *result_listener << "Scored elements do not match: ";
    ExplainMatchResult(ElementsAreArray(elements), actual_elements, result_listener);
    return false;
  }

  return true;
}

auto ConsistsOf(std::initializer_list<std::string> elements) {
  return ConsistsOfMatcher(std::unordered_set<std::string>{elements});
}

auto ConsistsOfScoredElements(std::initializer_list<std::pair<std::string, std::string>> elements) {
  return ConsistsOfScoredElementsMatcher(std::set<std::pair<std::string, std::string>>{elements});
}

auto IsScoredSubsetOf(std::initializer_list<std::pair<std::string, std::string>> elements) {
  return IsScoredSubsetOfMatcher(elements);
}

auto UnorderedScoredElementsAre(
    std::initializer_list<std::pair<std::string, std::string>> elements) {
  return UnorderedScoredElementsAreMatcher(elements);
}

auto ContainsLabeledScoredArray(
    std::string_view label, std::initializer_list<std::pair<std::string, std::string>> elements) {
  return ContainsLabeledScoredArrayMatcher(label,
                                           std::set<std::pair<std::string, std::string>>{elements});
}

TEST_F(ZSetFamilyTest, Add) {
  auto resp = Run({"zadd", "x", "1.1", "a"});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"zscore", "x", "a"});
  EXPECT_THAT(resp, "1.1");

  resp = Run({"zadd", "x", "2", "a"});
  EXPECT_THAT(resp, IntArg(0));
  resp = Run({"zscore", "x", "a"});
  EXPECT_THAT(resp, "2");

  resp = Run({"zadd", "x", "ch", "3", "a"});
  EXPECT_THAT(resp, IntArg(1));
  resp = Run({"zscore", "x", "a"});
  EXPECT_EQ(resp, "3");

  resp = Run({"zcard", "x"});
  EXPECT_THAT(resp, IntArg(1));

  EXPECT_THAT(Run({"zadd", "x", "", "a"}), ErrArg("not a valid float"));

  EXPECT_THAT(Run({"zadd", "ztmp", "xx", "10", "member"}), IntArg(0));

  const char kHighPrecision[] = "0.79028573343077946";

  Run({"zadd", "zs", kHighPrecision, "a"});
  EXPECT_EQ(Run({"zscore", "zs", "a"}), "0.7902857334307795");
  EXPECT_EQ(0.79028573343077946, 0.7902857334307795);
}

TEST_F(ZSetFamilyTest, AddNonUniqeMembers) {
  auto resp = Run({"zadd", "x", "2", "a", "1", "a"});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"zscore", "x", "a"});
  EXPECT_EQ(resp, "1");

  resp = Run({"zadd", "y", "3", "a", "1", "a", "2", "b"});
  EXPECT_THAT(resp, IntArg(2));
  EXPECT_EQ("1", Run({"zscore", "y", "a"}));
}

TEST_F(ZSetFamilyTest, ZRem) {
  auto resp = Run({"zadd", "x", "1.1", "b", "2.1", "a"});
  EXPECT_THAT(resp, IntArg(2));

  resp = Run({"zrem", "x", "b", "c"});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"zcard", "x"});
  EXPECT_THAT(resp, IntArg(1));
  EXPECT_THAT(Run({"zrange", "x", "0", "3", "byscore"}), "a");
  EXPECT_THAT(Run({"zrange", "x", "(-inf", "(+inf", "byscore"}), "a");
}

TEST_F(ZSetFamilyTest, ZRandMember) {
  auto resp = Run({"ZAdd", "x", "1", "a", "2", "b", "3", "c"});
  EXPECT_THAT(resp, IntArg(3));

  // Test if count > 0
  resp = Run({"ZRandMember", "x"});
  ASSERT_THAT(resp, ArgType(RespExpr::STRING));
  EXPECT_THAT(resp, AnyOf("a", "b", "c"));

  resp = Run({"ZRandMember", "x", "1"});
  ASSERT_THAT(resp, ArgType(RespExpr::STRING));
  EXPECT_THAT(resp, AnyOf("a", "b", "c"));

  resp = Run({"ZRandMember", "x", "2"});
  ASSERT_THAT(resp, ArrLen(2));
  EXPECT_THAT(resp.GetVec(), IsSubsetOf({"a", "b", "c"}));

  resp = Run({"ZRandMember", "x", "3"});
  ASSERT_THAT(resp, ArrLen(3));
  EXPECT_THAT(resp.GetVec(), UnorderedElementsAre("a", "b", "c"));

  // Test if count < 0
  resp = Run({"ZRandMember", "x", "-1"});
  ASSERT_THAT(resp, ArgType(RespExpr::STRING));
  EXPECT_THAT(resp, AnyOf("a", "b", "c"));

  resp = Run({"ZRandMember", "x", "-2"});
  ASSERT_THAT(resp, ArrLen(2));
  EXPECT_THAT(resp, ConsistsOf({"a", "b", "c"}));

  resp = Run({"ZRandMember", "x", "-3"});
  ASSERT_THAT(resp, ArrLen(3));
  EXPECT_THAT(resp, ConsistsOf({"a", "b", "c"}));

  // Test if count < 0, but the absolute value is larger than the size of the sorted set
  resp = Run({"ZRandMember", "x", "-15"});
  ASSERT_THAT(resp, ArrLen(15));
  EXPECT_THAT(resp, ConsistsOf({"a", "b", "c"}));

  // Test if count is 0
  ASSERT_THAT(Run({"ZRandMember", "x", "0"}), ArrLen(0));

  // Test if count is larger than the size of the sorted set
  resp = Run({"ZRandMember", "x", "15"});
  ASSERT_THAT(resp, ArrLen(3));
  EXPECT_THAT(resp.GetVec(), UnorderedElementsAre("a", "b", "c"));

  // Test if sorted set is empty
  EXPECT_THAT(Run({"ZAdd", "empty::zset", "1", "one"}), IntArg(1));
  EXPECT_THAT(Run({"ZRem", "empty::zset", "one"}), IntArg(1));
  ASSERT_THAT(Run({"ZRandMember", "empty::zset", "0"}), ArrLen(0));
  ASSERT_THAT(Run({"ZRandMember", "empty::zset", "3"}), ArrLen(0));
  ASSERT_THAT(Run({"ZRandMember", "empty::zset", "-4"}), ArrLen(0));

  // Test if key does not exist
  ASSERT_THAT(Run({"ZRandMember", "y"}), ArgType(RespExpr::NIL));
  ASSERT_THAT(Run({"ZRandMember", "y", "0"}), ArrLen(0));

  // Test WITHSCORES
  resp = Run({"ZRandMember", "x", "1", "WITHSCORES"});
  ASSERT_THAT(resp, ArrLen(2));
  EXPECT_THAT(resp, IsScoredSubsetOf({{"a", "1"}, {"b", "2"}, {"c", "3"}}));

  resp = Run({"ZRandMember", "x", "2", "WITHSCORES"});
  ASSERT_THAT(resp, ArrLen(4));
  EXPECT_THAT(resp, IsScoredSubsetOf({{"a", "1"}, {"b", "2"}, {"c", "3"}}));

  resp = Run({"ZRandMember", "x", "3", "WITHSCORES"});
  ASSERT_THAT(resp, ArrLen(6));
  EXPECT_THAT(resp, UnorderedScoredElementsAre({{"a", "1"}, {"b", "2"}, {"c", "3"}}));

  resp = Run({"ZRandMember", "x", "15", "WITHSCORES"});
  ASSERT_THAT(resp, ArrLen(6));
  EXPECT_THAT(resp, UnorderedScoredElementsAre({{"a", "1"}, {"b", "2"}, {"c", "3"}}));

  resp = Run({"ZRandMember", "x", "-1", "WITHSCORES"});
  ASSERT_THAT(resp, ArrLen(2));
  EXPECT_THAT(resp, ConsistsOfScoredElements({{"a", "1"}, {"b", "2"}, {"c", "3"}}));

  resp = Run({"ZRandMember", "x", "-2", "WITHSCORES"});
  ASSERT_THAT(resp, ArrLen(4));
  EXPECT_THAT(resp, ConsistsOfScoredElements({{"a", "1"}, {"b", "2"}, {"c", "3"}}));

  resp = Run({"ZRandMember", "x", "-3", "WITHSCORES"});
  ASSERT_THAT(resp, ArrLen(6));
  EXPECT_THAT(resp, ConsistsOfScoredElements({{"a", "1"}, {"b", "2"}, {"c", "3"}}));

  resp = Run({"ZRandMember", "x", "-15", "WITHSCORES"});
  ASSERT_THAT(resp, ArrLen(30));
  EXPECT_THAT(resp, ConsistsOfScoredElements({{"a", "1"}, {"b", "2"}, {"c", "3"}}));
}

TEST_F(ZSetFamilyTest, ZMScore) {
  Run({"zadd", "zms", "3.14", "a"});
  Run({"zadd", "zms", "42", "another"});

  auto resp = Run({"zmscore", "zms", "another", "a", "nofield"});
  ASSERT_EQ(RespExpr::ARRAY, resp.type);
  EXPECT_THAT(resp.GetVec(), ElementsAre("42", "3.14", ArgType(RespExpr::NIL)));
}

TEST_F(ZSetFamilyTest, ZRangeRank) {
  Run({"zadd", "x", "1.1", "a", "2.1", "b"});
  EXPECT_THAT(Run({"zrangebyscore", "x", "0", "(1.1"}), ArrLen(0));
  EXPECT_THAT(Run({"zrangebyscore", "x", "-inf", "1.1", "limit", "0", "10"}), "a");

  auto resp = Run({"zrangebyscore", "x", "-inf", "1.1", "limit", "0", "10", "WITHSCORES"});
  ASSERT_THAT(resp, ArrLen(2));
  ASSERT_THAT(resp.GetVec(), ElementsAre("a", "1.1"));

  resp = Run({"zrangebyscore", "x", "-inf", "1.1", "WITHSCORES", "limit", "0", "10"});
  ASSERT_THAT(resp, ArrLen(2));
  ASSERT_THAT(resp.GetVec(), ElementsAre("a", "1.1"));

  resp = Run({"zrangebyscore", "x", "-inf", "+inf", "LIMIT", "0", "-1"});
  ASSERT_THAT(resp, ArrLen(2));
  ASSERT_THAT(resp.GetVec(), ElementsAre("a", "b"));

  resp = Run({"zrevrangebyscore", "x", "+inf", "-inf", "limit", "0", "5"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  ASSERT_THAT(resp.GetVec(), ElementsAre("b", "a"));

  EXPECT_EQ(2, CheckedInt({"zcount", "x", "1.1", "2.1"}));
  EXPECT_EQ(1, CheckedInt({"zcount", "x", "(1.1", "2.1"}));
  EXPECT_EQ(0, CheckedInt({"zcount", "y", "(1.1", "2.1"}));
}

TEST_F(ZSetFamilyTest, ZRank) {
  Run({"zadd", "x", "1.1", "a", "2.1", "b"});
  EXPECT_EQ(0, CheckedInt({"zrank", "x", "a"}));
  EXPECT_EQ(1, CheckedInt({"zrank", "x", "b"}));
  EXPECT_EQ(1, CheckedInt({"zrevrank", "x", "a"}));
  EXPECT_EQ(0, CheckedInt({"zrevrank", "x", "b"}));
  EXPECT_THAT(Run({"zrevrank", "x", "c"}), ArgType(RespExpr::NIL));
  EXPECT_THAT(Run({"zrank", "y", "c"}), ArgType(RespExpr::NIL));
  EXPECT_THAT(Run({"zrevrank", "x", "c", "WITHSCORE"}), ArgType(RespExpr::NIL));
  EXPECT_THAT(Run({"zrank", "y", "c", "WITHSCORE"}), ArgType(RespExpr::NIL));

  auto resp = Run({"zrank", "x", "a", "WITHSCORE"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  ASSERT_THAT(resp.GetVec(), ElementsAre(IntArg(0), "1.1"));

  resp = Run({"zrank", "x", "b", "WITHSCORE"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  ASSERT_THAT(resp.GetVec(), ElementsAre(IntArg(1), "2.1"));

  resp = Run({"zrevrank", "x", "a", "WITHSCORE"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  ASSERT_THAT(resp.GetVec(), ElementsAre(IntArg(1), "1.1"));

  resp = Run({"zrevrank", "x", "b", "WITHSCORE"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  ASSERT_THAT(resp.GetVec(), ElementsAre(IntArg(0), "2.1"));

  resp = Run({"zrank", "x", "a", "WITHSCORES"});
  ASSERT_THAT(resp, ErrArg("syntax error"));

  resp = Run({"zrank", "x", "a", "WITHSCORES", "42"});
  ASSERT_THAT(resp, ErrArg("wrong number of arguments for 'zrank' command"));

  resp = Run({"zrevrank", "x", "a", "WITHSCORES", "42"});
  ASSERT_THAT(resp, ErrArg("wrong number of arguments for 'zrevrank' command"));
}

TEST_F(ZSetFamilyTest, LargeSet) {
  for (int i = 0; i < 129; ++i) {
    auto resp = Run({"zadd", "key", absl::StrCat(i), absl::StrCat("element:", i)});
    EXPECT_THAT(resp, IntArg(1)) << i;
  }
  Run({"zadd", "key", "129", ""});

  EXPECT_THAT(Run({"zrangebyscore", "key", "(-inf", "(0.0"}), ArrLen(0));
  EXPECT_THAT(Run({"zrangebyscore", "key", "(5", "0.0"}), ArrLen(0));
  EXPECT_THAT(Run({"zrangebylex", "key", "-", "(element:0"}), ArrLen(0));
  EXPECT_EQ(2, CheckedInt({"zremrangebyscore", "key", "127", "(129"}));
}

TEST_F(ZSetFamilyTest, ZRemRangeRank) {
  Run({"zadd", "x", "1.1", "a", "2.1", "b"});
  EXPECT_THAT(Run({"ZREMRANGEBYRANK", "y", "0", "1"}), IntArg(0));
  EXPECT_THAT(Run({"ZREMRANGEBYRANK", "x", "0", "0"}), IntArg(1));
  EXPECT_EQ(Run({"zrange", "x", "0", "5"}), "b");
  EXPECT_THAT(Run({"ZREMRANGEBYRANK", "x", "0", "1"}), IntArg(1));
  EXPECT_EQ(Run({"type", "x"}), "none");
}

TEST_F(ZSetFamilyTest, ZRemRangeScore) {
  Run({"zadd", "x", "1.1", "a", "2.1", "b"});
  EXPECT_THAT(Run({"ZREMRANGEBYSCORE", "y", "0", "1"}), IntArg(0));
  EXPECT_THAT(Run({"ZREMRANGEBYSCORE", "x", "-inf", "1.1"}), IntArg(1));
  EXPECT_EQ(Run({"zrange", "x", "0", "5"}), "b");
  EXPECT_THAT(Run({"ZREMRANGEBYSCORE", "x", "(2.0", "+inf"}), IntArg(1));
  EXPECT_EQ(Run({"type", "x"}), "none");
  EXPECT_THAT(Run({"zremrangebyscore", "x", "1", "NaN"}), ErrArg("min or max is not a float"));
}

TEST_F(ZSetFamilyTest, IncrBy) {
  auto resp = Run({"zadd", "key", "xx", "incr", "2.1", "member"});
  EXPECT_THAT(resp, ArgType(RespExpr::NIL));

  resp = Run({"zadd", "key", "nx", "incr", "2.1", "member"});
  EXPECT_THAT(resp, "2.1");

  resp = Run({"zadd", "key", "nx", "incr", "4.9", "member"});
  EXPECT_THAT(resp, ArgType(RespExpr::NIL));
}

TEST_F(ZSetFamilyTest, ByLex) {
  Run({
      "zadd", "key",      "0", "alpha", "0", "bar",   "0", "cool", "0", "down",
      "0",    "elephant", "0", "foo",   "0", "great", "0", "hill", "0", "omega",
  });

  auto resp = Run({"zrangebylex", "key", "-", "[cool"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(resp.GetVec(), ElementsAre("alpha", "bar", "cool"));

  EXPECT_EQ(3, CheckedInt({"ZLEXCOUNT", "key", "(foo", "+"}));
  EXPECT_EQ(0, CheckedInt({"ZLEXCOUNT", "key", "(foo", "[fop"}));
  EXPECT_EQ(3, CheckedInt({"ZREMRANGEBYLEX", "key", "(foo", "+"}));

  resp = Run({"zrangebylex", "key", "[a", "+"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  ASSERT_THAT(resp.GetVec(), ElementsAre("alpha", "bar", "cool", "down", "elephant", "foo"));

  resp = Run({"zrangebylex", "key", "-", "+", "LIMIT", "2", "3"});
  ASSERT_THAT(resp.GetVec(), ElementsAre("cool", "down", "elephant"));

  resp = Run({"zrangebylex", "key", "-", "+", "LIMIT", "5", "1"});
  ASSERT_THAT(resp, "foo");
}

TEST_F(ZSetFamilyTest, ZRevRangeByLex) {
  Run({
      "zadd", "key",      "0", "alpha", "0", "bar",   "0", "cool", "0", "down",
      "0",    "elephant", "0", "foo",   "0", "great", "0", "hill", "0", "omega",
  });

  auto resp = Run({"zrevrangebylex", "key", "[cool", "-"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(resp.GetVec(), ElementsAre("cool", "bar", "alpha"));

  EXPECT_EQ(3, CheckedInt({"ZLEXCOUNT", "key", "(foo", "+"}));
  EXPECT_EQ(3, CheckedInt({"ZREMRANGEBYLEX", "key", "(foo", "+"}));

  resp = Run({"zrevrangebylex", "key", "+", "[a"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  ASSERT_THAT(resp.GetVec(), ElementsAre("foo", "elephant", "down", "cool", "bar", "alpha"));

  Run({"zadd", "myzset", "0", "a", "0", "b", "0", "c", "0", "d", "0", "e", "0", "f", "0", "g"});
  resp = Run({"zrevrangebylex", "myzset", "(c", "-"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(resp.GetVec(), ElementsAre("b", "a"));
}

TEST_F(ZSetFamilyTest, ZRange) {
  Run({"zadd", "key", "0", "a", "1", "d", "1", "b", "2", "c", "4", "e"});

  auto resp = Run({"zrange", "key", "0", "2"});
  ASSERT_THAT(resp, ArrLen(3));
  ASSERT_THAT(resp.GetVec(), ElementsAre("a", "b", "d"));

  resp = Run({"zrange", "key", "1", "3", "WITHSCORES"});
  ASSERT_THAT(resp, ArrLen(6));
  ASSERT_THAT(resp.GetVec(), ElementsAre("b", "1", "d", "1", "c", "2"));

  resp = Run({"zrange", "key", "1", "3", "WITHSCORES", "REV"});
  ASSERT_THAT(resp, ArrLen(6));
  ASSERT_THAT(resp.GetVec(), ElementsAre("c", "2", "d", "1", "b", "1"));

  resp = Run({"zrange", "key", "(1", "4", "BYSCORE", "WITHSCORES"});
  ASSERT_THAT(resp, ArrLen(4));
  ASSERT_THAT(resp.GetVec(), ElementsAre("c", "2", "e", "4"));

  resp = Run({"zrange", "key", "-", "d", "BYLEX", "BYSCORE"});
  EXPECT_THAT(resp, ErrArg("BYSCORE and BYLEX options are not compatible"));

  resp = Run({"zrange", "key", "0", "-1", "LIMIT", "3", "-1"});
  ASSERT_THAT(resp, ArrLen(2));
  ASSERT_THAT(resp.GetVec(), ElementsAre("c", "e"));

  Run({"zremrangebyscore", "key", "0", "4"});

  Run({
      "zadd", "key",      "0", "alpha", "0", "bar",   "0", "cool", "0", "down",
      "0",    "elephant", "0", "foo",   "0", "great", "0", "hill", "0", "omega",
  });
  resp = Run({"zrange", "key", "-", "[cool", "BYLEX"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(resp.GetVec(), ElementsAre("alpha", "bar", "cool"));

  resp = Run({"zrange", "key", "[cool", "-", "REV", "BYLEX"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(resp.GetVec(), ElementsAre("cool", "bar", "alpha"));

  resp = Run({"zrange", "key", "+", "[cool", "REV", "BYLEX", "LIMIT", "2", "2"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(resp.GetVec(), ElementsAre("great", "foo"));

  resp = Run({"zrange", "key", "+", "[cool", "BYLEX", "LIMIT", "2", "2", "REV"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(resp.GetVec(), ElementsAre("great", "foo"));
}

TEST_F(ZSetFamilyTest, ZRevRange) {
  Run({"zadd", "key", "-inf", "a", "1", "b", "2", "c"});
  auto resp = Run({"zrevrangebyscore", "key", "2", "-inf"});
  ASSERT_THAT(resp, ArrLen(3));
  EXPECT_THAT(resp.GetVec(), ElementsAre("c", "b", "a"));

  resp = Run({"zrevrangebyscore", "key", "2", "-inf", "withscores"});
  ASSERT_THAT(resp, ArrLen(6));
  EXPECT_THAT(resp.GetVec(), ElementsAre("c", "2", "b", "1", "a", "-inf"));

  resp = Run({"zrevrange", "key", "0", "2"});
  ASSERT_THAT(resp, ArrLen(3));
  EXPECT_THAT(resp.GetVec(), ElementsAre("c", "b", "a"));

  resp = Run({"zrevrange", "key", "1", "2", "withscores"});
  ASSERT_THAT(resp, ArrLen(4));
  EXPECT_THAT(resp.GetVec(), ElementsAre("b", "1", "a", "-inf"));

  // Make sure that when using with upper case it works as well (see
  // https://github.com/dragonflydb/dragonfly/issues/326)
  resp = Run({"zrevrangebyscore", "key", "2", "-INF"});
  ASSERT_THAT(resp, ArrLen(3));
  EXPECT_THAT(resp.GetVec(), ElementsAre("c", "b", "a"));

  resp = Run({"zrevrangebyscore", "key", "2", "-INF", "withscores"});
  ASSERT_THAT(resp, ArrLen(6));
  EXPECT_THAT(resp.GetVec(), ElementsAre("c", "2", "b", "1", "a", "-inf"));
}

TEST_F(ZSetFamilyTest, ZScan) {
  string prefix(128, 'a');
  for (unsigned i = 0; i < 100; ++i) {
    Run({"zadd", "key", "1", absl::StrCat(prefix, i)});
  }

  EXPECT_EQ(100, CheckedInt({"zcard", "key"}));
  int64_t cursor = 0;
  size_t scan_len = 0;
  do {
    auto resp = Run({"zscan", "key", absl::StrCat(cursor)});
    ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
    ASSERT_THAT(resp.GetVec(), ElementsAre(ArgType(RespExpr::STRING), ArgType(RespExpr::ARRAY)));
    string_view token = ToSV(resp.GetVec()[0].GetBuf());
    ASSERT_TRUE(absl::SimpleAtoi(token, &cursor));
    auto sub_arr = resp.GetVec()[1].GetVec();
    scan_len += sub_arr.size();
  } while (cursor != 0);

  EXPECT_EQ(100 * 2, scan_len);

  // Check scan with count and match params
  scan_len = 0;
  do {
    auto resp = Run({"zscan", "key", absl::StrCat(cursor), "count", "5", "match", "*0"});
    ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
    ASSERT_THAT(resp.GetVec(), ElementsAre(ArgType(RespExpr::STRING), ArgType(RespExpr::ARRAY)));
    string_view token = ToSV(resp.GetVec()[0].GetBuf());
    ASSERT_TRUE(absl::SimpleAtoi(token, &cursor));
    auto sub_arr = resp.GetVec()[1].GetVec();
    scan_len += sub_arr.size();
  } while (cursor != 0);
  EXPECT_EQ(10 * 2, scan_len);  // expected members a0,a10,a20..,a90
}

TEST_F(ZSetFamilyTest, ZUnionError) {
  RespExpr resp;

  resp = Run({"zunion", "0"});
  EXPECT_THAT(resp, ErrArg("wrong number of arguments"));

  resp = Run({"zunion", "0", "myset"});
  EXPECT_THAT(resp, ErrArg("at least 1 input key is needed"));

  resp = Run({"zunion", "3", "z1", "z2", "z3", "weights", "1", "1", "k"});
  EXPECT_THAT(resp, ErrArg("weight value is not a float"));

  resp = Run({"zunion", "3", "z1", "z2", "z3", "weights", "1", "1", "2", "aggregate", "something"});
  EXPECT_THAT(resp, ErrArg("syntax error"));

  resp = Run({"zunion", "3", "z1", "z2", "z3", "weights", "1", "2", "aggregate", "something"});
  EXPECT_THAT(resp, ErrArg("weight value is not a float"));

  resp = Run({"zunion", "3", "z1", "z2", "z3", "aggregate", "sum", "somescore"});
  EXPECT_THAT(resp, ErrArg("syntax error"));

  resp = Run({"zunion", "3", "z1", "z2", "z3", "withscores", "someargs"});
  EXPECT_THAT(resp, ErrArg("syntax error"));

  resp = Run({"zunion", "1"});
  EXPECT_THAT(resp, ErrArg("wrong number of arguments"));

  resp = Run({"zunion", "2", "z1"});
  EXPECT_THAT(resp, ErrArg("syntax error"));

  resp = Run({"zunion", "2", "z1", "z2", "z3"});
  EXPECT_THAT(resp, ErrArg("syntax error"));

  resp = Run({"zunion", "2", "z1", "z2", "weights", "1", "2", "3"});
  EXPECT_THAT(resp, ErrArg("syntax error"));
}

TEST_F(ZSetFamilyTest, ZUnion) {
  RespExpr resp;

  EXPECT_EQ(2, CheckedInt({"zadd", "z1", "1", "a", "3", "b"}));
  EXPECT_EQ(2, CheckedInt({"zadd", "z2", "3", "c", "2", "b"}));
  EXPECT_EQ(2, CheckedInt({"zadd", "z3", "1", "c", "1", "d"}));

  resp = Run({"zunion", "3", "z1", "z2", "z3"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("a", "d", "c", "b"));

  resp = Run({"zunion", "3", "z1", "z2", "z3", "weights", "1", "1", "2"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("a", "d", "b", "c"));

  // Cover union of sets and zsets
  EXPECT_EQ(2, CheckedInt({"sadd", "s2", "b", "c"}));
  resp = Run({"zunion", "2", "z1", "s2", "weights", "1", "2", "withscores"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("a", "1", "c", "2", "b", "5"));

  resp = Run({"zunion", "3", "z1", "z2", "z3", "weights", "1", "1", "2", "withscores"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("a", "1", "d", "2", "b", "5", "c", "5"));

  resp = Run({"zunion", "3", "z1", "z2", "z3", "weights", "1", "1", "2", "aggregate", "min",
              "withscores"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("a", "1", "b", "2", "c", "2", "d", "2"));

  resp = Run({"zunion", "3", "z1", "z2", "z3", "withscores", "weights", "1", "1", "2", "aggregate",
              "min"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("a", "1", "b", "2", "c", "2", "d", "2"));

  resp = Run({"zunion", "3", "none1", "none2", "z3", "withscores", "weights", "1", "1", "2"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("c", "2", "d", "2"));

  resp = Run({"zunion", "3", "z1", "z2", "z3", "weights", "1", "1", "2", "aggregate", "max",
              "withscores"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("a", "1", "d", "2", "b", "3", "c", "3"));

  resp = Run({"zunion", "1", "z1", "weights", "2", "aggregate", "max", "withscores"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("a", "2", "b", "6"));

  for (unsigned i = 0; i < 256; ++i) {
    Run({"zadd", "large1", "1000", absl::StrCat("aaaaaaaaaa", i)});
    Run({"zadd", "large2", "1000", absl::StrCat("bbbbbbbbbb", i)});
    Run({"zadd", "large2", "1000", absl::StrCat("aaaaaaaaaa", i)});
  }
  resp = Run({"zunion", "2", "large2", "large1"});
  EXPECT_THAT(resp, ArrLen(512));
}

TEST_F(ZSetFamilyTest, ZUnionStore) {
  RespExpr resp;

  resp = Run({"zunionstore", "key", "0"});
  EXPECT_THAT(resp, ErrArg("wrong number of arguments"));

  resp = Run({"zunionstore", "key", "0", "aggregate"});
  EXPECT_THAT(resp, ErrArg("at least 1 input key is needed"));

  resp = Run({"zunionstore", "key", "0", "aggregate", "sum"});
  EXPECT_THAT(resp, ErrArg("at least 1 input key is needed"));
  resp = Run({"zunionstore", "key", "-1", "aggregate", "sum"});
  EXPECT_THAT(resp, ErrArg("out of range"));
  resp = Run({"zunionstore", "key", "2", "foo", "bar", "weights", "1"});
  EXPECT_THAT(resp, ErrArg("syntax error"));

  EXPECT_EQ(2, CheckedInt({"zadd", "z1", "1", "a", "2", "b"}));
  EXPECT_EQ(2, CheckedInt({"zadd", "z2", "3", "c", "2", "b"}));

  resp = Run({"zunionstore", "key", "2", "z1", "z2"});
  EXPECT_THAT(resp, IntArg(3));
  resp = Run({"zrange", "key", "0", "-1", "withscores"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("a", "1", "c", "3", "b", "4"));

  resp = Run({"zunionstore", "z1", "1", "z1"});
  EXPECT_THAT(resp, IntArg(2));

  resp = Run({"zunionstore", "z1", "2", "z1", "z2"});
  EXPECT_THAT(resp, IntArg(3));
  resp = Run({"zrange", "z1", "0", "-1", "withscores"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("a", "1", "c", "3", "b", "4"));

  Run({"set", "foo", "bar"});
  resp = Run({"zunionstore", "foo", "1", "z2"});
  EXPECT_THAT(resp, IntArg(2));
  resp = Run({"zrange", "foo", "0", "-1", "withscores"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("b", "2", "c", "3"));
}

TEST_F(ZSetFamilyTest, ZUnionStoreOpts) {
  EXPECT_EQ(2, CheckedInt({"zadd", "z1", "1", "a", "2", "b"}));
  EXPECT_EQ(2, CheckedInt({"zadd", "z2", "3", "c", "2", "b"}));
  RespExpr resp;

  EXPECT_EQ(3, CheckedInt({"zunionstore", "a", "2", "z1", "z2", "weights", "1", "3"}));
  resp = Run({"zrange", "a", "0", "-1", "withscores"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("a", "1", "b", "8", "c", "9"));

  resp = Run({"zunionstore", "a", "2", "z1", "z2", "weights", "1"});
  EXPECT_THAT(resp, ErrArg("syntax error"));

  resp = Run({"zunionstore", "z1", "1", "z1", "weights", "2"});
  EXPECT_THAT(resp, IntArg(2));
  resp = Run({"zrange", "z1", "0", "-1", "withscores"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("a", "2", "b", "4"));

  resp = Run({"zunionstore", "max", "2", "z1", "z2", "weights", "1", "0", "aggregate", "max"});
  ASSERT_THAT(resp, IntArg(3));
  resp = Run({"zrange", "max", "0", "-1", "withscores"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("c", "0", "a", "2", "b", "4"));

  // Check that infinity is handled correctly.
  Run({"ZADD", "src1", "inf", "x"});
  Run({"ZADD", "src2", "inf", "x"});
  Run({"ZUNIONSTORE", "dest", "2", "src1", "src2", "WEIGHTS", "1.0", "0.0"});
  resp = Run({"ZSCORE", "dest", "x"});
  EXPECT_THAT(resp, DoubleArg(numeric_limits<double>::infinity()));

  Run({"ZADD", "foo", "inf", "e1"});
  Run({"ZADD", "bar", "-inf", "e1", "0.0", "e2"});
  Run({"ZUNIONSTORE", "dest", "3", "foo", "bar", "foo"});
  resp = Run({"ZSCORE", "dest", "e1"});
  EXPECT_THAT(resp, DoubleArg(0));
}

TEST_F(ZSetFamilyTest, ZInterStore) {
  EXPECT_EQ(2, CheckedInt({"zadd", "z1", "1", "a", "2", "b"}));
  EXPECT_EQ(2, CheckedInt({"zadd", "z2", "3", "c", "2", "b"}));
  RespExpr resp;

  EXPECT_EQ(1, CheckedInt({"zinterstore", "a", "2", "z1", "z2"}));
  resp = Run({"zrange", "a", "0", "-1", "withscores"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("b", "4"));

  // support for sets
  EXPECT_EQ(2, CheckedInt({"sadd", "s2", "b", "c"}));
  EXPECT_EQ(1, CheckedInt({"zinterstore", "b", "2", "z1", "s2"}));
  resp = Run({"zrange", "b", "0", "-1", "withscores"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("b", "3"));

  Run({"ZADD", "foo", "10", "a"});
  EXPECT_EQ(1, CheckedInt({"ZINTERSTORE", "bar", "1", "foo", "weights", "2"}));
  resp = Run({"zrange", "bar", "0", "-1", "withscores"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("a", "20"));
}

TEST_F(ZSetFamilyTest, ZInter) {
  EXPECT_EQ(2, CheckedInt({"zadd", "z1", "1", "one", "2", "two"}));
  EXPECT_EQ(3, CheckedInt({"zadd", "z2", "1", "one", "2", "two", "3", "three"}));
  RespExpr resp;

  resp = Run({"zinter", "2", "z1", "z2"});
  EXPECT_THAT(resp, ArrLen(2));
  EXPECT_THAT(resp.GetVec(), ElementsAre("one", "two"));

  EXPECT_EQ(3, CheckedInt({"zadd", "z3", "1", "one", "2", "two", "3", "three"}));
  EXPECT_EQ(3, CheckedInt({"zadd", "z4", "4", "four", "5", "five", "6", "six"}));
  EXPECT_EQ(1, CheckedInt({"zadd", "z5", "6", "six"}));

  resp = Run({"zinter", "3", "z3", "z4", "z5"});
  EXPECT_THAT(resp, ArrLen(0));

  // zinter output sorts keys with equal scores lexicographically
  Run({"del", "z1", "z2", "z3", "z4", "z5"});
  Run({"zadd", "z1", "1", "e", "1", "a", "1", "b", "1", "x"});
  Run({"zadd", "z2", "1", "e", "1", "a", "1", "b", "1", "y"});
  Run({"zadd", "z3", "1", "e", "1", "a", "1", "b", "1", "z"});
  Run({"zadd", "z4", "1", "e", "1", "a", "1", "b", "1", "o"});
  EXPECT_THAT(Run({"zinter", "4", "z1", "z2", "z3", "z4"}).GetVec(), ElementsAre("a", "b", "e"));
}

TEST_F(ZSetFamilyTest, ZInterCard) {
  EXPECT_EQ(3, CheckedInt({"zadd", "z1", "1", "a", "2", "b", "3", "c"}));
  EXPECT_EQ(3, CheckedInt({"zadd", "z2", "2", "b", "3", "c", "4", "d"}));

  EXPECT_EQ(2, CheckedInt({"zintercard", "2", "z1", "z2"}));
  EXPECT_EQ(1, CheckedInt({"zintercard", "2", "z1", "z2", "LIMIT", "1"}));

  RespExpr resp;

  resp = Run({"zintercard", "2", "z1", "z2", "LIM"});
  EXPECT_THAT(resp, ErrArg("syntax error"));
  resp = Run({"zintercard", "2", "z1", "z2", "LIMIT"});
  EXPECT_THAT(resp, ErrArg("syntax error"));
  resp = Run({"zintercard", "2", "z1", "z2", "LIMIT", "a"});
  EXPECT_THAT(resp, ErrArg("limit value is not a positive integer"));

  resp = Run({"zintercard", "0", "z1"});
  EXPECT_THAT(resp, ErrArg("at least 1 input"));

  // support for sets
  EXPECT_EQ(3, CheckedInt({"sadd", "s2", "b", "c", "d"}));
  EXPECT_EQ(2, CheckedInt({"zintercard", "2", "z1", "s2"}));
}

TEST_F(ZSetFamilyTest, ZAddBug148) {
  auto resp = Run({"zadd", "key", "1", "9fe9f1eb"});
  EXPECT_THAT(resp, IntArg(1));
}

TEST_F(ZSetFamilyTest, ZMPopInvalidSyntax) {
  // Not enough arguments.
  auto resp = Run({"zmpop", "1", "a"});
  EXPECT_THAT(resp, ErrArg("wrong number of arguments"));

  // Zero keys.
  resp = Run({"zmpop", "0", "MIN", "COUNT", "1"});
  EXPECT_THAT(resp, ErrArg("syntax error"));

  // Number of keys not uint.
  resp = Run({"zmpop", "aa", "a", "MIN"});
  EXPECT_THAT(resp, ErrArg("value is not an integer or out of range"));

  // Missing MIN/MAX.
  resp = Run({"zmpop", "1", "a", "COUNT", "1"});
  EXPECT_THAT(resp, ErrArg("syntax error"));

  // Wrong number of keys.
  resp = Run({"zmpop", "1", "a", "b", "MAX"});
  EXPECT_THAT(resp, ErrArg("syntax error"));

  // Count with no number.
  resp = Run({"zmpop", "1", "a", "MAX", "COUNT"});
  EXPECT_THAT(resp, ErrArg("syntax error"));

  // Count number is not uint.
  resp = Run({"zmpop", "1", "a", "MIN", "COUNT", "boo"});
  EXPECT_THAT(resp, ErrArg("value is not an integer or out of range"));

  // Too many arguments.
  resp = Run({"zmpop", "1", "c", "MAX", "COUNT", "2", "foo"});
  EXPECT_THAT(resp, ErrArg("syntax error"));
}

TEST_F(ZSetFamilyTest, ZMPop) {
  // All sets are empty.
  auto resp = Run({"zmpop", "1", "e", "MIN"});
  EXPECT_THAT(resp, ArgType(RespExpr::NIL));

  // Min operation.
  resp = Run({"zadd", "a", "1", "a1", "2", "a2"});
  EXPECT_THAT(resp, IntArg(2));

  resp = Run({"zmpop", "1", "a", "MIN"});
  EXPECT_THAT(resp, ContainsLabeledScoredArray("a", {{"a1", "1"}}));

  resp = Run({"ZRANGE", "a", "0", "-1", "WITHSCORES"});
  EXPECT_THAT(resp, RespArray(ElementsAre("a2", "2")));

  // Max operation.
  resp = Run({"zadd", "b", "1", "b1", "2", "b2"});
  EXPECT_THAT(resp, IntArg(2));

  resp = Run({"zmpop", "1", "b", "MAX"});
  EXPECT_THAT(resp, ContainsLabeledScoredArray("b", {{"b2", "2"}}));

  resp = Run({"ZRANGE", "b", "0", "-1", "WITHSCORES"});
  EXPECT_THAT(resp, RespArray(ElementsAre("b1", "1")));

  // Count > 1.
  resp = Run({"zadd", "c", "1", "c1", "2", "c2"});
  EXPECT_THAT(resp, IntArg(2));

  resp = Run({"zmpop", "1", "c", "MAX", "COUNT", "2"});
  EXPECT_THAT(resp, ContainsLabeledScoredArray("c", {{"c1", "1"}, {"c2", "2"}}));

  resp = Run({"zcard", "c"});
  EXPECT_THAT(resp, IntArg(0));

  // Count > #elements in set.
  resp = Run({"zadd", "d", "1", "d1", "2", "d2"});
  EXPECT_THAT(resp, IntArg(2));

  resp = Run({"zmpop", "1", "d", "MAX", "COUNT", "3"});
  EXPECT_THAT(resp, ContainsLabeledScoredArray("d", {{"d1", "1"}, {"d2", "2"}}));

  resp = Run({"zcard", "d"});
  EXPECT_THAT(resp, IntArg(0));

  // First non empty set is not the first set.
  resp = Run({"zadd", "x", "1", "x1"});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"zadd", "y", "1", "y1"});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"zmpop", "3", "empty", "x", "y", "MAX"});
  EXPECT_THAT(resp, ContainsLabeledScoredArray("x", {{"x1", "1"}}));

  resp = Run({"zcard", "x"});
  EXPECT_THAT(resp, IntArg(0));

  resp = Run({"ZRANGE", "y", "0", "-1", "WITHSCORES"});
  EXPECT_THAT(resp, RespArray(ElementsAre("y1", "1")));
}

TEST_F(ZSetFamilyTest, ZPopMin) {
  auto resp = Run({"zadd", "key", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e", "6", "f"});
  EXPECT_THAT(resp, IntArg(6));

  resp = Run({"zpopmin", "key"});
  ASSERT_THAT(resp, ArrLen(2));
  EXPECT_THAT(resp.GetVec(), ElementsAre("a", "1"));

  resp = Run({"zpopmin", "key", "0"});
  ASSERT_THAT(resp, ArrLen(0));

  resp = Run({"zpopmin", "key", "2"});
  ASSERT_THAT(resp, ArrLen(4));
  EXPECT_THAT(resp.GetVec(), ElementsAre("b", "2", "c", "3"));

  resp = Run({"zpopmin", "key", "-1"});
  ASSERT_THAT(resp, ErrArg("value is out of range, must be positive"));

  resp = Run({"zpopmin", "key", "1"});
  ASSERT_THAT(resp, ArrLen(2));
  EXPECT_THAT(resp.GetVec(), ElementsAre("d", "4"));

  resp = Run({"zpopmin", "key", "3"});
  ASSERT_THAT(resp, ArrLen(4));
  EXPECT_THAT(resp.GetVec(), ElementsAre("e", "5", "f", "6"));

  resp = Run({"zpopmin", "key", "1"});
  ASSERT_THAT(resp, ArrLen(0));
}

TEST_F(ZSetFamilyTest, ZPopMax) {
  auto resp = Run({"zadd", "key", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e", "6", "f"});
  EXPECT_THAT(resp, IntArg(6));

  resp = Run({"zpopmax", "key"});
  ASSERT_THAT(resp, ArrLen(2));
  EXPECT_THAT(resp.GetVec(), ElementsAre("f", "6"));

  resp = Run({"zpopmax", "key", "2"});
  ASSERT_THAT(resp, ArrLen(4));
  EXPECT_THAT(resp.GetVec(), ElementsAre("e", "5", "d", "4"));

  resp = Run({"zpopmax", "key", "-1"});
  ASSERT_THAT(resp, ErrArg("value is out of range, must be positive"));

  resp = Run({"zpopmax", "key", "1"});
  ASSERT_THAT(resp, ArrLen(2));
  EXPECT_THAT(resp.GetVec(), ElementsAre("c", "3"));

  resp = Run({"zpopmax", "key", "3"});
  ASSERT_THAT(resp, ArrLen(4));
  EXPECT_THAT(resp.GetVec(), ElementsAre("b", "2", "a", "1"));

  resp = Run({"zpopmax", "key", "1"});
  ASSERT_THAT(resp, ArrLen(0));
}

TEST_F(ZSetFamilyTest, ZAddPopCrash) {
  for (int i = 0; i < 129; ++i) {
    auto resp = Run({"zadd", "key", absl::StrCat(i), absl::StrCat("element:", i)});
    EXPECT_THAT(resp, IntArg(1)) << i;
  }

  auto resp = Run({"zpopmin", "key"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("element:0", "0"));
}

TEST_F(ZSetFamilyTest, Resp3) {
  Run({"hello", "3"});
  Run({"zadd", "x", "1", "a", "2", "b"});
  auto resp = Run({"zrange", "x", "0", "-1", "WITHSCORES"});
  ASSERT_THAT(resp, ArrLen(2));
  ASSERT_THAT(resp.GetVec()[0].GetVec(), ElementsAre("a", DoubleArg(1)));
  ASSERT_THAT(resp.GetVec()[1].GetVec(), ElementsAre("b", DoubleArg(2)));
}

TEST_F(ZSetFamilyTest, BlockingIsReleased) {
  // Inputs for ZSET store commands.
  Run({"ZADD", "A", "1", "x", "2", "b"});
  Run({"ZADD", "B", "1", "x", "3", "b"});
  Run({"ZADD", "C", "1", "x", "10", "a"});
  Run({"ZADD", "D", "1", "x", "5", "c"});

  vector<string> blocking_keys{"zset1", "zset2", "zset3"};
  for (const auto& key : blocking_keys) {
    vector<vector<string>> unblocking_commands;
    // All commands output the same set {2,x}.
    unblocking_commands.push_back({"ZADD", key, "2", "x", "10", "y"});
    unblocking_commands.push_back({"ZINCRBY", key, "2", "x"});
    unblocking_commands.push_back({"ZINTERSTORE", key, "2", "A", "B"});
    unblocking_commands.push_back({"ZUNIONSTORE", key, "2", "C", "D"});
    // unblocking_commands.push_back({"ZDIFFSTORE", key, "2", "A", "B"}); // unimplemented

    for (auto& cmd : unblocking_commands) {
      RespExpr resp0;
      auto fb0 = pp_->at(0)->LaunchFiber(Launch::dispatch, [&] {
        resp0 = Run({"BZPOPMIN", "zset1", "zset2", "zset3", "0"});
        LOG(INFO) << "BZPOPMIN";
      });

      pp_->at(1)->Await([&] { return Run({cmd.data(), cmd.size()}); });
      fb0.Join();

      ASSERT_THAT(resp0, ArrLen(3)) << cmd[0];
      EXPECT_THAT(resp0.GetVec(), ElementsAre(key, "x", "2")) << cmd[0];

      Run({"DEL", key});
    }
  }
}

TEST_F(ZSetFamilyTest, BlockingWithIncorrectType) {
  RespExpr resp0;
  RespExpr resp1;
  auto fb0 = pp_->at(0)->LaunchFiber(Launch::dispatch, [&] {
    resp0 = Run({"BLPOP", "list1", "0"});
  });
  auto fb1 = pp_->at(1)->LaunchFiber(Launch::dispatch, [&] {
    resp1 = Run({"BZPOPMIN", "list1", "0"});
  });

  ThisFiber::SleepFor(50us);
  pp_->at(2)->Await([&] { return Run({"ZADD", "list1", "1", "a"}); });
  pp_->at(2)->Await([&] { return Run({"LPUSH", "list1", "0"}); });
  fb0.Join();
  fb1.Join();

  EXPECT_THAT(resp1.GetVec(), ElementsAre("list1", "a", "1"));
  EXPECT_THAT(resp0.GetVec(), ElementsAre("list1", "0"));
}

TEST_F(ZSetFamilyTest, BlockingTimeout) {
  RespExpr resp0;

  auto start = absl::Now();
  auto fb0 = pp_->at(0)->LaunchFiber(Launch::dispatch, [&] {
    resp0 = Run({"BZPOPMIN", "zset1", "1"});
    LOG(INFO) << "BZPOPMIN";
  });
  fb0.Join();
  auto dur = absl::Now() - start;

  // Check that the timeout duration is not too crazy.
  EXPECT_LT(AbsDuration(dur - absl::Milliseconds(1000)), absl::Milliseconds(300));
  EXPECT_THAT(resp0, ArgType(RespExpr::NIL_ARRAY));
}

TEST_F(ZSetFamilyTest, ZDiffError) {
  RespExpr resp;

  resp = Run({"zdiff", "-1", "z1"});
  EXPECT_THAT(resp, ErrArg("value is not an integer or out of range"));

  resp = Run({"zdiff", "0"});
  EXPECT_THAT(resp, ErrArg("wrong number of arguments"));

  resp = Run({"zdiff", "0", "z1"});
  EXPECT_THAT(resp, ErrArg("at least 1 input key is needed"));

  resp = Run({"zdiff", "0", "z1", "z2"});
  EXPECT_THAT(resp, ErrArg("at least 1 input key is needed"));
}

TEST_F(ZSetFamilyTest, ZDiff) {
  RespExpr resp;

  EXPECT_EQ(4, CheckedInt({"zadd", "z1", "1", "one", "2", "two", "3", "three", "4", "four"}));
  EXPECT_EQ(2, CheckedInt({"zadd", "z2", "1", "one", "5", "five"}));
  EXPECT_EQ(2, CheckedInt({"zadd", "z3", "2", "two", "3", "three"}));
  EXPECT_EQ(1, CheckedInt({"zadd", "z4", "4", "four"}));

  resp = Run({"zdiff", "1", "z1"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("one", "two", "three", "four"));

  resp = Run({"zdiff", "2", "z1", "z1"});
  EXPECT_THAT(resp.GetVec().empty(), true);

  resp = Run({"zdiff", "2", "z1", "doesnt_exist"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("one", "two", "three", "four"));

  resp = Run({"zdiff", "2", "z1", "z2"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("two", "three", "four"));

  resp = Run({"zdiff", "2", "z1", "z3"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("one", "four"));

  resp = Run({"zdiff", "4", "z1", "z2", "z3", "z4"});
  EXPECT_THAT(resp.GetVec().empty(), true);

  resp = Run({"zdiff", "2", "doesnt_exist", "key1"});
  EXPECT_THAT(resp.GetVec().empty(), true);

  // WITHSCORES
  resp = Run({"zdiff", "1", "z1", "WITHSCORES"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("one", "1", "two", "2", "three", "3", "four", "4"));

  resp = Run({"zdiff", "2", "z1", "z2", "WITHSCORES"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("two", "2", "three", "3", "four", "4"));
}

TEST_F(ZSetFamilyTest, Count) {
  for (int i = 0; i < 129; ++i) {
    auto resp = Run({"zadd", "key", absl::StrCat(i), absl::StrCat("element:", i)});
    EXPECT_THAT(resp, IntArg(1)) << i;
  }

  EXPECT_THAT(CheckedInt({"zcount", "key", "-inf", "+inf"}), 129);
  EXPECT_THAT(CheckedInt({"zlexcount", "key", "-", "+"}), 129);
}

TEST_F(ZSetFamilyTest, RangeLimit) {
  auto resp = Run({"ZRANGEBYSCORE", "", "0.0", "0.0", "limit", "0"});
  EXPECT_THAT(resp, ErrArg("syntax error"));
  resp = Run({"ZRANGEBYSCORE", "", "0.0", "0.0", "limit", "0", "0"});
  EXPECT_THAT(resp, ArrLen(0));

  resp = Run({"ZRANGEBYSCORE", "", "0.0", "0.0", "foo"});
  EXPECT_THAT(resp, ErrArg("unsupported option"));

  resp = Run({"ZRANGEBYLEX", "foo", "-", "+", "LIMIT", "-1", "3"});
  EXPECT_THAT(resp, ArrLen(0));
}

TEST_F(ZSetFamilyTest, RangeStore) {
  EXPECT_EQ(3, CheckedInt({"ZADD", "src", "1", "a", "2", "b", "3", "c"}));
  EXPECT_EQ(3, CheckedInt({"ZRANGESTORE", "dest", "src", "0", "-1"}));

  RespExpr resp = Run({"ZRANGE", "dest", "0", "-1", "withscores"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("a", "1", "b", "2", "c", "3"));

  // Override dest.

  EXPECT_EQ(0, CheckedInt({"ZRANGESTORE", "dest", "not-found", "0", "-1"}));

  resp = Run({"ZRANGE", "dest", "0", "-1"});
  EXPECT_THAT(resp, ArrLen(0));
}

}  // namespace dfly
