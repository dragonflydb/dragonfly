// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/hset_family.h"

extern "C" {
#include "redis/listpack.h"
#include "redis/object.h"
#include "redis/sds.h"
}

#include "base/gtest.h"
#include "base/logging.h"
#include "facade/facade_test.h"
#include "server/test_utils.h"

using namespace testing;
using namespace std;
using namespace util;
using namespace boost;
using namespace facade;

namespace dfly {

class HSetFamilyTest : public BaseFamilyTest {
 protected:
};

class HestFamilyTestProtocolVersioned : public HSetFamilyTest,
                                        public ::testing::WithParamInterface<string> {
 protected:
};

INSTANTIATE_TEST_CASE_P(HestFamilyTestProtocolVersioned, HestFamilyTestProtocolVersioned,
                        ::testing::Values("2", "3"));

TEST_F(HSetFamilyTest, Basic) {
  auto resp = Run({"hset", "x", "a"});
  EXPECT_THAT(resp, ErrArg("wrong number"));

  EXPECT_THAT(Run({"HSET", "hs", "key1", "val1", "key2"}), ErrArg("wrong number"));

  EXPECT_EQ(1, CheckedInt({"hset", "x", "a", "b"}));
  EXPECT_EQ(1, CheckedInt({"hlen", "x"}));

  EXPECT_EQ(1, CheckedInt({"hexists", "x", "a"}));
  EXPECT_EQ(0, CheckedInt({"hexists", "x", "b"}));
  EXPECT_EQ(0, CheckedInt({"hexists", "y", "a"}));

  EXPECT_EQ(0, CheckedInt({"hset", "x", "a", "b"}));
  EXPECT_EQ(0, CheckedInt({"hset", "x", "a", "c"}));
  EXPECT_EQ(0, CheckedInt({"hset", "x", "a", ""}));

  EXPECT_EQ(2, CheckedInt({"hset", "y", "a", "c", "d", "e"}));
  EXPECT_EQ(2, CheckedInt({"hdel", "y", "a", "d"}));

  EXPECT_THAT(Run({"hdel", "nokey", "a"}), IntArg(0));
}

TEST_F(HSetFamilyTest, HSet) {
  string val(1024, 'b');

  EXPECT_EQ(1, CheckedInt({"hset", "large", "a", val}));
  EXPECT_EQ(1, CheckedInt({"hlen", "large"}));
  EXPECT_EQ(1024, CheckedInt({"hstrlen", "large", "a"}));

  EXPECT_EQ(1, CheckedInt({"hset", "small", "", "565323349817"}));
}

TEST_P(HestFamilyTestProtocolVersioned, Get) {
  auto resp = Run({"hello", GetParam()});
  EXPECT_THAT(resp.GetVec()[6], "proto");
  EXPECT_THAT(resp.GetVec()[7], IntArg(atoi(GetParam().c_str())));

  resp = Run({"hset", "x", "a", "1", "b", "2", "c", "3"});
  EXPECT_THAT(resp, IntArg(3));

  resp = Run({"hmget", "unkwn", "a", "c"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(resp.GetVec(), ElementsAre(ArgType(RespExpr::NIL), ArgType(RespExpr::NIL)));

  resp = Run({"hkeys", "x"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(resp.GetVec(), UnorderedElementsAre("a", "b", "c"));

  resp = Run({"hvals", "x"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(resp.GetVec(), UnorderedElementsAre("1", "2", "3"));

  resp = Run({"hmget", "x", "a", "c", "d"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(resp.GetVec(), ElementsAre("1", "3", ArgType(RespExpr::NIL)));

  resp = Run({"hgetall", "x"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(resp.GetVec(), ElementsAre("a", "1", "b", "2", "c", "3"));
}

TEST_F(HSetFamilyTest, HSetNx) {
  EXPECT_EQ(1, CheckedInt({"hsetnx", "key", "field", "val"}));
  EXPECT_EQ(Run({"hget", "key", "field"}), "val");

  EXPECT_EQ(0, CheckedInt({"hsetnx", "key", "field", "val2"}));
  EXPECT_EQ(Run({"hget", "key", "field"}), "val");

  EXPECT_EQ(1, CheckedInt({"hsetnx", "key", "field2", "val2"}));
  EXPECT_EQ(Run({"hget", "key", "field2"}), "val2");

  // check dict path
  EXPECT_EQ(0, CheckedInt({"hsetnx", "key", "field2", string(512, 'a')}));
  EXPECT_EQ(Run({"hget", "key", "field2"}), "val2");
}

TEST_F(HSetFamilyTest, HIncr) {
  EXPECT_EQ(10, CheckedInt({"hincrby", "key", "field", "10"}));

  Run({"hset", "key", "a", " 1"});
  auto resp = Run({"hincrby", "key", "a", "10"});
  EXPECT_THAT(resp, ErrArg("hash value is not an integer"));
}

TEST_F(HSetFamilyTest, HScan) {
  for (int i = 0; i < 10; i++) {
    Run({"HSET", "myhash", absl::StrCat("Field-", i), absl::StrCat("Value-", i)});
  }

  // Note that even though this limit by 4, it would return more because
  // all fields are on listpack
  auto resp = Run({"hscan", "myhash", "0", "count", "4"});
  EXPECT_THAT(resp, ArrLen(2));
  auto vec = StrArray(resp.GetVec()[1]);
  EXPECT_EQ(vec.size(), 20);
  EXPECT_THAT(vec, Each(AnyOf(StartsWith("Field"), StartsWith("Value"))));

  // Now run with filter on the results - we are expecting to not getting
  // any result at this point
  resp = Run({"hscan", "myhash", "0", "match", "*x*"});  // nothing should match this
  EXPECT_THAT(resp, ArrLen(2));
  vec = StrArray(resp.GetVec()[1]);
  EXPECT_EQ(vec.size(), 0);

  // now we will do a positive match - anything that has 1 on it
  resp = Run({"hscan", "myhash", "0", "match", "*1*"});
  EXPECT_THAT(resp, ArrLen(2));
  vec = StrArray(resp.GetVec()[1]);
  EXPECT_EQ(vec.size(), 2);  // key/value = 2

  // Test with large hash to see that count limit the number of entries
  for (int i = 0; i < 200; i++) {
    Run({"HSET", "largehash", absl::StrCat("KeyNum-", i), absl::StrCat("KeyValue-", i)});
  }
  resp = Run({"hscan", "largehash", "0", "count", "20"});
  EXPECT_THAT(resp, ArrLen(2));
  vec = StrArray(resp.GetVec()[1]);

  // See https://redis.io/commands/scan/ --> "The COUNT option", for why this cannot be exact
  EXPECT_GE(vec.size(), 40);  // This should be larger than (20 * 2) and less than about 50
  EXPECT_LT(vec.size(), 60);
}

TEST_F(HSetFamilyTest, HScanLpMatchBug) {
  Run({"HSET", "key", "1", "2"});
  auto resp = Run({"hscan", "key", "0", "match", "1"});
  EXPECT_THAT(resp, ArrLen(2));
}

TEST_F(HSetFamilyTest, HincrbyFloat) {
  Run({"hincrbyfloat", "k", "a", "1.5"});
  EXPECT_EQ(Run({"hget", "k", "a"}), "1.5");

  Run({"hincrbyfloat", "k", "a", "1.5"});
  EXPECT_EQ(Run({"hget", "k", "a"}), "3");

  for (size_t i = 0; i < 500; ++i) {
    Run({"hincrbyfloat", "k", absl::StrCat("v", i), "1.5"});
  }

  for (size_t i = 0; i < 500; ++i) {
    EXPECT_EQ(Run({"hget", "k", absl::StrCat("v", i)}), "1.5");
  }
}

TEST_F(HSetFamilyTest, HRandFloat) {
  Run({"HSET", "k", "1", "2"});

  EXPECT_EQ(Run({"hrandfield", "k"}), "1");

  for (size_t i = 0; i < 500; ++i) {
    Run({"hincrbyfloat", "k", absl::StrCat("v", i), "1.1"});
  }

  Run({"hrandfield", "k"});
}

TEST_F(HSetFamilyTest, HRandField) {
  // exercise Redis' listpack encoding
  Run({"HSET", "k", "a", "0", "b", "1", "c", "2"});

  EXPECT_THAT(Run({"hrandfield", "k"}), AnyOf("a", "b", "c"));

  EXPECT_THAT(Run({"hrandfield", "k", "2"}).GetVec(), IsSubsetOf({"a", "b", "c"}));

  EXPECT_THAT(Run({"hrandfield", "k", "3"}).GetVec(), UnorderedElementsAre("a", "b", "c"));

  EXPECT_THAT(Run({"hrandfield", "k", "4"}).GetVec(), UnorderedElementsAre("a", "b", "c"));

  auto resp = Run({"hrandfield", "k", "4", "withvalues"});
  EXPECT_THAT(resp, ArrLen(6));
  auto vec = resp.GetVec();

  std::vector<RespExpr> k, v;
  for (unsigned int i = 0; i < vec.size(); ++i) {
    if (i % 2 == 1)
      v.push_back(vec[i]);
    else
      k.push_back(vec[i]);
  }

  EXPECT_THAT(v, UnorderedElementsAre("0", "1", "2"));
  EXPECT_THAT(k, UnorderedElementsAre("a", "b", "c"));

  resp = Run({"hrandfield", "k", "-4", "withvalues"});
  EXPECT_THAT(resp, ArrLen(8));
  vec = resp.GetVec();
  k.clear();
  v.clear();
  for (unsigned int i = 0; i < vec.size(); ++i) {
    if (i % 2 == 0) {
      if (vec[i] == "a")
        EXPECT_EQ(vec[i + 1], "0");
      else if (vec[i] == "b")
        EXPECT_EQ(vec[i + 1], "1");
      else if (vec[i] == "c")
        EXPECT_EQ(vec[i + 1], "2");
      else
        ADD_FAILURE();
    }
  }

  // exercise Dragonfly's string map encoding
  int num_entries = 500;
  for (int i = 0; i < num_entries; i++) {
    Run({"HSET", "largehash", std::to_string(i), std::to_string(i * 10)});
  }

  resp = Run({"hrandfield", "largehash"});
  EXPECT_LE(stoi(resp.GetString()), num_entries - 1);
  EXPECT_GE(stoi(resp.GetString()), 0);

  resp = Run({"hrandfield", "largehash", std::to_string(num_entries / 2)});
  vec = resp.GetVec();
  std::vector<std::string> string_vec;
  for (auto v : vec) {
    string_vec.push_back(v.GetString());
  }

  sort(string_vec.begin(), string_vec.end());
  auto it = std::unique(string_vec.begin(), string_vec.end());
  bool is_unique = (it == string_vec.end());
  EXPECT_TRUE(is_unique);

  for (const auto& str : string_vec) {
    EXPECT_LE(stoi(str), num_entries - 1);
    EXPECT_GE(stoi(str), 0);
  }

  resp = Run({"hrandfield", "largehash", std::to_string(num_entries * -1 - 1)});
  EXPECT_THAT(resp, ArrLen(num_entries + 1));
  vec = resp.GetVec();

  string_vec.clear();
  for (auto v : vec) {
    string_vec.push_back(v.GetString());
    int i = stoi(v.GetString());
    EXPECT_LE(i, num_entries - 1);
    EXPECT_GE(i, 0);
  }

  sort(string_vec.begin(), string_vec.end());
  it = std::unique(string_vec.begin(), string_vec.end());
  is_unique = (it == string_vec.end());
  EXPECT_FALSE(is_unique);

  resp = Run({"hrandfield", "largehash", std::to_string(num_entries * -1 - 1), "withvalues"});
  EXPECT_THAT(resp, ArrLen((num_entries + 1) * 2));
  vec = resp.GetVec();

  string_vec.clear();
  for (unsigned int i = 0; i < vec.size(); ++i) {
    if (i % 2 == 0) {
      int k = stoi(vec[i].GetString());
      EXPECT_LE(k, num_entries - 1);
      EXPECT_GE(k, 0);
      int v = stoi(vec[i + 1].GetString());
      EXPECT_EQ(v, k * 10);
      string_vec.push_back(vec[i].GetString());
    }
  }

  sort(string_vec.begin(), string_vec.end());
  it = std::unique(string_vec.begin(), string_vec.end());
  is_unique = (it == string_vec.end());
  EXPECT_FALSE(is_unique);
}

TEST_F(HSetFamilyTest, HSetEx) {
  TEST_current_time_ms = kMemberExpiryBase * 1000;  // to reset to test time.

  EXPECT_THAT(Run({"HSETEX", "k", "1", "f", "v"}), IntArg(1));

  AdvanceTime(500);
  EXPECT_THAT(Run({"HGET", "k", "f"}), "v");

  AdvanceTime(500);
  EXPECT_THAT(Run({"HGET", "k", "f"}), ArgType(RespExpr::NIL));
}

TEST_F(HSetFamilyTest, TriggerConvertToStrMap) {
  const int kElements = 200;
  // Enough for IsGoodForListpack to become false
  for (size_t i = 0; i < kElements; i++) {
    auto k = absl::StrCat(100500700u + i);
    Run({"HSET", "hk", k, "100500700"});
  }
  EXPECT_THAT(Run({"HLEN", "hk"}), IntArg(kElements));
}

TEST_F(HSetFamilyTest, Issue1140) {
  Run({"HSET", "CaseKey", "Foo", "Bar"});

  EXPECT_EQ("Bar", Run({"HGET", "CaseKey", "Foo"}));
}

}  // namespace dfly
