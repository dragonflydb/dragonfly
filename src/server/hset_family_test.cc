// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/hset_family.h"

#include <absl/cleanup/cleanup.h>

#include <tuple>

extern "C" {
#include "redis/listpack.h"
#include "redis/sds.h"
}

#include "base/gtest.h"
#include "base/logging.h"
#include "core/detail/gen_utils.h"
#include "facade/facade_test.h"
#include "server/test_utils.h"

using namespace testing;
using namespace std;
using namespace util;
using namespace facade;

namespace dfly {

class HSetFamilyTest : public BaseFamilyTest {
 protected:
};

class HestFamilyTestProtocolVersioned : public HSetFamilyTest,
                                        public ::testing::WithParamInterface<string> {
 protected:
};

INSTANTIATE_TEST_SUITE_P(HestFamilyTestProtocolVersioned, HestFamilyTestProtocolVersioned,
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
  // Simulate HSET on mirror map
  {
    absl::flat_hash_map<string, string> mirror;  // mirror

    // Generate HSET commands and check how many new entries were added
    absl::InsecureBitGen gen{};
    while (mirror.size() < 600) {
      vector<string> cmd = {"HSET", "hash"};
      size_t new_values = 0;
      for (int i = 0; i < 20; i++) {
        string key = GetRandomHex(gen, 3);
        string value = GetRandomHex(gen, 20, 10);
        new_values += mirror.contains(key) ? 0 : 1;
        mirror[key] = value;

        cmd.emplace_back(key);
        cmd.emplace_back(value);
      }

      EXPECT_THAT(Run(cmd), IntArg(new_values));
    }

    // Verify consistency
    EXPECT_THAT(Run({"HLEN", "hash"}), IntArg(mirror.size()));
    for (const auto& [key, value] : mirror)
      EXPECT_EQ(Run({"HGET", "hash", key}), mirror[key]);
  }

  // HSet with same key twice
  Run({"HSET", "hash", "key1", "value1", "key1", "value2"});
  EXPECT_EQ(Run({"HGET", "hash", "key1"}), "value2");

  // Wrong value cases
  EXPECT_THAT(Run({"HSET", "key"}), ErrArg("wrong number of arguments"));
  EXPECT_THAT(Run({"HSET", "key", "key"}), ErrArg("wrong number of arguments"));
  EXPECT_THAT(Run({"HSET", "key", "key", "value", "key2"}), ErrArg("wrong number of arguments"));
}

TEST_F(HSetFamilyTest, HSetNX) {
  // Should create new field
  EXPECT_THAT(Run({"HSETNX", "hash", "key1", "value1"}), IntArg(1));
  EXPECT_EQ(Run({"HGET", "hash", "key1"}), "value1");

  // Should not overwrite
  EXPECT_THAT(Run({"HSETNX", "hash", "key1", "value2"}), IntArg(0));
  EXPECT_EQ(Run({"HGET", "hash", "key1"}), "value1");

  // Wrong value cases
  EXPECT_THAT(Run({"HSETNX", "key"}), ErrArg("wrong number of arguments"));
  EXPECT_THAT(Run({"HSET", "key", "key"}), ErrArg("wrong number of arguments"));
}

// Listpack handles integers separately, so create a mix of different types
TEST_F(HSetFamilyTest, MixedTypes) {
  absl::flat_hash_set<string> str_keys, int_keys;
  for (int i = 0; i < 100; i++) {
    auto key1 = absl::StrCat("s", i);
    auto key2 = absl::StrCat("i", i);
    Run({"HSET", "hash", key1, "VALUE", key2, "123456"});
    str_keys.emplace(key1);
    int_keys.emplace(key2);
  }

  for (string_view key : str_keys)
    EXPECT_EQ(Run({{"HGET", "hash", key}}), "VALUE");

  for (string_view key : int_keys) {
    EXPECT_EQ(Run({{"HGET", "hash", key}}), "123456");
    EXPECT_EQ(CheckedInt({"hincrby", "hash", key, "1"}), 123456 + 1);
  }
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

  resp = Run({"hmget", "x", "a", "c", "d", "d", "c", "a"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(resp.GetVec(),
              ElementsAre("1", "3", ArgType(RespExpr::NIL), ArgType(RespExpr::NIL), "3", "1"));

  resp = Run({"hgetall", "x"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(resp.GetVec(), ElementsAre("a", "1", "b", "2", "c", "3"));
}

TEST_F(HSetFamilyTest, HIncrBy) {
  int total = 10;
  // Check new field is created
  EXPECT_EQ(CheckedInt({"hincrby", "key", "field", "10"}), 10);
  EXPECT_EQ(Run({"hget", "key", "field"}), "10");
  // Simulate multiple additions
  for (int i = -100; i < 100; i += 7) {
    total += i;
    EXPECT_EQ(CheckedInt({"hincrby", "key", "field", to_string(i)}), total);
  }

  // Overflow
  Run({"hset", "key", "field2", to_string(numeric_limits<int64_t>::max() - 1)});
  EXPECT_THAT(Run({"hincrby", "key", "field2", "2"}), ErrArg("would overflow"));

  // Error case
  Run({"hset", "key", "a", " 1"});
  auto resp = Run({"hincrby", "key", "a", "10"});
  EXPECT_THAT(resp, ErrArg("hash value is not an integer"));
}

TEST_F(HSetFamilyTest, HIncrRespected) {
  Run({"hset", "key", "a", "1"});
  EXPECT_EQ(11, CheckedInt({"hincrby", "key", "a", "10"}));
  EXPECT_EQ(11, CheckedInt({"hget", "key", "a"}));
}

TEST_F(HSetFamilyTest, HIncrCmdsPreserveTtl) {
  Run({"hsetex", "key", "5", "a", "1"});
  EXPECT_EQ(5, CheckedInt({"fieldttl", "key", "a"}));
  EXPECT_EQ(2, CheckedInt({"hincrby", "key", "a", "1"}));
  EXPECT_EQ(5, CheckedInt({"fieldttl", "key", "a"}));

  // If the field has already expired by the time hincrby runs, the TTL is default
  AdvanceTime(5 * 1000);
  EXPECT_EQ(1, CheckedInt({"hincrby", "key", "a", "1"}));
  EXPECT_EQ(-1, CheckedInt({"fieldttl", "key", "a"}));

  Run({"hsetex", "key", "5", "fl", "1.1"});
  EXPECT_EQ(5, CheckedInt({"fieldttl", "key", "fl"}));
  EXPECT_EQ("2.2", Run({"hincrbyfloat", "key", "fl", "1.1"}));
}

TEST_F(HSetFamilyTest, HScan) {
  auto resp = Run("hscan non-existing-key 100 count 5");
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  ASSERT_THAT(resp.GetVec(), ElementsAre(ArgType(RespExpr::STRING), ArgType(RespExpr::ARRAY)));
  EXPECT_EQ(ToSV(resp.GetVec()[0].GetBuf()), "0");
  EXPECT_EQ(StrArray(resp.GetVec()[1]).size(), 0);

  for (int i = 0; i < 10; i++) {
    Run({"HSET", "myhash", absl::StrCat("Field-", i), absl::StrCat("Value-", i)});
  }

  // Note that even though this limit by 4, it would return more because
  // all fields are on listpack
  resp = Run({"hscan", "myhash", "0", "count", "4"});
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

  // Test NOVALUES option on 'myhash' (which has 10 items)
  resp = Run({"hscan", "myhash", "0", "NOVALUES"});
  EXPECT_THAT(resp, ArrLen(2));
  vec = StrArray(resp.GetVec()[1]);
  EXPECT_EQ(vec.size(), 10);
  EXPECT_THAT(vec, Each(StartsWith("Field")));  // Should contain "Field-X", but never "Value-X"
}

// Verifies that the NOVALUES flag functions correctly when combined with other arguments
// like MATCH and COUNT, ensuring values are suppressed even during filtered or limited scans.
TEST_F(HSetFamilyTest, HScan_NoValuesCombinations) {
  Run({"HSET", "h_combos", "user:1", "v1", "user:2", "v2", "admin:1", "v3"});

  // case 1: MATCH + NOVALUES
  // We want only keys starting with "user*", and NO values.
  auto resp = Run({"HSCAN", "h_combos", "0", "MATCH", "user:*", "NOVALUES"});
  ASSERT_THAT(resp, ArrLen(2));
  auto vec = StrArray(resp.GetVec()[1]);

  // Should find: "user:1", "user:2" (2 items)
  // Should NOT find: "admin:1" (filtered out)
  // Should NOT find: "v1", "v2" (values suppressed)
  EXPECT_EQ(vec.size(), 2);
  EXPECT_THAT(vec, UnorderedElementsAre("user:1", "user:2"));

  // case 2: COUNT + NOVALUES
  // Populate a larger hash to force scanning behavior, verify no values and only key present
  for (int i = 0; i < 50; ++i) {
    Run({"HSET", "h_large", absl::StrCat("k", i), "v"});
  }
  resp = Run({"HSCAN", "h_large", "0", "COUNT", "10", "NOVALUES"});
  vec = StrArray(resp.GetVec()[1]);
  EXPECT_GT(vec.size(), 0);
  EXPECT_THAT(vec, Not(Contains("v")));
  EXPECT_THAT(vec, Each(StartsWith("k")));
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

TEST_F(HSetFamilyTest, HincrbyFloatCornerCases) {
  Run({"hset", "k", "mhv", "-1.8E+308", "phv", "1.8E+308", "nd", "-+-inf", "+inf", "+inf", "nan",
       "nan", "-inf", "-inf"});
  // we don't support long doubles, so in all next cases we should return errors
  EXPECT_THAT(Run({"hincrbyfloat", "k", "mhv", "-1"}), ErrArg("ERR hash value is not a float"));
  EXPECT_THAT(Run({"hincrbyfloat", "k", "phv", "1"}), ErrArg("ERR hash value is not a float"));
  EXPECT_THAT(Run({"hincrbyfloat", "k", "nd", "1"}), ErrArg("ERR hash value is not a float"));
  EXPECT_THAT(Run({"hincrbyfloat", "k", "+inf", "1"}),
              ErrArg("increment would produce NaN or Infinity"));
  EXPECT_THAT(Run({"hincrbyfloat", "k", "nan", "1"}), ErrArg("ERR hash value is not a float"));
  EXPECT_THAT(Run({"hincrbyfloat", "k", "-inf", "1"}),
              ErrArg("increment would produce NaN or Infinity"));
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

  auto resp = Run({"HSETEX", "k", "1", "f", "v"});
  EXPECT_THAT(resp, IntArg(1));

  AdvanceTime(500);
  EXPECT_THAT(Run({"HGET", "k", "f"}), "v");

  AdvanceTime(500);
  EXPECT_THAT(Run({"HGET", "k", "f"}), ArgType(RespExpr::NIL));

  const std::string_view long_time = "100"sv;

  resp = Run({"HSETEX", "k", long_time, "field1", "value"});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"HSETEX", "k", long_time, "field1", "new_value"});
  EXPECT_THAT(resp, IntArg(0));

  resp = Run({"HGET", "k", "field1"});
  EXPECT_THAT(resp, "new_value");  // HSETEX without NX option; value was replaced by new_value

  resp = Run({"HSETEX", "k", long_time, "field2", "value"});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"HSETEX", "k", "NX", long_time, "field2", "new_value"});
  EXPECT_THAT(resp, IntArg(0));

  resp = Run({"HGET", "k", "field2"});
  EXPECT_THAT(resp, "value");  // HSETEX with NX option; value was NOT replaced by new_value

  const std::string_view short_time = "1"sv;

  resp = Run({"HSETEX", "k", long_time, "field3", "value"});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"HSETEX", "k", short_time, "field3", "value"});
  EXPECT_THAT(resp, IntArg(0));

  AdvanceTime(1000);
  resp = Run({"HGET", "k", "field3"});
  EXPECT_THAT(resp, ArgType(RespExpr::NIL));
  // HSETEX without NX option; old expiration time was replaced by a new one

  resp = Run({"HSETEX", "k", long_time, "field4", "value"});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"HSETEX", "k", "NX", short_time, "field4", "value"});
  EXPECT_THAT(resp, IntArg(0));

  AdvanceTime(1100);
  resp = Run({"HGET", "k", "field4"});
  EXPECT_THAT(resp,
              "value");  // HSETEX with NX option; old expiration time was NOT replaced by a new one

  // KEEPTTL related asserts
  EXPECT_THAT(Run({"HSETEX", "k", long_time, "kttlfield", "value"}), IntArg(1));
  EXPECT_EQ(Run({"HGET", "k", "kttlfield"}), "value");
  EXPECT_EQ(CheckedInt({"FIELDTTL", "k", "kttlfield"}), 100);

  // KEEPTTL resets value of kttlfield, but preserves its TTL. afield is added with TTL=1
  EXPECT_THAT(Run({"HSETEX", "k", "KEEPTTL", "1", "kttlfield", "resetvalue", "afield", "aval"}),
              IntArg(1));
  EXPECT_EQ(CheckedInt({"FIELDTTL", "k", "kttlfield"}), 100);
  EXPECT_EQ(Run({"FIELDTTL", "k", "afield"}).GetInt(), 1);
  EXPECT_EQ(Run({"HGET", "k", "afield"}), "aval");
  // make afield expire
  AdvanceTime(1000);
  EXPECT_THAT(Run({"HGET", "k", "afield"}), ArgType(RespExpr::NIL));

  // kttlfield is still present although with updated value
  EXPECT_EQ(Run({"HGET", "k", "kttlfield"}), "resetvalue");
  EXPECT_EQ(Run({"FIELDTTL", "k", "kttlfield"}).GetInt(), 99);

  // If NX is supplied, with or without KEEPTTL neither expiry nor value is updated
  EXPECT_THAT(Run({"HSETEX", "k", "NX", "KEEPTTL", "1", "kttlfield", "value"}), IntArg(0));

  // No updates
  EXPECT_EQ(Run({"HGET", "k", "kttlfield"}), "resetvalue");
  EXPECT_EQ(Run({"FIELDTTL", "k", "kttlfield"}).GetInt(), 99);

  EXPECT_THAT(Run({"HSETEX", "k", "NX", "1", "kttlfield", "value"}), IntArg(0));
  // No updates
  EXPECT_EQ(Run({"HGET", "k", "kttlfield"}), "resetvalue");
  EXPECT_EQ(Run({"FIELDTTL", "k", "kttlfield"}).GetInt(), 99);

  // Invalid TTL handling
  EXPECT_THAT(Run({"HSETEX", "k", "NX", "zero", "kttlfield", "value"}),
              ErrArg("ERR value is not an integer or out of range"));

  // Exercise the code path where a field is added without TTL, but then we set a new expiration AND
  // provide KEEPTTL. Since there was no old expiry, the new TTL should be applied.
  EXPECT_EQ(Run({"HSET", "k", "nottl", "val"}), 1);
  EXPECT_EQ(Run({"HSETEX", "k", "KEEPTTL", long_time, "nottl", "newval"}), 0);
  EXPECT_EQ(Run({"FIELDTTL", "k", "nottl"}).GetInt(), 100);

  EXPECT_THAT(Run({"HSETEX", "k", "NX", "KEEPTTL", "NX", "1", "v", "v2"}),
              ErrArg("ERR wrong number of arguments for 'hsetex' command"));
  EXPECT_THAT(Run({"HSETEX", "k", "KEEPTTL", "KEEPTTL", "1", "v", "v2"}),
              ErrArg("ERR wrong number of arguments for 'hsetex' command"));
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

TEST_F(HSetFamilyTest, Issue2102) {
  // Set key with element that will expire after 1s
  EXPECT_EQ(CheckedInt({"HSETEX", "key", "10", "k1", "v1"}), 1);
  AdvanceTime(10'000);
  EXPECT_THAT(Run({"HGETALL", "key"}), RespArray(ElementsAre()));
}

TEST_F(HSetFamilyTest, HExpire) {
  EXPECT_EQ(CheckedInt({"HSET", "key", "k0", "v0", "k1", "v1", "k2", "v2"}), 3);
  EXPECT_THAT(Run({"HEXPIRE", "key", "10", "FIELDS", "3", "k0", "k1", "k2"}),
              RespArray(ElementsAre(IntArg(1), IntArg(1), IntArg(1))));
  AdvanceTime(10'000);
  EXPECT_THAT(Run({"HGETALL", "key"}), RespArray(ElementsAre()));

  EXPECT_EQ(CheckedInt({"HSETEX", "key2", "60", "k0", "v0", "k1", "v2"}), 2);
  EXPECT_THAT(Run({"HEXPIRE", "key2", "10", "FIELDS", "2", "k0", "k1"}),
              RespArray(ElementsAre(IntArg(1), IntArg(1))));
  AdvanceTime(10'000);
  EXPECT_THAT(Run({"HGETALL", "key2"}), RespArray(ElementsAre()));

  EXPECT_EQ(CheckedInt({"HSET", "key3", "k0", "v0", "k1", "v1", "k2", "v2", "k3", "v3", "k4", "v4",
                        "k5", "v5"}),
            6);
  EXPECT_THAT(Run({"HEXPIRE", "key3", "10", "XX", "FIELDS", "1", "k0"}), IntArg(0));
  EXPECT_THAT(Run({"HEXPIRE", "key3", "10", "NX", "FIELDS", "1", "k0"}), IntArg(1));
  EXPECT_THAT(Run({"HEXPIRE", "key3", "10", "NX", "FIELDS", "1", "k0"}), IntArg(0));
  EXPECT_THAT(Run({"HEXPIRE", "key3", "10", "XX", "FIELDS", "1", "k0"}), IntArg(1));
  EXPECT_THAT(Run({"HEXPIRE", "key3", "10", "NX", "FIELDS", "3", "k1", "k2", "k3"}),
              RespArray(ElementsAre(IntArg(1), IntArg(1), IntArg(1))));
  EXPECT_THAT(Run({"HEXPIRE", "key3", "8", "GT", "FIELDS", "1", "k2"}), IntArg(0));
  EXPECT_THAT(Run({"HEXPIRE", "key3", "12", "GT", "FIELDS", "1", "k2"}), IntArg(1));
  EXPECT_THAT(Run({"HEXPIRE", "key3", "8", "LT", "FIELDS", "1", "k3"}), IntArg(1));
  EXPECT_THAT(Run({"HEXPIRE", "key3", "12", "LT", "FIELDS", "1", "k3"}), IntArg(0));
  EXPECT_THAT(Run({"HEXPIRE", "key3", "10", "GT", "FIELDS", "1", "k4"}), IntArg(0));
  EXPECT_THAT(Run({"HEXPIRE", "key3", "10", "LT", "FIELDS", "1", "k5"}), IntArg(1));
  AdvanceTime(8'000);
  EXPECT_THAT(
      Run({"HGETALL", "key3"}),
      RespArray(UnorderedElementsAre("k0", "v0", "k1", "v1", "k2", "v2", "k4", "v4", "k5", "v5")));
  AdvanceTime(2'000);
  EXPECT_THAT(Run({"HGETALL", "key3"}), RespArray(UnorderedElementsAre("k2", "v2", "k4", "v4")));
  AdvanceTime(2'000);
  EXPECT_THAT(Run({"HGETALL", "key3"}), RespArray(ElementsAre("k4", "v4")));

  EXPECT_THAT(Run({"HEXPIRE", "key3", "10", "FIELDS", "1", "k4"}), IntArg(1));
  EXPECT_THAT(Run({"HEXPIRE", "key3", "0", "XX", "FIELDS", "1", "k4"}), IntArg(2));
  EXPECT_THAT(Run({"HGETALL", "key3"}), RespArray(ElementsAre()));

  EXPECT_EQ(
      CheckedInt({"HSET", "key4", "k0", "v0", "k1", "v1", "k2", "v2", "k3", "v3", "k4", "v4"}), 5);
  EXPECT_THAT(Run({"HEXPIRE", "key4", "0", "NX", "FIELDS", "2", "k0", "k1"}),
              RespElementsAre(IntArg(2), IntArg(2)));
  EXPECT_THAT(Run({"HEXPIRE", "key4", "0", "LT", "FIELDS", "2", "k2", "k3"}),
              RespElementsAre(IntArg(2), IntArg(2)));

  EXPECT_THAT(Run({"HEXPIRE", "key4", "0", "XX", "FIELDS", "1", "k4"}), IntArg(0));
  EXPECT_THAT(Run({"HEXPIRE", "key4", "10", "NX", "FIELDS", "1", "k4"}), IntArg(1));
  EXPECT_THAT(Run({"HEXPIRE", "key4", "0", "NX", "FIELDS", "1", "k4"}), IntArg(0));
  EXPECT_THAT(Run({"HEXPIRE", "key4", "0", "GT", "FIELDS", "1", "k4"}), IntArg(0));
  EXPECT_THAT(Run({"HEXPIRE", "key4", "0", "FIELDS", "1", "k4"}), IntArg(2));
  EXPECT_THAT(Run({"HGETALL", "key4"}), RespArray(ElementsAre()));
}

TEST_F(HSetFamilyTest, HExpireNoExpireEarly) {
  EXPECT_EQ(CheckedInt({"HSET", "key", "k0", "v0", "k1", "v1"}), 2);
  EXPECT_THAT(Run({"HEXPIRE", "key", "10", "FIELDS", "2", "k0", "k1"}),
              RespArray(ElementsAre(IntArg(1), IntArg(1))));
  AdvanceTime(9'000);
  EXPECT_THAT(Run({"HGETALL", "key"}), RespArray(UnorderedElementsAre("k0", "v0", "k1", "v1")));
}

TEST_F(HSetFamilyTest, HExpireNoSuchField) {
  EXPECT_EQ(CheckedInt({"HSET", "key", "k0", "v0"}), 1);
  EXPECT_THAT(Run({"HEXPIRE", "key", "10", "FIELDS", "2", "k0", "k1"}),
              RespArray(ElementsAre(IntArg(1), IntArg(-2))));
}

TEST_F(HSetFamilyTest, HExpireNoSuchKey) {
  EXPECT_THAT(Run({"HEXPIRE", "key", "10", "FIELDS", "2", "k0", "k1"}),
              RespArray(ElementsAre(IntArg(-2), IntArg(-2))));
}

TEST_F(HSetFamilyTest, HExpireNoAddNew) {
  Run({"HEXPIRE", "key", "10", "FIELDS", "1", "k0"});
  EXPECT_THAT(Run({"HGETALL", "key"}), RespArray(ElementsAre()));
}

TEST_F(HSetFamilyTest, HExpireWithNullChar) {
  string val_with_null("test\0test", 9);
  Run({"HSET", "hash", "field", val_with_null});
  string expected_val("test\0test", 9);
  EXPECT_EQ(ToSV(Run({"HGET", "hash", "field"}).GetBuf()), expected_val);
  Run({"HEXPIRE", "hash", "15", "FIELDS", "1", "field"});
  EXPECT_EQ(ToSV(Run({"HGET", "hash", "field"}).GetBuf()), expected_val);
}

TEST_F(HSetFamilyTest, RandomFieldAllExpired) {
  for (int i = 0; i < 10; ++i) {
    EXPECT_EQ(CheckedInt({"HSETEX", "key", "10", absl::StrCat("k", i), "v"}), 1);
  }
  AdvanceTime(10'000);
  EXPECT_THAT(Run({"HRANDFIELD", "key"}), ArgType(RespExpr::NIL));
}

TEST_F(HSetFamilyTest, RandomField1NotExpired) {
  for (int i = 0; i < 10; ++i) {
    EXPECT_EQ(CheckedInt({"HSETEX", "key", "10", absl::StrCat("k", i), "v"}), 1);
  }
  EXPECT_EQ(CheckedInt({"HSET", "key", "keep", "v"}), 1);

  AdvanceTime(10'000);
  EXPECT_THAT(Run({"HRANDFIELD", "key"}), "keep");
}

TEST_F(HSetFamilyTest, EmptyHashBug) {
  EXPECT_THAT(Run({"HSET", "foo", "a_field", "a_value"}), IntArg(1));
  EXPECT_THAT(Run({"HSETEX", "foo", "1", "b_field", "b_value"}), IntArg(1));
  EXPECT_THAT(Run({"HDEL", "foo", "a_field"}), IntArg(1));

  AdvanceTime(4000);

  EXPECT_THAT(Run({"HGETALL", "foo"}), RespArray(ElementsAre()));
  EXPECT_THAT(Run({"EXISTS", "foo"}), IntArg(0));
}

TEST_F(HSetFamilyTest, ScanAfterExpireSet) {
  EXPECT_THAT(Run({"HSET", "aset", "afield", "avalue"}), IntArg(1));
  EXPECT_THAT(Run({"HEXPIRE", "aset", "1", "FIELDS", "1", "afield"}), IntArg(1));

  const auto resp = Run({"HSCAN", "aset", "0", "count", "100"});
  EXPECT_THAT(resp, ArrLen(2));

  const auto vec = StrArray(resp.GetVec()[1]);
  EXPECT_EQ(vec.size(), 2);

  EXPECT_THAT(vec, Contains("afield").Times(1));
  EXPECT_THAT(vec, Contains("avalue").Times(1));
}

TEST_F(HSetFamilyTest, KeyRemovedWhenEmpty) {
  auto test_cmd = [&](const std::function<void()>& f, const std::string_view tag) {
    EXPECT_THAT(Run({"HSET", "a", "afield", "avalue"}), IntArg(1));
    EXPECT_THAT(Run({"HEXPIRE", "a", "1", "FIELDS", "1", "afield"}), IntArg(1));
    AdvanceTime(1000);

    EXPECT_THAT(Run({"EXISTS", "a"}), IntArg(1));
    f();
    EXPECT_THAT(Run({"EXISTS", "a"}), IntArg(0)) << "failed when testing " << tag;
  };

  test_cmd([&] { EXPECT_THAT(Run({"HGET", "a", "afield"}), ArgType(RespExpr::NIL)); }, "HGET");
  test_cmd([&] { EXPECT_THAT(Run({"HGETALL", "a"}), RespArray(ElementsAre())); }, "HGETALL");
  test_cmd([&] { EXPECT_THAT(Run({"HDEL", "a", "afield"}), IntArg(0)); }, "HDEL");
  test_cmd([&] { EXPECT_THAT(Run({"HSCAN", "a", "0"}).GetVec()[0], "0"); }, "HSCAN");
  test_cmd([&] { EXPECT_THAT(Run({"HMGET", "a", "afield"}), ArgType(RespExpr::NIL)); }, "HMGET");
  test_cmd([&] { EXPECT_THAT(Run({"HEXISTS", "a", "afield"}), IntArg(0)); }, "HEXISTS");
  test_cmd([&] { EXPECT_THAT(Run({"HSTRLEN", "a", "afield"}), IntArg(0)); }, "HSTRLEN");
}

TEST_F(HSetFamilyTest, HRandFieldRespFormat) {
  absl::flat_hash_map<std::string, std::string> expected{
      {"a", "1"},
      {"b", "2"},
      {"c", "3"},
  };
  Run({"HELLO", "3"});
  EXPECT_THAT(Run({"HSET", "key", "a", "1", "b", "2", "c", "3"}), IntArg(3));
  auto resp = Run({"HRANDFIELD", "key", "3", "WITHVALUES"});
  EXPECT_THAT(resp, ArrLen(3));
  for (const auto& v : resp.GetVec()) {
    EXPECT_THAT(v, ArrLen(2));
    const auto& kv = v.GetVec();
    EXPECT_THAT(kv[0], AnyOf("a", "b", "c"));
    EXPECT_THAT(kv[1], expected[kv[0].GetView()]);
  }

  Run({"HELLO", "2"});
  resp = Run({"HRANDFIELD", "key", "3", "WITHVALUES"});
  EXPECT_THAT(resp, ArrLen(6));
  const auto& vec = resp.GetVec();
  for (size_t i = 0; i < vec.size(); i += 2) {
    EXPECT_THAT(vec[i], AnyOf("a", "b", "c"));
    EXPECT_THAT(vec[i + 1], expected[vec[i].GetView()]);
  }
}

// Make sure no "Zombie Key": HEXPIRE with TTL 0 must delete the key
// if the hash becomes empty. If the key remains (zombie), saving the RDB or running
// commands like EXISTS against it may lead to crashes or other incorrect behavior.
TEST_F(HSetFamilyTest, HExpireZeroTTL_DeletesKey) {
  constexpr auto kRdbFile = "zombie_test.rdb";
  auto cleanup = absl::MakeCleanup([kRdbFile] { std::ignore = remove(kRdbFile); });
  Run({"HSET", "zombie", "f", "v"});
  auto resp = Run({"HEXPIRE", "zombie", "0", "FIELDS", "1", "f"});
  EXPECT_THAT(resp, IntArg(2));
  EXPECT_EQ(0, CheckedInt({"EXISTS", "zombie"}));
  EXPECT_EQ(Run({"SAVE", "RDB", kRdbFile}), "OK");
}

// HINCRBYFLOAT with NaN on a non-existing key must not create a zombie empty hash.
// Before the fix, the key was left in the DB with an empty listpack, causing HRANDFIELD
// to crash with CHECK(lplen > 0 && lplen % 2 == 0).
TEST_F(HSetFamilyTest, HIncrByFloatNaNDoesNotCreateKey) {
  EXPECT_THAT(Run({"HINCRBYFLOAT", "key", "field", "nan"}),
              ErrArg("increment would produce NaN or Infinity"));
  EXPECT_EQ(0, CheckedInt({"EXISTS", "key"}));
  EXPECT_THAT(Run({"HRANDFIELD", "key"}), ArgType(RespExpr::NIL));
}

}  // namespace dfly
