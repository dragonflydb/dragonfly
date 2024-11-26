// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/string_family.h"

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

class StringFamilyTest : public BaseFamilyTest {
 protected:
};

vector<int64_t> ToIntArr(const RespExpr& e) {
  vector<int64_t> res;
  CHECK_EQ(e.type, RespExpr::ARRAY);
  const RespVec* vec = get<RespVec*>(e.u);
  for (auto a : *vec) {
    int64_t val;
    std::string_view s = ToSV(a.GetBuf());
    CHECK(absl::SimpleAtoi(s, &val)) << s;
    res.push_back(val);
  }

  return res;
}

TEST_F(StringFamilyTest, SetGet) {
  EXPECT_EQ(Run({"set", "key", "val"}), "OK");
  EXPECT_EQ(Run({"get", "key"}), "val");
  EXPECT_EQ(Run({"set", "key1", "1"}), "OK");
  EXPECT_EQ(Run({"get", "key1"}), "1");
  EXPECT_EQ(Run({"set", "key", "2"}), "OK");
  EXPECT_EQ(Run({"get", "key"}), "2");
  EXPECT_THAT(Run({"get", "key3"}), ArgType(RespExpr::NIL));

  auto metrics = GetMetrics();
  EXPECT_EQ(7, metrics.coordinator_stats.tx_normal_cnt);
  EXPECT_EQ(3, metrics.events.hits);
  EXPECT_EQ(1, metrics.events.misses);
  EXPECT_EQ(3, metrics.events.mutations);
}

TEST_F(StringFamilyTest, Incr) {
  ASSERT_EQ(Run({"set", "key", "0"}), "OK");
  ASSERT_THAT(Run({"incr", "key"}), IntArg(1));

  ASSERT_EQ(Run({"set", "key1", "123456789"}), "OK");
  ASSERT_THAT(Run({"incrby", "key1", "0"}), IntArg(123456789));

  ASSERT_EQ(Run({"set", "key1", "-123456789"}), "OK");
  ASSERT_THAT(Run({"incrby", "key1", "0"}), IntArg(-123456789));

  ASSERT_EQ(Run({"set", "key1", "   -123  "}), "OK");
  ASSERT_THAT(Run({"incrby", "key1", "1"}), ErrArg("ERR value is not an integer"));

  ASSERT_THAT(Run({"incrby", "ne", "0"}), IntArg(0));
  ASSERT_THAT(Run({"decrby", "a", "-9223372036854775808"}), ErrArg("overflow"));
  auto metrics = GetMetrics();
  EXPECT_EQ(10, metrics.events.mutations);
  EXPECT_EQ(0, metrics.events.misses);
  EXPECT_EQ(0, metrics.events.hits);
}

TEST_F(StringFamilyTest, Append) {
  Run({"setex", "key", "100", "val"});
  EXPECT_THAT(Run({"ttl", "key"}), IntArg(100));

  EXPECT_THAT(Run({"append", "key", "bar"}), IntArg(6));
  EXPECT_THAT(Run({"ttl", "key"}), IntArg(100));
}

TEST_F(StringFamilyTest, Expire) {
  ASSERT_EQ(Run({"set", "key", "val", "PX", "20"}), "OK");

  AdvanceTime(10);
  EXPECT_EQ(Run({"get", "key"}), "val");

  AdvanceTime(10);

  EXPECT_THAT(Run({"get", "key"}), ArgType(RespExpr::NIL));

  ASSERT_THAT(Run({"set", "i", "1", "PX", "10"}), "OK");
  ASSERT_THAT(Run({"incr", "i"}), IntArg(2));

  AdvanceTime(10);
  ASSERT_THAT(Run({"incr", "i"}), IntArg(1));
}

TEST_F(StringFamilyTest, Keepttl) {
  ASSERT_EQ(Run({"set", "key", "val", "EX", "100"}), "OK");
  ASSERT_EQ(Run({"set", "key", "val"}), "OK");
  auto resp = Run({"ttl", "key"});
  auto actual = get<int64_t>(resp.u);
  ASSERT_EQ(actual, -1);

  resp = Run({"set", "key", "val", "EX", "200"});
  ASSERT_EQ(Run({"set", "key", "val", "KEEPTTL"}), "OK");

  resp = Run({"ttl", "key"});
  actual = get<int64_t>(resp.u);

  EXPECT_TRUE(actual > 0 && actual <= 200);
}

TEST_F(StringFamilyTest, SetOptionsSyntaxError) {
  auto TEST_current_time_s = TEST_current_time_ms / 1000;

  EXPECT_THAT(Run({"set", "key", "val", "EX", "1030", "PX", "1030"}), ErrArg("ERR syntax error"));
  EXPECT_THAT(
      Run({"set", "key", "val", "EX", "1030", "EXAT", absl::StrCat(TEST_current_time_s + 1030)}),
      ErrArg("ERR syntax error"));
  EXPECT_THAT(
      Run({"set", "key", "val", "EX", "1030", "PXAT", absl::StrCat(TEST_current_time_ms + 1030)}),
      ErrArg("ERR syntax error"));

  EXPECT_THAT(Run({"set", "key", "val", "PX", "1030", "EX", "1030"}), ErrArg("ERR syntax error"));
  EXPECT_THAT(
      Run({"set", "key", "val", "PX", "1030", "EXAT", absl::StrCat(TEST_current_time_s + 1030)}),
      ErrArg("ERR syntax error"));
  EXPECT_THAT(
      Run({"set", "key", "val", "PX", "1030", "PXAT", absl::StrCat(TEST_current_time_ms + 1030)}),
      ErrArg("ERR syntax error"));
  EXPECT_THAT(
      Run({"set", "key", "val", "EXAT", absl::StrCat(TEST_current_time_s + 1030), "EX", "1030"}),
      ErrArg("ERR syntax error"));
  EXPECT_THAT(
      Run({"set", "key", "val", "EXAT", absl::StrCat(TEST_current_time_s + 1030), "PX", "1030"}),
      ErrArg("ERR syntax error"));
  EXPECT_THAT(Run({"set", "key", "val", "EXAT", absl::StrCat(TEST_current_time_s + 1030), "PXAT",
                   absl::StrCat(TEST_current_time_ms + 1030)}),
              ErrArg("ERR syntax error"));

  EXPECT_THAT(
      Run({"set", "key", "val", "PXAT", absl::StrCat(TEST_current_time_ms + 1030), "EX", "1030"}),
      ErrArg("ERR syntax error"));
  EXPECT_THAT(
      Run({"set", "key", "val", "PXAT", absl::StrCat(TEST_current_time_ms + 1030), "PX", "1030"}),
      ErrArg("ERR syntax error"));
  EXPECT_THAT(Run({"set", "key", "val", "PXAT", absl::StrCat(TEST_current_time_ms + 1030), "EXAT",
                   absl::StrCat(TEST_current_time_s + 1030)}),
              ErrArg("ERR syntax error"));

  EXPECT_THAT(Run({"set", "key", "val", "EX", "1030", "KEEPTTL"}), ErrArg("ERR syntax error"));
  EXPECT_THAT(Run({"set", "key", "val", "PX", "1030", "KEEPTTL"}), ErrArg("ERR syntax error"));
  EXPECT_THAT(
      Run({"set", "key", "val", "EXAT", absl::StrCat(TEST_current_time_s + 1030), "KEEPTTL"}),
      ErrArg("ERR syntax error"));
  EXPECT_THAT(
      Run({"set", "key", "val", "PXAT", absl::StrCat(TEST_current_time_ms + 1030), "KEEPTTL"}),
      ErrArg("ERR syntax error"));

  EXPECT_THAT(Run({"set", "key", "val", "KEEPTTL", "PX", "1030"}), ErrArg("ERR syntax error"));
  EXPECT_THAT(
      Run({"set", "key", "val", "KEEPTTL", "PXAT", absl::StrCat(TEST_current_time_ms + 1030)}),
      ErrArg("ERR syntax error"));
  EXPECT_THAT(Run({"set", "key", "val", "KEEPTTL", "EX", "1030"}), ErrArg("ERR syntax error"));
  EXPECT_THAT(
      Run({"set", "key", "val", "KEEPTTL", "EXAT", absl::StrCat(TEST_current_time_s + 1030)}),
      ErrArg("ERR syntax error"));

  EXPECT_THAT(Run({"set", "key", "val", "NX", "XX"}), ErrArg("ERR syntax error"));
  EXPECT_THAT(Run({"set", "key", "val", "XX", "NX"}), ErrArg("ERR syntax error"));

  EXPECT_THAT(Run({"set", "key", "val", "PX", "9223372036854775800"}),
              ErrArg("invalid expire time"));
  EXPECT_THAT(Run({"SET", "foo", "bar", "EX", "18446744073709561"}), ErrArg("invalid expire time"));
}

TEST_F(StringFamilyTest, Set) {
  auto resp = Run({"set", "foo", "bar", "XX"});
  EXPECT_THAT(resp, ArgType(RespExpr::NIL));

  resp = Run({"set", "foo", "bar", "NX"});
  ASSERT_THAT(resp, "OK");
  resp = Run({"set", "foo", "bar", "NX"});
  EXPECT_THAT(resp, ArgType(RespExpr::NIL));

  resp = Run({"set", "foo", "bar", "xx"});
  ASSERT_THAT(resp, "OK");

  resp = Run({"set", "foo", "bar", "ex", "abc"});
  ASSERT_THAT(resp, ErrArg(kInvalidIntErr));

  resp = Run({"set", "foo", "bar", "ex", "-1"});
  ASSERT_THAT(resp, ErrArg("invalid expire time"));

  resp = Run({"set", "foo", "bar", "ex", "1"});
  ASSERT_THAT(resp, "OK");

  ASSERT_THAT(Run({"sadd", "s1", "1"}), IntArg(1));
  ASSERT_THAT(Run({"set", "s1", "2"}), "OK");
}

TEST_F(StringFamilyTest, SetHugeKey) {
  const string key(36000000, 'b');
  auto resp = Run({"set", key, "1"});
  ASSERT_THAT(resp, "OK");
  Run({"del", key});
}

TEST_F(StringFamilyTest, MSetLong) {
  vector<string> command({"mset"});
  for (unsigned i = 0; i < 12000; ++i) {
    command.push_back(StrCat("key", i));
    command.push_back(StrCat("val", i));
  }
  auto resp = Run(absl::MakeSpan(command));
  EXPECT_EQ(resp, "OK");
}

TEST_F(StringFamilyTest, MGetSet) {
  Run({"mset", "z", "0"});         // single key
  auto resp = Run({"mget", "z"});  // single key
  EXPECT_THAT(resp, "0");

  Run({"mset", "x", "0", "b", "0"});

  ASSERT_EQ(2, GetDebugInfo("IO0").shards_count);

  auto mget_fb = pp_->at(0)->LaunchFiber([&] {
    for (size_t i = 0; i < 1000; ++i) {
      RespExpr resp = Run({"mget", "b", "x"});
      ASSERT_EQ(RespExpr::ARRAY, resp.type);
      auto ivec = ToIntArr(resp);

      ASSERT_GE(ivec[1], ivec[0]);
    }
  });

  auto set_fb = pp_->at(1)->LaunchFiber([&] {
    for (size_t i = 1; i < 2000; ++i) {
      Run({"set", "x", StrCat(i)});
      Run({"set", "b", StrCat(i)});
    }
  });

  mget_fb.Join();
  set_fb.Join();
}

TEST_F(StringFamilyTest, MGetCachingModeBug2276) {
  absl::FlagSaver fs;
  SetTestFlag("cache_mode", "true");
  ResetService();
  Run({"debug", "populate", "18000", "key", "32", "RAND"});

  // Scan starts traversing the database, because we populated the database with lots of items we
  // assume that scan will return items from the same bucket that reside next to each other.
  auto resp = Run({"scan", "0"});
  ASSERT_THAT(resp, ArrLen(2));
  StringVec vec = StrArray(resp.GetVec()[1]);
  ASSERT_GE(vec.size(), 10);

  auto get_bump_ups = [](const string& str) -> size_t {
    const string matcher = "bump_ups:";
    const auto pos = str.find(matcher) + matcher.size();
    const auto next_new_line =
        str.find("\r\n", pos);  // Find the position of the next "\r\n" after the initial position
    const auto sub = str.substr(pos, next_new_line - pos);
    return atoi(sub.c_str());
  };

  resp = Run({"info", "stats"});
  EXPECT_EQ(get_bump_ups(resp.GetString()), 0);

  auto mget_resp = StrArray(Run(
      {"mget", vec[0], vec[1], vec[2], vec[3], vec[4], vec[5], vec[6], vec[7], vec[8], vec[9]}));

  resp = Run({"info", "stats"});
  size_t bumps1 = get_bump_ups(resp.GetString());
  EXPECT_GT(bumps1, 0);
  EXPECT_LT(bumps1, 10);  // we assume that some bumps are blocked because items reside next to each
                          // other in the slot.

  for (int i = 0; i < 10; ++i) {
    auto get_resp = Run({"get", vec[i]});
    EXPECT_EQ(get_resp, mget_resp[i]);
  }

  resp = Run({"info", "stats"});
  size_t bumps2 = get_bump_ups(resp.GetString());
  EXPECT_GT(bumps2, bumps1);
}

TEST_F(StringFamilyTest, MGetCachingModeBug2465) {
  absl::FlagSaver fs;
  SetTestFlag("cache_mode", "true");
  ResetService();
  Run({"debug", "populate", "18000", "key", "32", "RAND"});

  // Scan starts traversing the database, because we populated the database with lots of items we
  // assume that scan will return items from the same bucket that reside next to each other.
  auto resp = Run({"scan", "0"});
  ASSERT_THAT(resp, ArrLen(2));
  StringVec vec = StrArray(resp.GetVec()[1]);
  ASSERT_GE(vec.size(), 10);

  auto get_bump_ups = [](const string& str) -> size_t {
    const string matcher = "bump_ups:";
    const auto pos = str.find(matcher) + matcher.size();
    const auto next_new_line =
        str.find("\r\n", pos);  // Find the position of the next "\r\n" after the initial position
    const auto sub = str.substr(pos, next_new_line - pos);
    return atoi(sub.c_str());
  };

  resp = Run({"info", "stats"});
  EXPECT_EQ(get_bump_ups(resp.GetString()), 0);

  Run({"del", vec[1]});
  Run({"lpush", vec[1], "a"});

  resp = Run({"get", vec[2]});
  string val = resp.GetString();
  auto mget_resp = StrArray(Run({"mget", vec[2], vec[2], vec[2]}));
  EXPECT_THAT(mget_resp, ElementsAre(val, val, val));

  resp = Run({"info", "stats"});
  size_t bumps = get_bump_ups(resp.GetString());
  EXPECT_EQ(bumps, 3);  // one bump for del and one for get and one for mget
}

TEST_F(StringFamilyTest, MSetGet) {
  Run({"mset", "x", "0", "y", "0", "a", "0", "b", "0"});
  ASSERT_EQ(2, GetDebugInfo().shards_count);

  Run({"mset", "x", "0", "y", "0"});
  ASSERT_EQ(1, GetDebugInfo().shards_count);

  Run({"mset", "x", "1", "b", "5", "x", "0"});
  ASSERT_EQ(2, GetDebugInfo().shards_count);

  int64_t val = CheckedInt({"get", "x"});
  EXPECT_EQ(0, val);

  val = CheckedInt({"get", "b"});
  EXPECT_EQ(5, val);

  auto mset_fb = pp_->at(0)->LaunchFiber([&] {
    for (size_t i = 0; i < 1000; ++i) {
      RespExpr resp = Run({"mset", "x", StrCat(i), "b", StrCat(i)});
      ASSERT_EQ(resp, "OK") << i;
    }
  });

  // A problematic order when mset is not atomic: set x, get x, get b (old), set b
  auto get_fb = pp_->at(2)->LaunchFiber([&] {
    for (size_t i = 0; i < 1000; ++i) {
      int64_t x = CheckedInt({"get", "x"});
      int64_t z = CheckedInt({"get", "b"});

      ASSERT_LE(x, z) << "Inconsistency at " << i;
    }
  });

  mset_fb.Join();
  get_fb.Join();
}

TEST_F(StringFamilyTest, MSetDel) {
  auto mset_fb = pp_->at(0)->LaunchFiber([&] {
    for (size_t i = 0; i < 1000; ++i) {
      Run({"mset", "x", "0", "z", "0"});
    }
  });

  auto del_fb = pp_->at(2)->LaunchFiber([&] {
    for (size_t i = 0; i < 1000; ++i) {
      CheckedInt({"del", "x", "z"});
    }
  });

  mset_fb.Join();
  del_fb.Join();
}

TEST_F(StringFamilyTest, IntKey) {
  Run({"mset", "1", "1", "-1000", "-1000"});
  auto resp = Run({"get", "1"});
  ASSERT_THAT(resp, "1");
}

TEST_F(StringFamilyTest, SingleShard) {
  Run({"mset", "x", "1", "y", "1"});
  ASSERT_EQ(1, GetDebugInfo("IO0").shards_count);

  Run({"mget", "x", "y", "b"});
  ASSERT_EQ(2, GetDebugInfo("IO0").shards_count);

  auto resp = Run({"mget", "x", "y"});
  ASSERT_EQ(1, GetDebugInfo("IO0").shards_count);
  ASSERT_THAT(ToIntArr(resp), ElementsAre(1, 1));

  auto mset_fb = pp_->at(0)->LaunchFiber([&] {
    for (size_t i = 0; i < 100; ++i) {
      Run({"mset", "x", "0", "y", "0"});
    }
  });

  // Specially multiple shards to avoid fast-path.
  auto mget_fb = pp_->at(1)->LaunchFiber([&] {
    for (size_t i = 0; i < 100; ++i) {
      Run({"mget", "x", "b", "y"});
    }
  });
  mset_fb.Join();
  mget_fb.Join();
}

TEST_F(StringFamilyTest, MSetIncr) {
  /*  serializable orders
   init: x=z=0

   mset x=z=1
   mset, incr x, incr z = 2, 2
   incr x, mset, incr z = 1, 2
   incr x, incr z, mset = 1, 1
*/

  /* unserializable scenario when mset is not atomic with respect to incr x
      set x, incr x, incr z, set z = 2, 1
    */

  Run({"mset", "a", "0", "b", "0", "c", "0"});
  ASSERT_EQ(2, GetDebugInfo("IO0").shards_count);

  auto mset_fb = pp_->at(0)->LaunchFiber([&] {
    for (size_t i = 1; i < 1000; ++i) {
      string base = StrCat(i * 900);
      auto resp = Run({"mset", "b", base, "a", base, "c", base});
      ASSERT_EQ(resp, "OK");
    }
  });

  auto get_fb = pp_->at(1)->LaunchFiber([&] {
    for (unsigned j = 0; j < 900; ++j) {
      int64_t a = CheckedInt({"incr", "a"});
      int64_t b = CheckedInt({"incr", "b"});
      ASSERT_LE(a, b);

      int64_t c = CheckedInt({"incr", "c"});
      if (a > c) {
        LOG(ERROR) << "Consistency error ";
      }
      ASSERT_LE(a, c);
    }
  });
  mset_fb.Join();
  get_fb.Join();
}

TEST_F(StringFamilyTest, SetEx) {
  ASSERT_EQ(Run({"setex", "key", "1", "val"}), "OK");
  ASSERT_EQ(Run({"setex", "key", "10", "val"}), "OK");
  ASSERT_THAT(Run({"ttl", "key"}), IntArg(10));
  ASSERT_THAT(Run({"setex", "key", "0", "val"}), ErrArg("invalid expire time"));
  ASSERT_EQ(Run({"setex", "key", StrCat(5 * 365 * 24 * 3600), "val"}), "OK");
  ASSERT_THAT(Run({"setex", "key", StrCat(1 << 30), "val"}), "OK");
  ASSERT_THAT(Run({"ttl", "key"}), IntArg(kMaxExpireDeadlineSec));
  ASSERT_THAT(Run({"SETEX", "foo", "18446744073709561", "bar"}), ErrArg("invalid expire time"));
}

TEST_F(StringFamilyTest, Range) {
  Run({"set", "key1", "Hello World"});
  EXPECT_EQ(Run({"getrange", "key1", "5", "3"}), "");

  Run({"SETRANGE", "key1", "6", "Earth"});
  EXPECT_EQ(Run({"get", "key1"}), "Hello Earth");

  Run({"SETRANGE", "key2", "2", "Earth"});
  EXPECT_EQ(Run({"get", "key2"}), string_view("\000\000Earth", 7));

  Run({"SETRANGE", "key3", "0", ""});
  EXPECT_EQ(0, CheckedInt({"exists", "key3"}));

  Run({"SETRANGE", "key3", "0", "abc"});
  EXPECT_EQ(1, CheckedInt({"exists", "key3"}));

  Run({"SET", "key3", "123"});
  EXPECT_EQ(Run({"getrange", "key3", "2", "3"}), "3");
  EXPECT_EQ(Run({"getrange", "key3", "3", "3"}), "");
  EXPECT_EQ(Run({"getrange", "key3", "4", "5"}), "");

  Run({"SET", "num", "1234"});
  EXPECT_EQ(Run({"getrange", "num", "3", "5000"}), "4");
  EXPECT_EQ(Run({"getrange", "num", "-5000", "10000"}), "1234");

  Run({"SET", "key4", "1"});
  EXPECT_EQ(Run({"getrange", "key4", "-1", "-2"}), "");
  EXPECT_EQ(Run({"getrange", "key4", "0", "-2"}), "1");

  EXPECT_EQ(CheckedInt({"SETRANGE", "key5", "1", ""}), 0);
  EXPECT_EQ(Run({"GET", "key5"}).type, facade::RespExpr::NIL);

  EXPECT_EQ(CheckedInt({"SETRANGE", "num", "6", ""}), 4);
  EXPECT_EQ(Run({"GET", "num"}), "1234");

  // we support only 256MB string so this test is failed now
  // EXPECT_THAT(CheckedInt({"SETRANGE", "", "268435456", "0"}), 268435457);
}

TEST_F(StringFamilyTest, IncrByFloat) {
  Run({"SET", "nonum", "  11"});
  auto resp = Run({"INCRBYFLOAT", "nonum", "1.0"});
  EXPECT_THAT(resp, ErrArg("not a valid float"));

  Run({"SET", "nonum", "11 "});
  resp = Run({"INCRBYFLOAT", "nonum", "1.0"});
  EXPECT_THAT(resp, ErrArg("not a valid float"));

  Run({"SET", "num", "2.566"});
  resp = Run({"INCRBYFLOAT", "num", "1.0"});
  EXPECT_EQ(resp, "3.566");
}

TEST_F(StringFamilyTest, SetNx) {
  // Make sure that we "screen out" invalid parameters for this command
  // this is important as it uses similar path as the "normal" set
  auto resp = Run({"setnx", "foo", "bar", "XX"});
  EXPECT_THAT(resp, ErrArg("wrong number of arguments"));

  resp = Run({"setnx", "foo", "bar", "NX"});
  ASSERT_THAT(resp, ErrArg("wrong number of arguments"));

  resp = Run({"setnx", "foo", "bar", "xx"});
  ASSERT_THAT(resp, ErrArg("wrong number of arguments"));

  resp = Run({"setnx", "foo", "bar", "ex", "abc"});
  ASSERT_THAT(resp, ErrArg("wrong number of arguments"));

  resp = Run({"setnx", "foo", "bar", "ex", "-1"});
  ASSERT_THAT(resp, ErrArg("wrong number of arguments"));

  resp = Run({"setnx", "foo", "bar", "ex", "1"});
  ASSERT_THAT(resp, ErrArg("wrong number of arguments"));

  // now let see how it goes for the valid parameters
  EXPECT_EQ(1, CheckedInt({"setnx", "foo", "bar"}));
  EXPECT_EQ(Run({"get", "foo"}), "bar");
  // second call to the same key should return 0 as we have it
  EXPECT_EQ(0, CheckedInt({"setnx", "foo", "hello"}));
  EXPECT_EQ(Run({"get", "foo"}), "bar");  // the value was not changed
}

TEST_F(StringFamilyTest, SetPxAtExAt) {
  // Expiration time as set at unix time
  auto TEST_current_time_s = TEST_current_time_ms / 1000;

  auto resp = Run({"set", "foo", "bar", "EXAT", "-1"});
  ASSERT_THAT(resp, ErrArg("invalid expire time"));
  resp = Run({"set", "foo", "bar", "EXAT", absl::StrCat(TEST_current_time_s - 1)});
  ASSERT_THAT(resp, "OK");  // it would return OK but will not set the value - expiration time is 0
                            // (checked with Redis)
  EXPECT_EQ(Run({"get", "foo"}).type, facade::RespExpr::NIL);

  resp = Run({"set", "foo", "bar", "PXAT", "-1"});
  ASSERT_THAT(resp, ErrArg("invalid expire time"));

  resp = Run({"set", "foo", "bar", "PXAT", absl::StrCat(TEST_current_time_ms - 23)});
  ASSERT_THAT(resp, "OK");  // it would return OK but will not set the value (checked with Redis)
  EXPECT_EQ(Run({"get", "foo"}).type, facade::RespExpr::NIL);

  resp = Run({"set", "foo", "bar", "EXAT", absl::StrCat(TEST_current_time_s + 1)});
  ASSERT_THAT(resp, "OK");  // valid expiration time
  EXPECT_EQ(Run({"get", "foo"}), "bar");

  resp = Run({"set", "foo2", "abc", "PXAT", absl::StrCat(TEST_current_time_ms + 300)});
  ASSERT_THAT(resp, "OK");
  EXPECT_EQ(Run({"get", "foo2"}), "abc");
}

TEST_F(StringFamilyTest, SetStick) {
  Run({"set", "foo", "bar", "STICK"});
  EXPECT_THAT(Run({"STICK", "foo"}), IntArg(0));
}

TEST_F(StringFamilyTest, GetDel) {
  auto resp = Run({"set", "foo", "bar"});
  EXPECT_THAT(resp, "OK");

  resp = Run({"getdel", "foo"});
  // foo's value
  ASSERT_THAT(resp, ArgType(RespExpr::STRING));

  resp = Run({"get", "foo"});
  ASSERT_THAT(resp, ArgType(RespExpr::NIL));
}

TEST_F(StringFamilyTest, GetEx) {
  auto resp = Run({"set", "foo", "bar"});
  EXPECT_THAT(resp, "OK");

  resp = Run({"getex", "foo", "EX"});
  EXPECT_THAT(resp, ErrArg("syntax error"));

  resp = Run({"getex", "foo", "EX", "1", "px", "1"});
  EXPECT_THAT(resp, ErrArg("syntax error"));

  resp = Run({"getex", "foo", "bar", "EX"});
  EXPECT_THAT(resp, ErrArg("syntax error"));

  resp = Run({"getex", "foo", "PERSIST", "1"});
  EXPECT_THAT(resp, ErrArg("syntax error"));

  resp = Run({"getex", "foo", "PXAT"});
  EXPECT_THAT(resp, ErrArg("syntax error"));

  resp = Run({"getex", "foo", "EX", "0"});
  EXPECT_THAT(resp, ErrArg("invalid expire time"));

  resp = Run({"getex", "foo", "PXAT", "-1"});
  EXPECT_THAT(resp, ErrArg("invalid expire time"));

  EXPECT_EQ(Run({"getex", "foo"}), "bar");

  resp = Run({"getex", "foo", "PERSIST"});
  EXPECT_EQ(resp, "bar");
  EXPECT_THAT(Run({"TTL", "foo"}), IntArg(-1));

  resp = Run({"getex", "foo", "pxat", absl::StrCat(TEST_current_time_ms - 1)});
  EXPECT_EQ(resp, "bar");

  EXPECT_THAT(Run({"getex", "foo"}), ArgType(RespExpr::NIL));

  Run({"set", "foo", "bar"});

  resp = Run({"getex", "foo", "PXAT", absl::StrCat(TEST_current_time_ms + 10)});
  EXPECT_EQ(resp, "bar");

  AdvanceTime(9);
  EXPECT_EQ(Run({"getex", "foo"}), "bar");

  AdvanceTime(1);
  EXPECT_THAT(Run({"getex", "foo"}), ArgType(RespExpr::NIL));

  Run({"set", "foo", "bar"});

  resp = Run({"getex", "foo", "exat", absl::StrCat(TEST_current_time_ms / 1000 - 1)});
  EXPECT_EQ(resp, "bar");
  EXPECT_THAT(Run({"getex", "foo"}), ArgType(RespExpr::NIL));

  Run({"set", "foo", "bar"});

  uint64_t next_two_seconds = TEST_current_time_ms + 2000;
  uint64_t next_two_seconds_round_down = static_cast<uint64_t>(next_two_seconds / 1000);
  uint64_t diff = next_two_seconds_round_down * 1000 - TEST_current_time_ms;

  resp = Run({"getex", "foo", "EXAT", absl::StrCat(next_two_seconds_round_down)});
  EXPECT_EQ(resp, "bar");

  AdvanceTime(diff - 1);
  EXPECT_EQ(Run({"getex", "foo"}), "bar");

  AdvanceTime(1);
  EXPECT_THAT(Run({"getex", "foo"}), ArgType(RespExpr::NIL));

  Run({"set", "foo", "bar"});

  resp = Run({"getex", "foo", "PX", "10"});

  AdvanceTime(9);
  EXPECT_EQ(Run({"getex", "foo"}), "bar");

  AdvanceTime(1);
  EXPECT_THAT(Run({"getex", "foo"}), ArgType(RespExpr::NIL));

  Run({"set", "foo", "bar"});

  resp = Run({"getex", "foo", "ex", "1"});

  AdvanceTime(999);
  EXPECT_EQ(Run({"getex", "foo"}), "bar");

  AdvanceTime(1);
  EXPECT_THAT(Run({"getex", "foo"}), ArgType(RespExpr::NIL));
}

TEST_F(StringFamilyTest, ClThrottle) {
  const int64_t limit = 5;
  const char* const key = "foo";
  const char* const max_burst = "4";  // limit - 1
  const char* const count = "1";
  const char* const period = "10";

  // You can never make a request larger than the maximum.
  auto resp = Run({"cl.throttle", key, max_burst, count, period, "6"});
  ASSERT_EQ(RespExpr::ARRAY, resp.type);
  ASSERT_THAT(resp.GetVec(),
              ElementsAre(IntArg(1), IntArg(limit), IntArg(5), IntArg(-1), IntArg(0)));

  // Rate limit normal requests appropriately.
  resp = Run({"cl.throttle", key, max_burst, count, period});
  ASSERT_EQ(RespExpr::ARRAY, resp.type);
  ASSERT_THAT(resp.GetVec(),
              ElementsAre(IntArg(0), IntArg(limit), IntArg(4), IntArg(-1), IntArg(11)));

  resp = Run({"cl.throttle", key, max_burst, count, period});
  ASSERT_EQ(RespExpr::ARRAY, resp.type);
  ASSERT_THAT(resp.GetVec(),
              ElementsAre(IntArg(0), IntArg(limit), IntArg(3), IntArg(-1), IntArg(21)));

  resp = Run({"cl.throttle", key, max_burst, count, period});
  ASSERT_EQ(RespExpr::ARRAY, resp.type);
  ASSERT_THAT(resp.GetVec(),
              ElementsAre(IntArg(0), IntArg(limit), IntArg(2), IntArg(-1), IntArg(31)));

  resp = Run({"cl.throttle", key, max_burst, count, period});
  ASSERT_EQ(RespExpr::ARRAY, resp.type);
  ASSERT_THAT(resp.GetVec(),
              ElementsAre(IntArg(0), IntArg(limit), IntArg(1), IntArg(-1), IntArg(41)));

  resp = Run({"cl.throttle", key, max_burst, count, period});
  ASSERT_EQ(RespExpr::ARRAY, resp.type);
  ASSERT_THAT(resp.GetVec(),
              ElementsAre(IntArg(0), IntArg(limit), IntArg(0), IntArg(-1), IntArg(51)));

  resp = Run({"cl.throttle", key, max_burst, count, period});
  ASSERT_EQ(RespExpr::ARRAY, resp.type);
  ASSERT_THAT(resp.GetVec(),
              ElementsAre(IntArg(1), IntArg(limit), IntArg(0), IntArg(11), IntArg(51)));

  AdvanceTime(30000);
  resp = Run({"cl.throttle", key, max_burst, count, period, "1"});
  ASSERT_EQ(RespExpr::ARRAY, resp.type);
  ASSERT_THAT(resp.GetVec(),
              ElementsAre(IntArg(0), IntArg(limit), IntArg(2), IntArg(-1), IntArg(31)));

  AdvanceTime(1000);
  resp = Run({"cl.throttle", key, max_burst, count, period, "1"});
  ASSERT_EQ(RespExpr::ARRAY, resp.type);
  ASSERT_THAT(resp.GetVec(),
              ElementsAre(IntArg(0), IntArg(limit), IntArg(1), IntArg(-1), IntArg(40)));

  AdvanceTime(9000);
  resp = Run({"cl.throttle", key, max_burst, count, period, "1"});
  ASSERT_EQ(RespExpr::ARRAY, resp.type);
  ASSERT_THAT(resp.GetVec(),
              ElementsAre(IntArg(0), IntArg(limit), IntArg(1), IntArg(-1), IntArg(41)));

  AdvanceTime(40000);
  resp = Run({"cl.throttle", key, max_burst, count, period, "1"});
  ASSERT_EQ(RespExpr::ARRAY, resp.type);
  ASSERT_THAT(resp.GetVec(),
              ElementsAre(IntArg(0), IntArg(limit), IntArg(4), IntArg(-1), IntArg(11)));

  AdvanceTime(15000);
  resp = Run({"cl.throttle", key, max_burst, count, period, "1"});
  ASSERT_EQ(RespExpr::ARRAY, resp.type);
  ASSERT_THAT(resp.GetVec(),
              ElementsAre(IntArg(0), IntArg(limit), IntArg(4), IntArg(-1), IntArg(11)));

  // Zero-volume request just peeks at the state.
  resp = Run({"cl.throttle", key, max_burst, count, period, "0"});
  ASSERT_EQ(RespExpr::ARRAY, resp.type);
  ASSERT_THAT(resp.GetVec(),
              ElementsAre(IntArg(0), IntArg(limit), IntArg(4), IntArg(-1), IntArg(11)));

  // High-volume request uses up more of the limit.
  resp = Run({"cl.throttle", key, max_burst, count, period, "2"});
  ASSERT_EQ(RespExpr::ARRAY, resp.type);
  ASSERT_THAT(resp.GetVec(),
              ElementsAre(IntArg(0), IntArg(limit), IntArg(2), IntArg(-1), IntArg(31)));

  // Large requests cannot exceed limits
  resp = Run({"cl.throttle", key, max_burst, count, period, "5"});
  ASSERT_EQ(RespExpr::ARRAY, resp.type);
  ASSERT_THAT(resp.GetVec(),
              ElementsAre(IntArg(1), IntArg(limit), IntArg(2), IntArg(31), IntArg(31)));

  // Zero rates aren't supported
  resp = Run({"cl.throttle", "bar", "10", "1", "0"});
  ASSERT_EQ(RespExpr::ERROR, resp.type);
  EXPECT_THAT(resp, ErrArg("zero rates are not supported"));

  // count == 0
  resp = Run({"cl.throttle", "bar", "10", "0", "1"});
  ASSERT_EQ(RespExpr::ERROR, resp.type);
  EXPECT_THAT(resp, ErrArg(kInvalidIntErr));
}

TEST_F(StringFamilyTest, SetMGetWithNilResp3) {
  Run({"hello", "3"});

  EXPECT_EQ(Run({"set", "key", "val"}), "OK");
  EXPECT_EQ(Run({"get", "key"}), "val");
  RespExpr resp = Run({"mget", "key", "nonexist"});
  ASSERT_EQ(RespExpr::ARRAY, resp.type);
  EXPECT_THAT(resp.GetVec(), ElementsAre("val", ArgType(RespExpr::NIL)));
}

TEST_F(StringFamilyTest, SetWithGetParam) {
  EXPECT_THAT(Run({"set", "key1", "val1", "get"}), ArgType(RespExpr::NIL));
  EXPECT_EQ(Run({"set", "key1", "val2", "get"}), "val1");

  EXPECT_THAT(Run({"set", "key2", "val2", "nx", "get"}), ArgType(RespExpr::NIL));
  EXPECT_THAT(Run({"set", "key2", "not used", "nx", "get"}), "val2");
  EXPECT_EQ(Run({"get", "key2"}), "val2");

  EXPECT_THAT(Run({"set", "key3", "not used", "xx", "get"}), ArgType(RespExpr::NIL));
  EXPECT_THAT(Run({"set", "key2", "val3", "xx", "get"}), "val2");
  EXPECT_EQ(Run({"get", "key2"}), "val3");

  EXPECT_THAT(Run({"sadd", "key4", "1"}), IntArg(1));
  EXPECT_THAT(Run({"set", "key4", "2", "get"}), ErrArg("wrong kind of value"));
  EXPECT_THAT(Run({"set", "key4", "2", "xx", "get"}), ErrArg("wrong kind of value"));
}

TEST_F(StringFamilyTest, SetWithHashtagsNoCluster) {
  SetTestFlag("cluster_mode", "");
  SetTestFlag("lock_on_hashtags", "false");
  ResetService();

  auto fb = ExpectUsedKeys({"{key}1"});
  EXPECT_EQ(Run({"set", "{key}1", "val1"}), "OK");
  fb.Join();
  EXPECT_FALSE(IsLocked(0, "{key}1"));

  fb = ExpectUsedKeys({"{key}2"});
  EXPECT_EQ(Run({"set", "{key}2", "val2"}), "OK");
  fb.Join();

  fb = ExpectUsedKeys({"{key}1", "{key}2"});
  EXPECT_THAT(Run({"mget", "{key}1", "{key}2"}), RespArray(ElementsAre("val1", "val2")));
  fb.Join();
  EXPECT_NE(1, GetDebugInfo().shards_count);
}

TEST_F(StringFamilyTest, SetWithHashtagsWithEmulatedCluster) {
  SetTestFlag("cluster_mode", "emulated");
  SetTestFlag("lock_on_hashtags", "false");
  ResetService();

  auto fb = ExpectUsedKeys({"{key}1"});
  EXPECT_EQ(Run({"set", "{key}1", "val1"}), "OK");
  fb.Join();

  fb = ExpectUsedKeys({"{key}2"});
  EXPECT_EQ(Run({"set", "{key}2", "val2"}), "OK");
  fb.Join();

  fb = ExpectUsedKeys({"{key}1", "{key}2"});
  EXPECT_THAT(Run({"mget", "{key}1", "{key}2"}), RespArray(ElementsAre("val1", "val2")));
  fb.Join();
  EXPECT_EQ(1, GetDebugInfo().shards_count);
}

TEST_F(StringFamilyTest, SetWithHashtagsWithHashtagLock) {
  SetTestFlag("cluster_mode", "emulated");
  SetTestFlag("lock_on_hashtags", "true");
  ResetService();

  auto fb = ExpectUsedKeys({"key"});
  EXPECT_EQ(Run({"set", "{key}1", "val1"}), "OK");
  fb.Join();

  fb = ExpectUsedKeys({"key"});
  EXPECT_EQ(Run({"set", "{key}2", "val2"}), "OK");
  fb.Join();

  fb = ExpectUsedKeys({"key"});
  EXPECT_THAT(Run({"mget", "{key}1", "{key}2"}), RespArray(ElementsAre("val1", "val2")));
  fb.Join();
  EXPECT_EQ(1, GetDebugInfo().shards_count);
}

TEST_F(StringFamilyTest, MultiSetWithHashtagsDontLockHashtags) {
  SetTestFlag("cluster_mode", "");
  SetTestFlag("lock_on_hashtags", "false");
  ResetService();

  auto fb = ExpectUsedKeys({"{key}1", "{key}2", "{key}3"});

  EXPECT_EQ(Run({"multi"}), "OK");
  EXPECT_EQ(Run({"set", "{key}1", "val1"}), "QUEUED");
  EXPECT_EQ(Run({"set", "{key}2", "val2"}), "QUEUED");
  EXPECT_EQ(Run({"eval", "return redis.call('set', KEYS[1], 'val3')", "1", "{key}3"}), "QUEUED");
  EXPECT_THAT(Run({"exec"}), RespArray(ElementsAre("OK", "OK", "OK")));
  fb.Join();
}

TEST_F(StringFamilyTest, MultiSetWithHashtagsLockHashtags) {
  SetTestFlag("cluster_mode", "emulated");
  SetTestFlag("lock_on_hashtags", "true");
  ResetService();

  auto fb = ExpectUsedKeys({"key"});

  EXPECT_EQ(Run({"multi"}), "OK");
  EXPECT_EQ(Run({"set", "{key}1", "val1"}), "QUEUED");
  EXPECT_EQ(Run({"set", "{key}2", "val2"}), "QUEUED");
  EXPECT_EQ(Run({"eval", "return redis.call('set', KEYS[1], 'val3')", "1", "{key}3"}), "QUEUED");
  EXPECT_THAT(Run({"exec"}), RespArray(ElementsAre("OK", "OK", "OK")));
  fb.Join();
}

TEST_F(StringFamilyTest, EmptyKeys) {
  EXPECT_EQ(0, CheckedInt({"strlen", "foo"}));
  EXPECT_EQ(Run({"SUBSTR", "foo", "0", "-1"}), "");
}

}  // namespace dfly
