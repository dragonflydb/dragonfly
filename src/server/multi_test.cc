// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <absl/flags/reflection.h>
#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>

#include "base/flags.h"
#include "base/gtest.h"
#include "base/logging.h"
#include "core/interpreter.h"
#include "facade/facade_test.h"
#include "server/conn_context.h"
#include "server/main_service.h"
#include "server/test_utils.h"
#include "server/transaction.h"

ABSL_DECLARE_FLAG(bool, multi_exec_squash);
ABSL_DECLARE_FLAG(bool, lua_auto_async);
ABSL_DECLARE_FLAG(bool, lua_allow_undeclared_auto_correct);
ABSL_DECLARE_FLAG(std::string, default_lua_flags);

namespace dfly {

using namespace std;
using namespace util;
using absl::StrCat;
using ::io::Result;
using testing::ElementsAre;
using testing::HasSubstr;

namespace {

constexpr unsigned kPoolThreadCount = 4;

const char kKey1[] = "x";
const char kKey2[] = "b";
const char kKey3[] = "c";
const char kKey4[] = "y";

const char kKeySid0[] = "x";
const char kKeySid1[] = "c";
const char kKeySid2[] = "b";

}  // namespace

// This test is responsible for server and main service
// (connection, transaction etc) families.
class MultiTest : public BaseFamilyTest {
 protected:
  MultiTest() : BaseFamilyTest() {
    num_threads_ = kPoolThreadCount;
  }
};

// Check constants are valid.
TEST_F(MultiTest, VerifyConstants) {
  Run({"mget", kKeySid0, kKeySid1, kKeySid2});
  ASSERT_EQ(3, GetDebugInfo().shards_count);
}

TEST_F(MultiTest, MultiAndFlush) {
  RespExpr resp = Run({"multi"});
  ASSERT_EQ(resp, "OK");

  resp = Run({"get", kKey1});
  ASSERT_EQ(resp, "QUEUED");

  EXPECT_THAT(Run({"FLUSHALL"}), ErrArg("'FLUSHALL' inside MULTI is not allowed"));
}

TEST_F(MultiTest, MultiWithError) {
  EXPECT_THAT(Run({"exec"}), ErrArg("EXEC without MULTI"));
  EXPECT_THAT(Run({"multi"}), "OK");
  EXPECT_THAT(Run({"set", "x", "y"}), "QUEUED");
  EXPECT_THAT(Run({"set", "x"}), ErrArg("wrong number of arguments for 'set' command"));
  EXPECT_THAT(Run({"exec"}), ErrArg("EXECABORT Transaction discarded because of previous errors"));

  EXPECT_THAT(Run({"multi"}), "OK");
  EXPECT_THAT(Run({"set", "z", "y"}), "QUEUED");
  EXPECT_THAT(Run({"exec"}), "OK");

  EXPECT_THAT(Run({"get", "x"}), ArgType(RespExpr::NIL));
  EXPECT_THAT(Run({"get", "z"}), "y");
}

TEST_F(MultiTest, Multi) {
  RespExpr resp = Run({"multi"});
  ASSERT_EQ(resp, "OK");

  resp = Run({"get", kKey1});
  ASSERT_EQ(resp, "QUEUED");

  resp = Run({"get", kKey4});
  ASSERT_EQ(resp, "QUEUED");

  resp = Run({"exec"});
  ASSERT_THAT(resp, ArrLen(2));
  ASSERT_THAT(resp.GetVec(), ElementsAre(ArgType(RespExpr::NIL), ArgType(RespExpr::NIL)));

  atomic_bool tx_empty = true;
  shard_set->RunBriefInParallel([&](EngineShard* shard) {
    if (!shard->txq()->Empty())
      tx_empty.store(false);
  });
  EXPECT_TRUE(tx_empty);

  resp = Run({"get", kKey4});
  ASSERT_THAT(resp, ArgType(RespExpr::NIL));

  ASSERT_FALSE(IsLocked(0, kKey1));
  ASSERT_FALSE(IsLocked(0, kKey4));
  ASSERT_FALSE(service_->IsShardSetLocked());
}

TEST_F(MultiTest, MultiGlobalCommands) {
  ASSERT_THAT(Run({"set", "key", "val"}), "OK");

  ASSERT_THAT(Run({"multi"}), "OK");
  ASSERT_THAT(Run({"move", "key", "2"}), "QUEUED");
  ASSERT_THAT(Run({"save"}), "QUEUED");

  RespExpr resp = Run({"exec"});
  ASSERT_THAT(resp, ArrLen(2));

  ASSERT_THAT(Run({"get", "key"}), ArgType(RespExpr::NIL));

  ASSERT_THAT(Run({"select", "2"}), "OK");
  ASSERT_THAT(Run({"get", "key"}), "val");

  ASSERT_FALSE(IsLocked(0, "key"));
  ASSERT_FALSE(IsLocked(2, "key"));
}

TEST_F(MultiTest, HitMissStats) {
  RespExpr resp = Run({"set", "Key1", "VAL"});
  ASSERT_EQ(resp, "OK");

  resp = Run({"get", "Key1"});
  ASSERT_EQ(resp, "VAL");

  resp = Run({"get", "Key2"});
  ASSERT_THAT(resp, ArgType(RespExpr::NIL));

  auto metrics = GetMetrics();
  EXPECT_THAT(metrics.events.hits, 1);
  EXPECT_THAT(metrics.events.misses, 1);
}

TEST_F(MultiTest, MultiEmpty) {
  RespExpr resp = Run({"multi"});
  ASSERT_EQ(resp, "OK");
  resp = Run({"exec"});
  EXPECT_THAT(resp, ArrLen(0));
  EXPECT_FALSE(service_->IsShardSetLocked());

  Run({"multi"});
  ASSERT_EQ(Run({"ping", "foo"}), "QUEUED");
  resp = Run({"exec"});
  EXPECT_EQ(resp, "foo");

  Run({"multi"});
  Run({"set", "a", ""});
  resp = Run({"exec"});
  EXPECT_EQ(resp, "OK");

  resp = Run({"get", "a"});
  EXPECT_EQ(resp, "");
}

TEST_F(MultiTest, MultiSeq) {
  RespExpr resp = Run({"multi"});
  ASSERT_EQ(resp, "OK");

  resp = Run({"set", kKey1, absl::StrCat(1)});
  ASSERT_EQ(resp, "QUEUED");
  resp = Run({"get", kKey1});
  ASSERT_EQ(resp, "QUEUED");
  resp = Run({"mget", kKey1, kKey4});
  ASSERT_EQ(resp, "QUEUED");
  resp = Run({"exec"});

  ASSERT_FALSE(IsLocked(0, kKey1));
  ASSERT_FALSE(IsLocked(0, kKey4));
  ASSERT_FALSE(service_->IsShardSetLocked());

  ASSERT_THAT(resp, ArrLen(3));
  const auto& arr = resp.GetVec();
  EXPECT_THAT(arr, ElementsAre("OK", "1", ArrLen(2)));

  ASSERT_THAT(arr[2].GetVec(), ElementsAre("1", ArgType(RespExpr::NIL)));
}

TEST_F(MultiTest, MultiConsistent) {
  Run({"mset", kKey1, "base", kKey4, "base"});

  auto mset_fb = pp_->at(0)->LaunchFiber([&] {
    for (size_t i = 1; i < 10; ++i) {
      string base = StrCat(i * 900);
      RespExpr resp = Run({"mset", kKey1, base, kKey4, base});
      ASSERT_EQ(resp, "OK");
    }
  });

  auto fb = pp_->at(1)->LaunchFiber([&] {
    RespExpr resp = Run({"multi"});
    ASSERT_EQ(resp, "OK");
    ThisFiber::SleepFor(1ms);

    resp = Run({"get", kKey1});
    ASSERT_EQ(resp, "QUEUED");

    resp = Run({"get", kKey4});
    ASSERT_EQ(resp, "QUEUED");

    resp = Run({"mget", kKey4, kKey1});
    ASSERT_EQ(resp, "QUEUED");

    resp = Run({"exec"});
    ASSERT_THAT(resp, ArrLen(3));
    const RespVec& resp_arr = resp.GetVec();
    ASSERT_THAT(resp_arr, ElementsAre(ArgType(RespExpr::STRING), ArgType(RespExpr::STRING),
                                      ArgType(RespExpr::ARRAY)));
    ASSERT_EQ(resp_arr[0].GetBuf(), resp_arr[1].GetBuf());
    const RespVec& sub_arr = resp_arr[2].GetVec();
    EXPECT_THAT(sub_arr, ElementsAre(ArgType(RespExpr::STRING), ArgType(RespExpr::STRING)));
    EXPECT_EQ(sub_arr[0].GetBuf(), sub_arr[1].GetBuf());
    EXPECT_EQ(sub_arr[0].GetBuf(), resp_arr[0].GetBuf());
  });

  mset_fb.Join();
  fb.Join();

  ASSERT_FALSE(IsLocked(0, kKey1));
  ASSERT_FALSE(IsLocked(0, kKey4));
  ASSERT_FALSE(service_->IsShardSetLocked());
}

TEST_F(MultiTest, MultiConsistent2) {
  const int kKeyCount = 50;
  const int kRuns = 50;
  const int kJobs = 20;

  vector<string> all_keys(kKeyCount);
  for (size_t i = 0; i < kKeyCount; i++)
    all_keys[i] = absl::StrCat("key", i);

  auto cb = [&](string id) {
    for (size_t r = 0; r < kRuns; r++) {
      size_t num_keys = (rand() % 5) + 1;
      set<string_view> keys;
      for (size_t i = 0; i < num_keys; i++)
        keys.insert(all_keys[rand() % kKeyCount]);

      Run(id, {"MULTI"});
      for (auto key : keys)
        Run(id, {"INCR", key});
      for (auto key : keys)
        Run(id, {"DECR", key});
      auto resp = Run(id, {"EXEC"});

      ASSERT_EQ(resp.GetVec().size(), keys.size() * 2);
      for (size_t i = 0; i < keys.size(); i++) {
        EXPECT_EQ(resp.GetVec()[i].GetInt(), optional<int64_t>(1));
        EXPECT_EQ(resp.GetVec()[i + keys.size()].GetInt(), optional<int64_t>(0));
      }
    }
  };

  vector<Fiber> fbs(kJobs);
  for (size_t i = 0; i < kJobs; i++) {
    fbs[i] = pp_->at(i % pp_->size())->LaunchFiber([i, cb]() { cb(absl::StrCat("worker", i)); });
  }

  for (auto& fb : fbs)
    fb.Join();
}

TEST_F(MultiTest, MultiRename) {
  RespExpr resp = Run({"mget", kKey1, kKey4});
  ASSERT_EQ(1, GetDebugInfo().shards_count);

  resp = Run({"multi"});
  ASSERT_EQ(resp, "OK");
  Run({"set", kKey1, "1"});

  resp = Run({"rename", kKey1, kKey4});
  ASSERT_EQ(resp, "QUEUED");
  resp = Run({"exec"});

  ASSERT_THAT(resp, ArrLen(2));
  EXPECT_THAT(resp.GetVec(), ElementsAre("OK", "OK"));

  // Now rename with keys spawning multiple shards.
  Run({"mget", kKey4, kKey2});
  ASSERT_EQ(2, GetDebugInfo().shards_count);

  Run({"multi"});
  resp = Run({"rename", kKey4, kKey2});
  ASSERT_EQ(resp, "QUEUED");
  resp = Run({"exec"});
  EXPECT_EQ(resp, "OK");

  EXPECT_FALSE(IsLocked(0, kKey1));
  EXPECT_FALSE(IsLocked(0, kKey2));
  EXPECT_FALSE(IsLocked(0, kKey4));
  EXPECT_FALSE(service_->IsShardSetLocked());
}

TEST_F(MultiTest, MultiHop) {
  Run({"set", kKey1, "1"});

  auto p1_fb = pp_->at(1)->LaunchFiber([&] {
    for (int i = 0; i < 100; ++i) {
      auto resp = Run({"rename", kKey1, kKey2});
      ASSERT_EQ(resp, "OK");
      EXPECT_EQ(2, GetDebugInfo("IO1").shards_count);

      resp = Run({"rename", kKey2, kKey1});
      ASSERT_EQ(resp, "OK");
    }
  });

  // mset should be executed either as ooo or via tx-queue because previous transactions
  // have been unblocked and executed as well. In other words, this mset should never block
  // on serializability constraints.
  auto p2_fb = pp_->at(2)->LaunchFiber([&] {
    for (int i = 0; i < 100; ++i) {
      Run({"mset", kKey3, "1", kKey4, "2"});
    }
  });

  p1_fb.Join();
  p2_fb.Join();
}

TEST_F(MultiTest, FlushDb) {
  Run({"mset", kKey1, "1", kKey4, "2"});
  auto resp = Run({"flushdb"});
  ASSERT_EQ(resp, "OK");

  auto fb0 = pp_->at(0)->LaunchFiber([&] {
    for (unsigned i = 0; i < 100; ++i) {
      Run({"flushdb"});
    }
  });

  pp_->at(1)->Await([&] {
    for (unsigned i = 0; i < 100; ++i) {
      Run({"mset", kKey1, "1", kKey4, "2"});
      int64_t ival = CheckedInt({"exists", kKey1, kKey4});
      ASSERT_TRUE(ival == 0 || ival == 2) << i << " " << ival;
    }
  });

  fb0.Join();

  ASSERT_FALSE(IsLocked(0, kKey1));
  ASSERT_FALSE(IsLocked(0, kKey4));
  ASSERT_FALSE(service_->IsShardSetLocked());
}

// Triggers a false possitive and therefore we turn it off
// There seem not to be a good solution to handle these false positives
// since sanitizers work well with u_context which is *very* slow
#ifndef SANITIZERS
TEST_F(MultiTest, Eval) {
  if (auto config = absl::GetFlag(FLAGS_default_lua_flags); config != "") {
    GTEST_SKIP() << "Skipped Eval test because default_lua_flags is set";
    return;
  }
  absl::FlagSaver saver;
  absl::SetFlag(&FLAGS_lua_allow_undeclared_auto_correct, true);

  RespExpr resp;

  resp = Run({"incrby", "foo", "42"});
  EXPECT_THAT(resp, IntArg(42));

  // first time running the script will return error and will change the script flag to allow
  // undeclared
  resp = Run({"eval", "return redis.call('get', 'foo')", "0"});
  EXPECT_THAT(resp, ErrArg("undeclared"));

  // running the same script the second time will succeed
  resp = Run({"eval", "return redis.call('get', 'foo')", "0"});
  EXPECT_THAT(resp, "42");

  Run({"script", "flush"});  // Reset global flag due to lua_allow_undeclared_auto_correct effect

  resp = Run({"eval", "return redis.call('get', 'foo')", "1", "bar"});
  EXPECT_THAT(resp, ErrArg("undeclared"));
  ASSERT_FALSE(IsLocked(0, "foo"));

  Run({"script", "flush"});  // Reset global flag from autocorrect

  resp = Run({"eval", "return redis.call('get', 'foo')", "1", "foo"});
  EXPECT_THAT(resp, "42");
  ASSERT_FALSE(IsLocked(0, "foo"));

  resp = Run({"eval", "return redis.call('get', KEYS[1])", "1", "foo"});
  EXPECT_THAT(resp, "42");
  ASSERT_FALSE(IsLocked(0, "foo"));
  ASSERT_FALSE(service_->IsShardSetLocked());

  resp = Run({"eval", "return 77", "2", "foo", "zoo"});
  EXPECT_THAT(resp, IntArg(77));

  // a,b important here to spawn multiple shards.
  resp = Run({"eval", "return redis.call('exists', KEYS[2])", "2", "a", "b"});
  // EXPECT_EQ(2, GetDebugInfo().shards_count);
  EXPECT_THAT(resp, IntArg(0));

  resp = Run({"eval", "return redis.call('hmset', KEYS[1], 'f1', '2222')", "1", "hmap"});
  EXPECT_EQ(resp, "OK");

  resp = Run({"hvals", "hmap"});
  EXPECT_EQ(resp, "2222");

  Run({"sadd", "s1", "a", "b"});
  Run({"sadd", "s2", "a", "c"});
  resp = Run({"eval", "return redis.call('SUNION', KEYS[1], KEYS[2])", "2", "s1", "s2"});
  ASSERT_THAT(resp, ArrLen(3));
  const auto& arr = resp.GetVec();
  EXPECT_THAT(arr, ElementsAre("a", "b", "c"));

  Run({"zadd", "z1", "123", "a", "12345678912345", "b", "12.5", "c"});
  const char* kGetScore = "return redis.call('ZSCORE', KEYS[1], ARGV[1]) .. '-works'";

  resp = Run({"eval", kGetScore, "1", "z1", "a"});
  EXPECT_EQ(resp, "123-works");
  resp = Run({"eval", kGetScore, "1", "z1", "b"});
  EXPECT_EQ(resp, "12345678912345-works");
  resp = Run({"eval", kGetScore, "1", "z1", "c"});
  EXPECT_EQ(resp, "12.5-works");

  // Multiple calls in a Lua script
  EXPECT_EQ(Run({"eval",
                 R"(redis.call('set', 'foo', '42')
                    return redis.call('get', 'foo'))",
                 "1", "foo"}),
            "42");

  auto condition = [&]() { return IsLocked(0, "foo"); };
  auto fb = ExpectConditionWithSuspension(condition);
  EXPECT_EQ(Run({"eval",
                 R"(redis.call('set', 'foo', '42')
                    return redis.call('get', 'foo'))",
                 "1", "foo"}),
            "42");
  fb.Join();

  // Call multi-shard command scan from single shard mode
  resp = Run({"eval", "return redis.call('scan', '0'); ", "1", "key"});
  EXPECT_EQ(resp.GetVec()[0], "0");
  EXPECT_EQ(resp.GetVec()[1].type, RespExpr::Type::ARRAY);
}
#endif

TEST_F(MultiTest, Watch) {
  auto kExecFail = ArgType(RespExpr::NIL);
  auto kExecSuccess = ArgType(RespExpr::ARRAY);

  // Check watch doesn't run in multi.
  Run({"multi"});
  ASSERT_THAT(Run({"watch", "a"}), ErrArg("'WATCH' inside MULTI is not allowed"));
  Run({"discard"});

  // Check watch on existing key.
  Run({"set", "a", "1"});
  EXPECT_EQ(Run({"watch", "a"}), "OK");
  Run({"set", "a", "2"});
  Run({"multi"});
  ASSERT_THAT(Run({"exec"}), kExecFail);

  // Check watch with nonempty exec body
  EXPECT_EQ(Run({"watch", "a"}), "OK");
  Run({"multi"});
  Run({"get", "a"});
  Run({"get", "b"});
  Run({"get", "c"});
  ASSERT_THAT(Run({"exec"}), kExecSuccess);

  // Check watch data cleared after EXEC.
  Run({"set", "a", "1"});
  Run({"multi"});
  ASSERT_THAT(Run({"exec"}), kExecSuccess);

  // Check watch on non-existent key.
  Run({"del", "b"});
  EXPECT_EQ(Run({"watch", "b"}), "OK");  // didn't exist yet
  Run({"set", "b", "1"});
  Run({"multi"});
  ASSERT_THAT(Run({"exec"}), kExecFail);

  // Check EXEC doesn't miss watched key expiration.
  Run({"watch", "a"});
  Run({"expire", "a", "1"});
  AdvanceTime(1000);
  Run({"multi"});
  Run({"get", "a"});
  ASSERT_THAT(Run({"exec"}), kExecFail);

  // Check unwatch.
  Run({"watch", "a"});
  Run({"unwatch"});
  Run({"set", "a", "3"});
  Run({"multi"});
  ASSERT_THAT(Run({"exec"}), kExecSuccess);

  // Check double expire
  Run({"watch", "a", "b"});
  Run({"set", "a", "2"});
  Run({"set", "b", "2"});
  Run({"multi"});
  ASSERT_THAT(Run({"exec"}), kExecFail);

  // Check EXPIRE + new key.
  Run({"set", "a", "1"});
  Run({"del", "c"});
  Run({"watch", "c"});  // didn't exist yet
  Run({"watch", "a"});
  Run({"set", "c", "1"});
  Run({"expire", "a", "1"});  // a existed

  AdvanceTime(1000);

  Run({"multi"});
  ASSERT_THAT(Run({"exec"}), kExecFail);

  // Check FLUSHDB touches watched keys
  Run({"select", "1"});
  Run({"set", "a", "1"});
  Run({"watch", "a"});
  Run({"flushdb"});
  Run({"multi"});
  ASSERT_THAT(Run({"exec"}), kExecFail);

  // Check multi db watches are not supported.
  Run({"select", "1"});
  Run({"set", "a", "1"});
  Run({"watch", "a"});
  Run({"select", "0"});
  Run({"multi"});
  ASSERT_THAT(Run({"exec"}), ArgType(RespExpr::ERROR));

  // Check watch keys are isolated between databases.
  Run({"set", "a", "1"});
  Run({"watch", "a"});
  Run({"select", "1"});
  Run({"set", "a", "2"});  // changing a on db 1
  Run({"select", "0"});
  Run({"multi"});
  ASSERT_THAT(Run({"exec"}), kExecSuccess);
}

TEST_F(MultiTest, MultiOOO) {
  GTEST_SKIP() << "Command squashing breaks stats";

  auto fb0 = pp_->at(0)->LaunchFiber([&] {
    for (unsigned i = 0; i < 100; i++) {
      Run({"multi"});
      Run({"rpush", "a", "bar"});
      Run({"exec"});
    }
  });

  pp_->at(1)->Await([&] {
    for (unsigned i = 0; i < 100; ++i) {
      Run({"multi"});
      Run({"rpush", "b", "bar"});
      Run({"exec"});
    }
  });

  fb0.Join();
  auto metrics = GetMetrics();

  // OOO works in LOCK_AHEAD mode.
  EXPECT_EQ(200, metrics.shard_stats.tx_ooo_total);
}

// Lua scripts lock their keys ahead and thus can run out of order.
TEST_F(MultiTest, EvalOOO) {
  if (auto config = absl::GetFlag(FLAGS_default_lua_flags); config != "") {
    GTEST_SKIP() << "Skipped EvalOOO test because default_lua_flags is set";
    return;
  }

  // Assign to prevent asyc optimization.
  const char* kScript = "local r = redis.call('MGET', unpack(KEYS)); return 'OK'";

  // Check single call.
  {
    auto resp = Run({"eval", kScript, "3", kKey1, kKey2, kKey3});
    ASSERT_EQ(resp, "OK");
  }

  const int kTimes = 10;
  // Check scripts running on different shards don't block each other.
  {
    auto run = [this, kScript](auto key) {
      for (int i = 0; i < kTimes; i++)
        Run({"eval", kScript, "1", key});
    };

    auto f1 = pp_->at(0)->LaunchFiber([&]() { run(kKeySid0); });
    auto f2 = pp_->at(1)->LaunchFiber([&]() { run(kKeySid1); });

    f1.Join();
    f2.Join();
  }

  auto metrics = GetMetrics();
  auto sum = metrics.coordinator_stats.eval_io_coordination_cnt +
             metrics.coordinator_stats.eval_shardlocal_coordination_cnt;
  EXPECT_EQ(1 + 2 * kTimes, sum);
}

// Run MULTI/EXEC commands in parallel, where each command is:
//        MULTI - SET k1 v - SET k2 v - SET k3 v - EXEC
// but the order of the commands inside appears in any permutation.
TEST_F(MultiTest, MultiContendedPermutatedKeys) {
  constexpr int kRounds = 5;

  auto run = [this](vector<string> keys, bool reversed) {
    int i = 0;
    do {
      Run({"multi"});
      auto apply = [this](auto key) { Run({"set", key, "v"}); };

      if (reversed)
        for_each(keys.rbegin(), keys.rend(), apply);
      else
        for_each(keys.begin(), keys.end(), apply);

      Run({"exec"});
    } while (next_permutation(keys.begin(), keys.end()) || i++ < kRounds);
  };

  vector<string> keys = {kKeySid0, kKeySid1, kKey3};

  auto f1 = pp_->at(1)->LaunchFiber([run, keys]() { run(keys, false); });
  auto f2 = pp_->at(2)->LaunchFiber([run, keys]() { run(keys, true); });

  f1.Join();
  f2.Join();
}

TEST_F(MultiTest, MultiCauseUnblocking) {
  const int kRounds = 10;
  vector<string> keys = {kKeySid0, kKeySid1, kKeySid2};

  auto push = [this, keys]() mutable {
    int i = 0;
    do {
      Run({"multi"});
      for (auto k : keys)
        Run({"lpush", k, "v"});
      Run({"exec"});
    } while (next_permutation(keys.begin(), keys.end()) || i++ < kRounds);
  };

  auto pop = [this, keys]() mutable {
    int i = 0;
    do {
      for (int j = keys.size() - 1; j >= 0; j--)
        ASSERT_THAT(Run({"blpop", keys[j], "0"}), ArrLen(2));
    } while (next_permutation(keys.begin(), keys.end()) || i++ < kRounds);
  };

  auto f1 = pp_->at(1)->LaunchFiber([push]() mutable { push(); });
  auto f2 = pp_->at(2)->LaunchFiber([pop]() mutable { pop(); });

  f1.Join();
  f2.Join();
}

TEST_F(MultiTest, ExecGlobalFallback) {
  Run({"multi"});
  Run({"set", "a", "1"});  // won't run ooo, because it became part of global
  Run({"move", "a", "1"});
  Run({"exec"});
  EXPECT_EQ(1, GetMetrics().coordinator_stats.tx_global_cnt);
}

#ifndef SANITIZERS
TEST_F(MultiTest, ScriptFlagsCommand) {
  if (auto flags = absl::GetFlag(FLAGS_default_lua_flags); flags != "") {
    GTEST_SKIP() << "Skipped ScriptFlagsCommand test because default_lua_flags is set";
    return;
  }

  const char* kUndeclared1 = "return redis.call('GET', 'random-key-1');";
  const char* kUndeclared2 = "return redis.call('GET', 'random-key-2');";

  Run({"set", "random-key-1", "works"});
  Run({"set", "random-key-2", "works"});

  // Check SCRIPT FLAGS is applied correctly to loaded scripts.
  {
    auto sha_resp = Run({"script", "load", kUndeclared1});
    auto sha = facade::ToSV(sha_resp.GetBuf());

    EXPECT_THAT(Run({"evalsha", sha, "0"}), ErrArg("undeclared"));

    EXPECT_EQ(Run({"script", "flags", sha, "allow-undeclared-keys"}), "OK");

    EXPECT_THAT(Run({"evalsha", sha, "0"}), "works");
  }

  // Check SCRIPT FLAGS can be applied by sha before loading.
  {
    char sha_buf[41];
    Interpreter::FuncSha1(kUndeclared2, sha_buf);
    string_view sha{sha_buf, 40};

    EXPECT_THAT(Run({"script", "flags", sha, "allow-undeclared-keys"}), "OK");

    EXPECT_THAT(Run({"eval", kUndeclared2, "0"}), "works");
  }
}
#endif

TEST_F(MultiTest, ScriptFlagsEmbedded) {
  const char* s1 = R"(
  --!df flags=allow-undeclared-keys
  return redis.call('GET', 'random-key');
)";

  // Check eval finds script flags.
  Run({"set", "random-key", "works"});
  EXPECT_EQ(Run({"eval", s1, "0"}), "works");

  const char* s2 = R"(
  --!df flags=this-is-an-error
  redis.call('SET', 'random-key', 'failed')
  )";

  EXPECT_THAT(Run({"eval", s2, "0"}), ErrArg("Invalid flag: this-is-an-error"));
}

// Flaky because of https://github.com/google/sanitizers/issues/1760
#ifndef SANITIZERS
TEST_F(MultiTest, UndeclaredKeyFlag) {
  absl::FlagSaver fs;  // lua_undeclared_keys_shas changed via CONFIG cmd below

  const char* script = "return redis.call('GET', 'random-key');";
  Run({"set", "random-key", "works"});

  // Get SHA for script in a persistent way
  string sha = Run({"script", "load", script}).GetString();

  // Make sure we can't run the script before setting the flag
  EXPECT_THAT(Run({"evalsha", sha, "0"}), ErrArg("undeclared"));
  EXPECT_THAT(Run({"eval", script, "0"}), ErrArg("undeclared"));

  // Clear all Lua scripts so we can configure the cache
  EXPECT_THAT(Run({"script", "flush"}), "OK");
  EXPECT_THAT(Run({"script", "exists", sha}), IntArg(0));

  EXPECT_THAT(
      Run({"config", "set", "lua_undeclared_keys_shas", absl::StrCat(sha, ",NON-EXISTING-HASH")}),
      "OK");

  // Check eval finds script flags.
  EXPECT_EQ(Run({"eval", script, "0"}), "works");
  EXPECT_EQ(Run({"evalsha", sha, "0"}), "works");
}

TEST_F(MultiTest, ScriptBadCommand) {
  const char* s1 = "redis.call('FLUSHALL')";
  const char* s2 = "redis.call('FLUSHALL'); redis.set(KEYS[1], ARGS[1]);";
  const char* s3 = "redis.acall('FLUSHALL'); redis.set(KEYS[1], ARGS[1]);";
  const char* s4 = R"(
    --!df flags=disable-atomicity
    redis.call('FLUSHALL');
    return "OK";
  )";

  auto resp = Run({"eval", s1, "0"});  // tx won't be scheduled at all
  EXPECT_THAT(resp, ErrArg("This Redis command is not allowed from script"));

  resp = Run({"eval", s2, "1", "works", "false"});  // will be scheduled as lock ahead
  EXPECT_THAT(resp, ErrArg("This Redis command is not allowed from script"));

  resp = Run({"eval", s3, "1", "works", "false"});  // also async call will happen
  EXPECT_THAT(resp, ErrArg("This Redis command is not allowed from script"));

  resp = Run({"eval", s4, "0"});
  EXPECT_EQ(resp, "OK");
}
#endif

TEST_F(MultiTest, MultiEvalModeConflict) {
  const char* s1 = R"(
  --!df flags=allow-undeclared-keys
  return redis.call('GET', 'random-key');
)";

  EXPECT_EQ(Run({"multi"}), "OK");
  // Check eval finds script flags.
  EXPECT_EQ(Run({"set", "random-key", "works"}), "QUEUED");
  EXPECT_EQ(Run({"eval", s1, "0"}), "QUEUED");
  EXPECT_THAT(Run({"exec"}),
              RespArray(ElementsAre(
                  "OK", ErrArg("Multi mode conflict when running eval in multi transaction"))));
}

// Run multi-exec transactions that move values from a source list
// to destination list through two contended channels.
TEST_F(MultiTest, ContendedList) {
  constexpr int listSize = 50;
  constexpr int stepSize = 5;

  auto run = [this](string_view src, string_view dest) {
    for (int i = 0; i < listSize / stepSize; i++) {
      Run({"multi"});
      Run({"sort", src});
      for (int j = 0; j < stepSize; j++)
        Run({"lmove", src, j % 2 ? "chan-1" : "chan-2", "RIGHT", "RIGHT"});
      for (int j = 0; j < stepSize; j++)
        Run({"lmove", j % 2 ? "chan-1" : "chan-2", dest, "LEFT", "RIGHT"});
      Run({"exec"});
    }
  };

  for (int i = 0; i < listSize; i++) {
    Run({"lpush", "l1", "a"});
    Run({"lpush", "l2", "b"});
  }

  auto f1 = pp_->at(1)->LaunchFiber([run]() mutable { run("l1", "l1-out"); });
  auto f2 = pp_->at(2)->LaunchFiber([run]() mutable { run("l2", "l2-out"); });

  f1.Join();
  f2.Join();

  for (int i = 0; i < listSize; i++) {
    EXPECT_EQ(Run({"lpop", "l1-out"}), "a");
    EXPECT_EQ(Run({"lpop", "l2-out"}), "b");
  }

  EXPECT_THAT(Run({"llen", "chan-1"}), IntArg(0));
  EXPECT_THAT(Run({"llen", "chan-2"}), IntArg(0));
}

// Test that squashing makes single-key ops atomic withing a non-atomic tx
// because it runs them within one hop.
TEST_F(MultiTest, TestSquashing) {
  absl::FlagSaver fs;
  absl::SetFlag(&FLAGS_multi_exec_squash, true);

  const char* keys[] = {kKeySid0, kKeySid1, kKeySid2};

  atomic_bool done{false};
  auto f1 = pp_->at(1)->LaunchFiber([this, keys, &done]() {
    while (!done.load()) {
      for (auto key : keys)
        ASSERT_THAT(Run({"llen", key}), IntArg(0));
    }
  });

  for (unsigned times = 0; times < 10; times++) {
    Run({"multi"});
    for (auto key : keys)
      Run({"lpush", key, "works"});
    for (auto key : keys)
      Run({"lpop", key});
    Run({"exec"});
  }

  done.store(true);
  f1.Join();

  // Test some more unusual commands
  Run({"multi"});
  Run({"mget", "x1", "x2", "x3"});
  Run({"mget", "x4"});
  Run({"mget", "x5", "x6", "x7", "x8"});
  Run({"ft.search", "i1", "*"});
  Run({"exec"});
}

TEST_F(MultiTest, MultiLeavesTxQueue) {
  // Tests the scenario, where the OOO multi-tx is scheduled into tx queue and there is another
  // tx (mget) after it that runs and tests for atomicity.
  absl::FlagSaver fs;
  absl::SetFlag(&FLAGS_multi_exec_squash, false);

  for (unsigned i = 0; i < 20; ++i) {
    string key = StrCat("x", i);
    LOG(INFO) << key << ": shard " << Shard(key, shard_set->size());
  }

  Run({"mget", "x5", "x8", "x9", "x13", "x16", "x17"});
  ASSERT_EQ(1, GetDebugInfo().shards_count);

  auto fb1 = pp_->at(1)->LaunchFiber(Launch::post, [&] {
    // Runs multi on shard0 1000 times.
    for (unsigned j = 0; j < 1000; ++j) {
      Run({"multi"});
      Run({"incrby", "x13", "1"});
      Run({"incrby", "x16", "1"});
      Run({"incrby", "x17", "1"});
      Run({"exec"});
    }
  });

  auto fb2 = pp_->at(2)->LaunchFiber(Launch::dispatch, [&] {
    // Runs multi on shard0 1000 times.
    for (unsigned j = 0; j < 1000; ++j) {
      Run({"multi"});
      Run({"incrby", "x5", "1"});
      Run({"incrby", "x8", "1"});
      Run({"incrby", "x9", "1"});
      Run({"exec"});
    }
  });

  auto check_triple = [](const RespExpr::Vec& arr, unsigned start) {
    if (arr[start].type != arr[start + 1].type || arr[start + 1].type != arr[start + 2].type) {
      return false;
    }

    if (arr[0].type == RespExpr::STRING) {
      string s0 = arr[start].GetString();
      string s1 = arr[start + 1].GetString();
      string s2 = arr[start + 2].GetString();
      if (s0 != s1 || s1 != s2) {
        return false;
      }
    }
    return true;
  };

  bool success = pp_->at(0)->Await([&]() -> bool {
    for (unsigned j = 0; j < 1000; ++j) {
      auto resp = Run({"mget", "x5", "x8", "x9", "x13", "x16", "x17"});
      const RespExpr::Vec& arr = resp.GetVec();
      CHECK_EQ(6u, arr.size());

      if (!check_triple(arr, 0)) {
        LOG(ERROR) << "inconsistent " << arr[0] << " " << arr[1] << " " << arr[2];
        return false;
      }
      if (!check_triple(arr, 3)) {
        LOG(ERROR) << "inconsistent " << arr[3] << " " << arr[4] << " " << arr[5];
        return false;
      }
    }
    return true;
  });

  fb1.Join();
  fb2.Join();
  ASSERT_TRUE(success);
}

TEST_F(MultiTest, TestLockedKeys) {
  auto condition = [&]() { return IsLocked(0, "key1") && IsLocked(0, "key2"); };
  auto fb = ExpectConditionWithSuspension(condition);

  EXPECT_EQ(Run({"multi"}), "OK");
  EXPECT_EQ(Run({"set", "key1", "val1"}), "QUEUED");
  EXPECT_EQ(Run({"set", "key2", "val2"}), "QUEUED");
  EXPECT_EQ(Run({"mset", "key1", "val3", "key1", "val4"}), "QUEUED");
  EXPECT_THAT(Run({"exec"}), RespArray(ElementsAre("OK", "OK", "OK")));
  fb.Join();
  EXPECT_FALSE(IsLocked(0, "key1"));
  EXPECT_FALSE(IsLocked(0, "key2"));
}

TEST_F(MultiTest, EvalExpiration) {
  // Make sure expiration is correctly set even from Lua scripts
  if (auto config = absl::GetFlag(FLAGS_default_lua_flags); config != "") {
    GTEST_SKIP() << "Skipped Eval test because default_lua_flags is set";
    return;
  }

  Run({"eval", "redis.call('set', 'x', 0, 'ex', 5, 'nx')", "1", "x"});
  EXPECT_LE(CheckedInt({"pttl", "x"}), 5000);
}

TEST_F(MultiTest, MemoryInScript) {
  EXPECT_EQ(Run({"set", "x", "y"}), "OK");

  auto resp = Run({"eval", "return redis.call('MEMORY', 'USAGE', KEYS[1])", "1", "x"});
  EXPECT_THAT(resp, IntArg(0));
}

TEST_F(MultiTest, NoKeyTransactional) {
  Run({"multi"});
  Run({"ft._list"});
  Run({"exec"});
}

class MultiEvalTest : public BaseFamilyTest {
 protected:
  MultiEvalTest() : BaseFamilyTest() {
    num_threads_ = kPoolThreadCount;
    absl::SetFlag(&FLAGS_default_lua_flags, "allow-undeclared-keys");
  }

  absl::FlagSaver fs_;
};

TEST_F(MultiEvalTest, MultiAllEval) {
  RespExpr brpop_resp;

  // Run the fiber at creation.
  auto fb0 = pp_->at(1)->LaunchFiber(Launch::dispatch, [&] {
    brpop_resp = Run({"brpop", "x", "1"});
  });
  Run({"multi"});
  Run({"eval", "return redis.call('lpush', 'x', 'y')", "0"});
  Run({"eval", "return redis.call('lpop', 'x')", "0"});
  RespExpr exec_resp = Run({"exec"});
  fb0.Join();

  EXPECT_THAT(exec_resp.GetVec(), ElementsAre(IntArg(1), "y"));

  EXPECT_THAT(brpop_resp, ArgType(RespExpr::NIL_ARRAY));
}

TEST_F(MultiEvalTest, MultiSomeEval) {
  RespExpr brpop_resp;

  // Run the fiber at creation.
  auto fb0 = pp_->at(1)->LaunchFiber(Launch::dispatch, [&] {
    brpop_resp = Run({"brpop", "x", "1"});
  });
  Run({"multi"});
  Run({"eval", "return redis.call('lpush', 'x', 'y')", "0"});
  Run({"lpop", "x"});
  RespExpr exec_resp = Run({"exec"});
  fb0.Join();

  EXPECT_THAT(exec_resp.GetVec(), ElementsAre(IntArg(1), "y"));

  EXPECT_THAT(brpop_resp, ArgType(RespExpr::NIL_ARRAY));
}

// Flaky because of https://github.com/google/sanitizers/issues/1760
#ifndef SANITIZERS
TEST_F(MultiEvalTest, ScriptSquashingUknownCmd) {
  absl::FlagSaver fs;
  absl::SetFlag(&FLAGS_lua_auto_async, true);

  // The script below contains two commands for which execution can't even be prepared
  // (FIRST/SECOND WRONG). The first is issued with pcall, so its error should be completely
  // ignored, the second one should cause an abort and no further commands should be executed
  string_view s = R"(
    redis.pcall('INCR', 'A')
    redis.pcall('FIRST WRONG')
    redis.pcall('INCR', 'A')
    redis.call('SECOND WRONG')
    redis.pcall('INCR', 'A')
  )";

  EXPECT_THAT(Run({"EVAL", s, "1", "A"}), ErrArg("unknown command `SECOND WRONG`"));
  EXPECT_EQ(Run({"get", "A"}), "2");
}
#endif

TEST_F(MultiEvalTest, MultiAndEval) {
  // We had a bug in borrowing interpreters which caused a crash in this scenario
  Run({"multi"});
  Run({"eval", "return redis.call('set', 'x', 'y1')", "1", "x"});
  Run({"exec"});

  Run({"eval", "return redis.call('set', 'x', 'y1')", "1", "x"});

  Run({"multi"});
  Run({"eval", "return 'OK';", "0"});
  auto resp = Run({"exec"});
  EXPECT_EQ(resp, "OK");

  // We had a bug running script load inside multi
  Run({"multi"});
  Run({"script", "load", "return '5'"});
  Run({"exec"});

  Run({"multi"});
  Run({"script", "load", "return '5'"});
  Run({"get", "x"});
  Run({"exec"});

  Run({"multi"});
  Run({"script", "load", "return '5'"});
  Run({"mset", "x1", "y1", "x2", "y2"});
  Run({"exec"});

  Run({"multi"});
  Run({"script", "load", "return '5'"});
  Run({"eval", "return redis.call('set', 'x', 'y')", "1", "x"});
  Run({"get", "x"});
  Run({"exec"});

  Run({"get", "x"});
}

TEST_F(MultiTest, MultiTypes) {
  // we had a bug with namespaces for type command in multi/exec
  EXPECT_THAT(Run({"multi"}), "OK");
  EXPECT_THAT(Run({"type", "sdfx3"}), "QUEUED");
  EXPECT_THAT(Run({"type", "asdasd2"}), "QUEUED");
  EXPECT_THAT(Run({"type", "wer124"}), "QUEUED");
  EXPECT_THAT(Run({"type", "asafdasd"}), "QUEUED");
  EXPECT_THAT(Run({"type", "dsfgser"}), "QUEUED");
  EXPECT_THAT(Run({"type", "erg2"}), "QUEUED");
  EXPECT_THAT(Run({"exec"}),
              RespArray(ElementsAre("none", "none", "none", "none", "none", "none")));
}

TEST_F(MultiTest, EvalRo) {
  RespExpr resp;

  resp = Run({"set", "foo", "bar"});
  EXPECT_THAT(resp, "OK");

  resp = Run({"eval_ro", "return redis.call('get', KEYS[1])", "1", "foo"});
  EXPECT_THAT(resp, "bar");

  resp = Run({"eval_ro", "return redis.call('set', KEYS[1], 'car')", "1", "foo"});
  EXPECT_THAT(resp, ErrArg("Write commands are not allowed from read-only scripts"));
}

TEST_F(MultiTest, EvalShaRo) {
  RespExpr resp;

  const char* read_script = "return redis.call('get', KEYS[1]);";
  const char* write_script = "return redis.call('set', KEYS[1], 'car');";

  auto sha_resp = Run({"script", "load", read_script});
  auto read_sha = facade::ToSV(sha_resp.GetBuf());
  sha_resp = Run({"script", "load", write_script});
  auto write_sha = facade::ToSV(sha_resp.GetBuf());

  resp = Run({"set", "foo", "bar"});
  EXPECT_THAT(resp, "OK");

  resp = Run({"evalsha_ro", read_sha, "1", "foo"});
  EXPECT_THAT(resp, "bar");

  resp = Run({"evalsha_ro", write_sha, "1", "foo"});
  EXPECT_THAT(resp, ErrArg("Write commands are not allowed from read-only scripts"));
}

}  // namespace dfly
