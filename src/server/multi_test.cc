// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

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

ABSL_DECLARE_FLAG(int, multi_exec_mode);
ABSL_DECLARE_FLAG(std::string, default_lua_config);

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
const char kKey2Sid0[] = "y";

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

TEST_F(MultiTest, MultiAndEval) {
  ShardId sid1 = Shard(kKey1, num_threads_ - 1);
  ShardId sid2 = Shard(kKey2, num_threads_ - 1);
  ShardId sid3 = Shard(kKey3, num_threads_ - 1);
  ShardId sid4 = Shard(kKey4, num_threads_ - 1);
  EXPECT_EQ(0, sid1);
  EXPECT_EQ(2, sid2);
  EXPECT_EQ(1, sid3);
  EXPECT_EQ(0, sid4);

  RespExpr resp = Run({"multi"});
  ASSERT_EQ(resp, "OK");

  resp = Run({"get", kKey1});
  ASSERT_EQ(resp, "QUEUED");

  resp = Run({"get", kKey4});
  ASSERT_EQ(resp, "QUEUED");
  EXPECT_THAT(Run({"eval", "return redis.call('exists', KEYS[2])", "2", "a", "b"}),
              ErrArg("'EVAL' Dragonfly does not allow execution of a server-side Lua script inside "
                     "transaction block"));
}

TEST_F(MultiTest, MultiAndFlush) {
  RespExpr resp = Run({"multi"});
  ASSERT_EQ(resp, "OK");

  resp = Run({"get", kKey1});
  ASSERT_EQ(resp, "QUEUED");

  EXPECT_THAT(Run({"FLUSHALL"}), ErrArg("'FLUSHALL' inside MULTI is not allowed"));
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

  ASSERT_FALSE(service_->IsLocked(0, kKey1));
  ASSERT_FALSE(service_->IsLocked(0, kKey4));
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

  ASSERT_FALSE(service_->IsLocked(0, "key"));
  ASSERT_FALSE(service_->IsLocked(2, "key"));
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

  ASSERT_THAT(resp, ArrLen(0));
  ASSERT_FALSE(service_->IsShardSetLocked());

  Run({"multi"});
  ASSERT_EQ(Run({"ping", "foo"}), "QUEUED");
  resp = Run({"exec"});
  // one cell arrays are promoted to respexpr.
  EXPECT_EQ(resp, "foo");
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

  ASSERT_FALSE(service_->IsLocked(0, kKey1));
  ASSERT_FALSE(service_->IsLocked(0, kKey4));
  ASSERT_FALSE(service_->IsShardSetLocked());

  ASSERT_THAT(resp, ArrLen(3));
  const auto& arr = resp.GetVec();
  EXPECT_THAT(arr, ElementsAre("OK", "1", ArrLen(2)));

  ASSERT_THAT(arr[2].GetVec(), ElementsAre("1", ArgType(RespExpr::NIL)));
}

TEST_F(MultiTest, MultiConsistent) {
  int multi_mode = absl::GetFlag(FLAGS_multi_exec_mode);
  if (multi_mode == Transaction::NON_ATOMIC)
    return;

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
    fibers_ext::SleepFor(1ms);

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

  ASSERT_FALSE(service_->IsLocked(0, kKey1));
  ASSERT_FALSE(service_->IsLocked(0, kKey4));
  ASSERT_FALSE(service_->IsShardSetLocked());
}

TEST_F(MultiTest, MultiWeirdCommands) {
  // FIXME: issue https://github.com/dragonflydb/dragonfly/issues/457
  // once we would have fix for supporting EVAL from within transaction
  Run({"multi"});
  EXPECT_THAT(Run({"eval", "return 42", "0"}),
              ErrArg("'EVAL' Dragonfly does not allow execution of a server-side Lua script inside "
                     "transaction block"));
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

  EXPECT_FALSE(service_->IsLocked(0, kKey1));
  EXPECT_FALSE(service_->IsLocked(0, kKey2));
  EXPECT_FALSE(service_->IsLocked(0, kKey4));
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

  ASSERT_FALSE(service_->IsLocked(0, kKey1));
  ASSERT_FALSE(service_->IsLocked(0, kKey4));
  ASSERT_FALSE(service_->IsShardSetLocked());
}

TEST_F(MultiTest, Eval) {
  if (auto config = absl::GetFlag(FLAGS_default_lua_config); config != "") {
    LOG(WARNING) << "Skipped Eval test because default_lua_config is set";
    return;
  }

  RespExpr resp;

  resp = Run({"incrby", "foo", "42"});
  EXPECT_THAT(resp, IntArg(42));

  resp = Run({"eval", "return redis.call('get', 'foo')", "0"});
  EXPECT_THAT(resp, ErrArg("undeclared"));

  resp = Run({"eval", "return redis.call('get', 'foo')", "1", "bar"});
  EXPECT_THAT(resp, ErrArg("undeclared"));

  ASSERT_FALSE(service_->IsLocked(0, "foo"));

  resp = Run({"eval", "return redis.call('get', 'foo')", "1", "foo"});
  EXPECT_THAT(resp, "42");
  ASSERT_FALSE(service_->IsLocked(0, "foo"));

  resp = Run({"eval", "return redis.call('get', KEYS[1])", "1", "foo"});
  EXPECT_THAT(resp, "42");

  ASSERT_FALSE(service_->IsLocked(0, "foo"));
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
}

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
  int mode = absl::GetFlag(FLAGS_multi_exec_mode);
  if (mode == Transaction::LOCK_AHEAD || mode == Transaction::NON_ATOMIC)
    EXPECT_EQ(200, metrics.ooo_tx_transaction_cnt);
  else
    EXPECT_EQ(0, metrics.ooo_tx_transaction_cnt);
}

// Lua scripts lock their keys ahead and thus can run out of order.
TEST_F(MultiTest, EvalOOO) {
  if (auto config = absl::GetFlag(FLAGS_default_lua_config); config != "") {
    LOG(WARNING) << "Skipped Eval test because default_lua_config is set";
    return;
  }

  const char* kScript = "redis.call('MGET', unpack(KEYS)); return 'OK'";

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
  EXPECT_EQ(1 + 2 * kTimes, metrics.ooo_tx_transaction_cnt);
}

// Run MULTI/EXEC commands in parallel, where each command is:
//        MULTI - SET k1 v - SET k2 v - SET k3 v - EXEC
// but the order of the commands inside appears in any permutation.
TEST_F(MultiTest, MultiContendedPermutatedKeys) {
  const int kRounds = 5;

  auto run = [this, kRounds](vector<string> keys, bool reversed) {
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

  auto push = [this, keys, kRounds]() mutable {
    int i = 0;
    do {
      Run({"multi"});
      for (auto k : keys)
        Run({"lpush", k, "v"});
      Run({"exec"});
    } while (next_permutation(keys.begin(), keys.end()) || i++ < kRounds);
  };

  auto pop = [this, keys, kRounds]() mutable {
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
  // Check global command MOVE falls back to global mode from lock ahead.
  absl::SetFlag(&FLAGS_multi_exec_mode, Transaction::LOCK_AHEAD);
  Run({"multi"});
  Run({"set", "a", "1"});  // won't run ooo, because it became part of global
  Run({"move", "a", "1"});
  Run({"exec"});
  EXPECT_EQ(0, GetMetrics().ooo_tx_transaction_cnt);

  // Check non atomic mode does not fall back to global.
  absl::SetFlag(&FLAGS_multi_exec_mode, Transaction::NON_ATOMIC);
  Run({"multi"});
  Run({"set", "a", "1"});  // will run ooo
  Run({"move", "a", "1"});
  Run({"exec"});
  EXPECT_EQ(1, GetMetrics().ooo_tx_transaction_cnt);
}

TEST_F(MultiTest, ScriptConfig) {
  if (auto config = absl::GetFlag(FLAGS_default_lua_config); config != "") {
    LOG(WARNING) << "Skipped Eval test because default_lua_config is set";
    return;
  }

  const char* kUndeclared1 = "return redis.call('GET', 'random-key-1');";
  const char* kUndeclared2 = "return redis.call('GET', 'random-key-2');";

  Run({"set", "random-key-1", "works"});
  Run({"set", "random-key-2", "works"});

  // Check SCRIPT CONFIG is applied correctly to loaded scripts.
  {
    auto sha_resp = Run({"script", "load", kUndeclared1});
    auto sha = facade::ToSV(sha_resp.GetBuf());

    EXPECT_THAT(Run({"evalsha", sha, "0"}), ErrArg("undeclared"));

    EXPECT_THAT(Run({"script", "config", sha, "allow-undeclared-keys"}), "OK");
    EXPECT_THAT(Run({"evalsha", sha, "0"}), "works");
  }

  // Check SCRIPT CONFIG can be applied by sha before loading.
  {
    char sha_buf[41];
    Interpreter::FuncSha1(kUndeclared2, sha_buf);
    string_view sha{sha_buf, 40};

    EXPECT_THAT(Run({"script", "config", sha, "allow-undeclared-keys"}), "OK");

    EXPECT_THAT(Run({"eval", kUndeclared2, "0"}), "works");
  }
}

TEST_F(MultiTest, ScriptPragmas) {
  const char* s1 = R"(
  -- pragma: allow-undeclared-keys
  return redis.call('GET', 'random-key');
)";

  // Check eval finds script pragmas.
  Run({"set", "random-key", "works"});
  EXPECT_EQ(Run({"eval", s1, "0"}), "works");

  const char* s2 = R"(
  -- pragma: this-is-an-error
  redis.call('SET', 'random-key', 'failed')
  )";

  EXPECT_THAT(Run({"eval", s2, "0"}), ErrArg("Invalid argument: Invalid pragma: this-is-an-error"));
}

// Run multi-exec transactions that move values from a source list
// to destination list through two contended channels.
TEST_F(MultiTest, ContendedList) {
  if (absl::GetFlag(FLAGS_multi_exec_mode) == Transaction::NON_ATOMIC)
    return;

  const int listSize = 50;
  const int stepSize = 5;

  auto run = [this, listSize, stepSize](string_view src, string_view dest) {
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
    EXPECT_EQ(Run({"lpop", "l1"}), "a");
    EXPECT_EQ(Run({"lpop", "l2"}), "b");
  }
  EXPECT_EQ(Run({"llen", "chan-1"}), "0");
  EXPECT_EQ(Run({"llen", "chan-2"}), "0");
}

}  // namespace dfly
