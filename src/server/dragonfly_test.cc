// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

extern "C" {
#include "redis/sds.h"
#include "redis/zmalloc.h"
}

#include <absl/strings/ascii.h>
#include <absl/strings/charconv.h>
#include <absl/strings/str_join.h>
#include <absl/strings/strip.h>
#include <fast_float/fast_float.h>
#include <gmock/gmock.h>

#include "base/flags.h"
#include "base/gtest.h"
#include "base/logging.h"
#include "facade/facade_test.h"
#include "server/conn_context.h"
#include "server/main_service.h"
#include "server/test_utils.h"

ABSL_DECLARE_FLAG(float, mem_defrag_threshold);
ABSL_DECLARE_FLAG(float, mem_defrag_waste_threshold);
ABSL_DECLARE_FLAG(uint32_t, mem_defrag_check_sec_interval);
ABSL_DECLARE_FLAG(std::vector<std::string>, rename_command);
ABSL_DECLARE_FLAG(bool, lua_resp2_legacy_float);
ABSL_DECLARE_FLAG(double, eviction_memory_budget_threshold);

namespace dfly {

using namespace std;
using namespace util;
using absl::SetFlag;
using absl::StrCat;
using fb2::Fiber;
using ::io::Result;
using testing::AnyOf;
using testing::ElementsAre;
using testing::HasSubstr;

namespace {

constexpr unsigned kPoolThreadCount = 4;

const char kKey1[] = "x";
const char kKey2[] = "b";

const char kKeySid0[] = "x";
const char kKeySid1[] = "c";
const char kKeySid2[] = "b";

}  // namespace

// This test is responsible for server and main service
// (connection, transaction etc) families.
class DflyEngineTest : public BaseFamilyTest {
 protected:
  DflyEngineTest() : BaseFamilyTest() {
    num_threads_ = kPoolThreadCount;
  }
};

class DflyEngineTestWithRegistry : public BaseFamilyTest {
 protected:
  DflyEngineTestWithRegistry() : BaseFamilyTest() {
    num_threads_ = kPoolThreadCount;
    ResetService();
  }
};

class SingleThreadDflyEngineTest : public BaseFamilyTest {
 protected:
  SingleThreadDflyEngineTest() : BaseFamilyTest() {
    num_threads_ = 1;
  }
};

class DefragDflyEngineTest : public SingleThreadDflyEngineTest {};

// TODO: to implement equivalent parsing in redis parser.
TEST_F(DflyEngineTest, Sds) {
  int argc;
  sds* argv = sdssplitargs("\r\n", &argc);
  EXPECT_EQ(0, argc);
  sdsfreesplitres(argv, argc);

  argv = sdssplitargs("\026 \020 \200 \277 \r\n", &argc);
  EXPECT_EQ(4, argc);
  EXPECT_STREQ("\026", argv[0]);
  sdsfreesplitres(argv, argc);

  argv = sdssplitargs(R"(abc "oops\n" )"
                      "\r\n",
                      &argc);
  EXPECT_EQ(2, argc);
  EXPECT_STREQ("oops\n", argv[1]);
  sdsfreesplitres(argv, argc);

  argv = sdssplitargs(R"( "abc\xf0" )"
                      "\t'oops\n'  \r\n",
                      &argc);
  ASSERT_EQ(2, argc);
  EXPECT_STREQ("abc\xf0", argv[0]);
  EXPECT_STREQ("oops\n", argv[1]);
  sdsfreesplitres(argv, argc);
}

class DflyRenameCommandTest : public DflyEngineTest {
 protected:
  DflyRenameCommandTest() : DflyEngineTest() {
    // rename flushall to myflushall, flushdb command will not be able to execute
    absl::SetFlag(
        &FLAGS_rename_command,
        std::vector<std::string>({"flushall=myflushall", "flushdb=", "ping=abcdefghijklmnop"}));
  }

  void TearDown() {
    absl::SetFlag(&FLAGS_rename_command, std::vector<std::string>({""}));
    DflyEngineTest::TearDown();
  }
};

TEST_F(DflyRenameCommandTest, RenameCommand) {
  Run({"set", "a", "1"});
  ASSERT_EQ(1, CheckedInt({"dbsize"}));
  // flushall should not execute anything and should return error, as it was renamed.
  ASSERT_THAT(Run({"flushall"}), ErrArg("unknown command `FLUSHALL`"));

  ASSERT_EQ(1, CheckedInt({"dbsize"}));

  ASSERT_EQ(Run({"myflushall"}), "OK");

  ASSERT_EQ(0, CheckedInt({"dbsize"}));

  ASSERT_THAT(Run({"flushdb", "0"}), ErrArg("unknown command `FLUSHDB`"));

  ASSERT_THAT(Run({""}), ErrArg("unknown command ``"));

  ASSERT_THAT(Run({"ping"}), ErrArg("unknown command `PING`"));
  ASSERT_THAT(Run({"abcdefghijklmnop"}), "PONG");
}

TEST_F(SingleThreadDflyEngineTest, GlobalSingleThread) {
  Run({"set", "a", "1"});
  Run({"move", "a", "1"});
}

TEST_F(DflyEngineTest, EvalResp) {
  auto resp = Run({"eval", "return 43", "0"});
  EXPECT_THAT(resp, IntArg(43));

  resp = Run({"eval", "return {5, 'foo', 17.5}", "0"});
  ASSERT_THAT(resp, ArrLen(3));
  EXPECT_THAT(resp.GetVec(), ElementsAre(IntArg(5), "foo", "17.5"));

  resp = Run({"eval", "return {map={a=1,b=2}}", "0"});
  ASSERT_THAT(resp, ArrLen(4));
  EXPECT_THAT(resp.GetVec(), AnyOf(ElementsAre("a", IntArg(1), "b", IntArg(2)),
                                   ElementsAre("b", IntArg(2), "a", IntArg(1))));
}

TEST_F(DflyEngineTest, EvalPublish) {
  auto resp = pp_->at(1)->Await([&] { return Run({"subscribe", "foo"}); });
  EXPECT_THAT(resp, ArrLen(3));

  resp = Run({"eval", "return redis.call('publish', 'foo', 'bar')", "0"});
  EXPECT_THAT(resp, IntArg(1));
}

TEST_F(DflyEngineTest, EvalBug59) {
  auto resp = Run({"eval", R"(
local epoch
if redis.call('exists', KEYS[2]) ~= 0 then
  epoch = redis.call("hget", KEYS[2], "e")
end
if epoch == false or epoch == nil then
  epoch = ARGV[6]
  redis.call("hset", KEYS[2], "e", epoch)
end
local offset = redis.call("hincrby", KEYS[2], "s", 1)
if ARGV[5] ~= '0' then
	redis.call("expire", KEYS[2], ARGV[5])
end
redis.call("xadd", KEYS[1], "MAXLEN", ARGV[2], offset, "d", ARGV[1])
redis.call("expire", KEYS[1], ARGV[3])
if ARGV[4] ~= '' then
	local payload = "__" .. "p1:" .. offset .. ":" .. epoch .. "__" .. ARGV[1]
	redis.call("publish", ARGV[4], payload)
end

return {offset, epoch}
    )",
                   "2", "x", "y", "1", "2", "3", "4", "5", "6"});
  ASSERT_THAT(resp, ArrLen(2));
  EXPECT_THAT(resp.GetVec(), ElementsAre(IntArg(1), "6"));
}

// Scenario: 1. a lua call A schedules itself on shards 0, 1, 2.
//           2. another lua call B schedules itself on shards 1,2 but on shard 1 (or 2) it
//              schedules itself before A.
//              the order of scheduling: shard 0: A, shard 1: B, A. shard 2: B, A.
//           3. A is executes its first command first, which coincendently runs only on shard 0,
//              hence A finishes before B and then it tries to cleanup.
//           4. There was an incorrect cleanup of multi-transactions that breaks for shard 1 (or 2)
//              because it assume the A is at front of the queue.
TEST_F(DflyEngineTest, EvalBug713) {
  const char* script = "return redis.call('get', KEYS[1])";

  // A
  auto fb0 = pp_->at(1)->LaunchFiber([&] {
    ThisFiber::Yield();
    for (unsigned i = 0; i < 50; ++i) {
      Run({"eval", script, "3", kKeySid0, kKeySid1, kKeySid2});
    }
  });

  // B
  for (unsigned j = 0; j < 50; ++j) {
    Run({"eval", script, "2", kKeySid1, kKeySid2});
  }
  fb0.Join();
}

// Tests deadlock that happenned due to a fact that trans->Schedule was called
// before interpreter->Lock().
//
// The problematic scenario:
// 1. transaction 1 schedules itself and blocks on an interpreter lock
// 2. transaction 2 schedules itself, but meanwhile an interpreter unlocks itself and
//    transaction 2 grabs the lock but can not progress due to transaction 1 already
//    scheduled before.
TEST_F(DflyEngineTest, EvalBug713b) {
  const char* script = "return redis.call('get', KEYS[1])";

  const uint32_t kNumFibers = 20;
  Fiber fibers[kNumFibers];

  for (unsigned j = 0; j < kNumFibers; ++j) {
    fibers[j] = pp_->at(1)->LaunchFiber([j, script, this] {
      for (unsigned i = 0; i < 50; ++i) {
        Run(StrCat("fb", j), {"eval", script, "3", kKeySid0, kKeySid1, kKeySid2});
      }
    });
  }

  for (unsigned j = 0; j < kNumFibers; ++j) {
    fibers[j].Join();
  }
}

TEST_F(DflyEngineTest, EvalSha) {
  auto resp = Run({"script", "load", "return 5"});
  EXPECT_THAT(resp, ArgType(RespExpr::STRING));

  string sha{ToSV(resp.GetBuf())};

  resp = Run({"evalsha", sha, "0"});
  EXPECT_THAT(resp, IntArg(5));

  absl::AsciiStrToUpper(&sha);
  resp = Run({"evalsha", sha, "0"});
  EXPECT_THAT(resp, IntArg(5));

  resp = Run({"evalsha", "foobar", "0"});
  EXPECT_THAT(resp, ErrArg("No matching"));

  resp = Run({"script", "load", "\n return 5"});

  // Important to keep spaces in order to be compatible with Redis.
  // See https://github.com/dragonflydb/dragonfly/issues/146
  EXPECT_THAT(resp, "c6459b95a0e81df97af6fdd49b1a9e0287a57363");
}

TEST_F(DflyEngineTest, ScriptFlush) {
  auto resp = Run({"script", "load", "return 5"});
  EXPECT_THAT(resp, ArgType(RespExpr::STRING));
  string sha{ToSV(resp.GetBuf())};
  resp = Run({"evalsha", sha, "0"});
  EXPECT_THAT(5, resp.GetInt());
  resp = Run({"script", "exists", sha});
  EXPECT_THAT(1, resp.GetInt());

  resp = Run({"script", "flush"});
  resp = Run({"script", "exists", sha});
  EXPECT_THAT(0, resp.GetInt());
  EXPECT_THAT(Run({"evalsha", sha, "0"}), ErrArg("NOSCRIPT No matching script. Please use EVAL."));

  resp = Run({"script", "load", "return 5"});
  EXPECT_THAT(resp, ArgType(RespExpr::STRING));
  sha = string{ToSV(resp.GetBuf())};
  resp = Run({"evalsha", sha, "0"});
  EXPECT_THAT(5, resp.GetInt());
  resp = Run({"script", "exists", sha});
  EXPECT_THAT(1, resp.GetInt());
}

TEST_F(DflyEngineTestWithRegistry, Hello) {
  auto resp = Run({"hello"});
  ASSERT_THAT(resp, ArrLen(14));
  resp = Run({"hello", "2"});
  ASSERT_THAT(resp, ArrLen(14));

  EXPECT_THAT(
      resp.GetVec(),
      ElementsAre("server", "redis", "version", "7.4.0", "dragonfly_version",
                  ArgType(RespExpr::STRING), "proto", IntArg(2), "id", ArgType(RespExpr::INT64),
                  "mode", testing::AnyOf("standalone", "cluster"), "role", "master"));

  resp = Run({"hello", "3"});
  ASSERT_THAT(resp, ArrLen(14));
  EXPECT_THAT(
      resp.GetVec(),
      ElementsAre("server", "redis", "version", "7.4.0", "dragonfly_version",
                  ArgType(RespExpr::STRING), "proto", IntArg(3), "id", ArgType(RespExpr::INT64),
                  "mode", testing::AnyOf("standalone", "cluster"), "role", "master"));

  EXPECT_THAT(Run({"hello", "2", "AUTH", "uname", "pwd"}),
              ErrArg("WRONGPASS invalid username-password pair or user is disabled."));

  EXPECT_THAT(Run({"hello", "2", "AUTH", "default", "pwd"}),
              ErrArg("WRONGPASS invalid username-password pair or user is disabled."));

  resp = Run({"hello", "3", "AUTH", "default", ""});
  ASSERT_THAT(resp, ErrArg("WRONGPASS invalid username-password pair or user is disabled."));

  TestInitAclFam();

  resp = Run({"hello", "3", "AUTH", "default", "tmp"});
  ASSERT_THAT(resp, ArrLen(14));

  resp = Run({"hello", "3", "AUTH", "default", "tmp", "SETNAME", "myname"});
  ASSERT_THAT(resp, ArrLen(14));
}

TEST_F(DflyEngineTest, Memcache) {
  using MP = MemcacheParser;

  auto resp = RunMC(MP::SET, "key", "bar", 1);
  EXPECT_THAT(resp, ElementsAre("STORED"));

  resp = RunMC(MP::GET, "key");
  EXPECT_THAT(resp, ElementsAre("VALUE key 1 3", "bar", "END"));

  resp = RunMC(MP::ADD, "key", "bar", 1);
  EXPECT_THAT(resp, ElementsAre("NOT_STORED"));

  resp = RunMC(MP::REPLACE, "key2", "bar", 1);
  EXPECT_THAT(resp, ElementsAre("NOT_STORED"));

  resp = RunMC(MP::ADD, "key2", "bar2", 2);
  EXPECT_THAT(resp, ElementsAre("STORED"));

  resp = GetMC(MP::GET, {"key2", "key"});
  EXPECT_THAT(resp, ElementsAre("VALUE key2 2 4", "bar2", "VALUE key 1 3", "bar", "END"));

  resp = RunMC(MP::APPEND, "key2", "val2", 0);
  EXPECT_THAT(resp, ElementsAre("STORED"));
  resp = RunMC(MP::GET, "key2");
  EXPECT_THAT(resp, ElementsAre("VALUE key2 2 8", "bar2val2", "END"));

  resp = RunMC(MP::APPEND, "unkn", "val2", 0);
  EXPECT_THAT(resp, ElementsAre("NOT_STORED"));

  resp = RunMC(MP::GET, "unkn");
  EXPECT_THAT(resp, ElementsAre("END"));
}

TEST_F(DflyEngineTest, MemcacheFlags) {
  using MP = MemcacheParser;

  auto resp = Run("resp", {"SET", "key", "bar", "_MCFLAGS", "42"});
  ASSERT_EQ(resp, "OK");
  MCResponse resp2 = RunMC(MP::GET, "key");
  EXPECT_THAT(resp2, ElementsAre("VALUE key 42 3", "bar", "END"));

  ASSERT_EQ(Run("resp", {"flushdb"}), "OK");
  pp_->AwaitFiberOnAll([](auto*) {
    if (auto* shard = EngineShard::tlocal(); shard) {
      EXPECT_EQ(namespaces->GetDefaultNamespace()
                    .GetDbSlice(shard->shard_id())
                    .GetDBTable(0)
                    ->mcflag.size(),
                0u);
    }
  });
}

TEST_F(DflyEngineTest, LimitMemory) {
  mi_option_enable(mi_option_limit_os_alloc);
  string blob(128, 'a');
  for (size_t i = 0; i < 10000; ++i) {
    auto resp = Run({"set", absl::StrCat(blob, i), blob});
    ASSERT_EQ(resp, "OK");
  }
}

TEST_F(DflyEngineTest, FlushAll) {
  auto fb0 = pp_->at(0)->LaunchFiber([&] { Run({"flushall"}); });

  auto fb1 = pp_->at(1)->LaunchFiber([&] {
    Run({"select", "2"});

    for (size_t i = 1; i < 100; ++i) {
      RespExpr resp = Run({"set", "foo", "bar"});
      ASSERT_EQ(resp, "OK");
      ThisFiber::Yield();
    }
  });

  fb0.Join();
  fb1.Join();
}

TEST_F(DflyEngineTest, OOM) {
  max_memory_limit = 300000;
  size_t i = 0;
  RespExpr resp;
  for (; i < 10000; i += 3) {
    resp = Run({"mset", StrCat("key", i), "bar", StrCat("key", i + 1), "bar", StrCat("key", i + 2),
                "bar"});
    if (resp != "OK")
      break;
    ASSERT_EQ(resp, "OK");
  }
  EXPECT_THAT(resp, ErrArg("Out of mem"));

  string_view commands[5] = {"set", "rpush", "sadd", "zadd", "hset"};
  for (unsigned j = 0; j < ABSL_ARRAYSIZE(commands); ++j) {
    string_view cmd = commands[j];
    vector<string_view> run_args({cmd, ""});
    if (cmd == "zadd") {
      run_args.push_back("1.1");
    } else if (cmd == "hset") {
      run_args.push_back("foo");
    }
    run_args.push_back("bar");

    for (unsigned i = 0; i < 5000; ++i) {
      auto str = StrCat("key", cmd, i);
      run_args[1] = str;
      resp = Run(run_args);

      if (resp.type == RespExpr::ERROR)
        break;

      ASSERT_THAT(resp, testing::AnyOf(IntArg(1), "OK")) << cmd;
    }
    EXPECT_THAT(resp, ErrArg("Out of mem"));
  }
}

#ifndef SANITIZERS
/// Reproduces the case where items with expiry data were evicted,
/// and then written with the same key.
TEST_F(DflyEngineTest, Bug207) {
  max_memory_limit = 300000 * 4;

  // The threshold is set to 0.3 to trigger eviction earlier and prevent OOM.
  absl::FlagSaver fs;
  absl::SetFlag(&FLAGS_eviction_memory_budget_threshold, 0.3);

  shard_set->TEST_EnableCacheMode();

  /* The value should be large enough to avoid being inlined. Heartbeat evicts only objects for
   * which HasAllocated() returns true. */
  std::string value(1000, '.');

  ssize_t i = 0;
  RespExpr resp;
  for (; i < 1000; ++i) {
    resp = Run({"setex", StrCat("key", i), "30", value});
    ASSERT_EQ(resp, "OK");
  }

  auto evicted_count = [](const string& str) -> size_t {
    const string matcher = "evicted_keys:";
    const auto pos = str.find(matcher) + matcher.size();
    const auto sub = str.substr(pos, 1);
    return atoi(sub.c_str());
  };

  resp = Run({"info", "stats"});
  EXPECT_GT(evicted_count(resp.GetString()), 0);

  for (; i > 0; --i) {
    resp = Run({"setex", StrCat("key", i), "30", "bar"});
    ASSERT_EQ(resp, "OK");
  }
}

TEST_F(DflyEngineTest, StickyEviction) {
  max_memory_limit = 600000;  // 0.6mb
  shard_set->TEST_EnableCacheMode();

  string tmp_val(100, '.');

  ssize_t failed = -1;
  for (ssize_t i = 0; i < 5000; ++i) {
    string key = StrCat("volatile", i);
    ASSERT_EQ("OK", Run({"set", key, tmp_val}));
  }

  bool done = false;
  for (ssize_t i = 0; !done && i < 5000; ++i) {
    string key = StrCat("key", i);
    while (true) {
      if (Run({"set", key, tmp_val}) != "OK") {
        failed = i;
        done = true;
        break;
      }

      // Eviction could have happened right after set, before stick. If so, try again
      if (Run({"stick", key}).GetInt() == 1) {
        break;
      }
    }
  }

  ASSERT_GE(failed, 0);
  // Make sure none of the sticky values was evicted
  for (ssize_t i = 0; i < failed; ++i) {
    ASSERT_THAT(Run({"exists", StrCat("key", i)}), IntArg(1));
  }
}

#endif

TEST_F(DflyEngineTest, PSubscribe) {
  single_response_ = false;
  auto resp = pp_->at(1)->Await([&] { return Run({"psubscribe", "a*", "b*"}); });
  EXPECT_THAT(resp, ArrLen(3));
  resp = pp_->at(0)->Await([&] { return Run({"publish", "ab", "foo"}); });
  EXPECT_THAT(resp, IntArg(1));

  pp_->AwaitFiberOnAll([](ProactorBase* pb) {});

  ASSERT_EQ(1, SubscriberMessagesLen("IO1"));

  const auto& msg = GetPublishedMessage("IO1", 0);
  EXPECT_EQ("foo", msg.message);
  EXPECT_EQ("ab", msg.channel);
  EXPECT_EQ("a*", msg.pattern);
}

TEST_F(DflyEngineTest, Unsubscribe) {
  auto resp = Run({"unsubscribe", "a"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("unsubscribe", "a", IntArg(0)));

  resp = Run({"unsubscribe"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("unsubscribe", ArgType(RespExpr::NIL), IntArg(0)));

  single_response_ = false;

  Run({"subscribe", "a", "b"});

  resp = Run({"unsubscribe", "a"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("unsubscribe", "a", IntArg(1)));

  resp = Run({"unsubscribe"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("unsubscribe", "b", IntArg(0)));
}

TEST_F(DflyEngineTest, PUnsubscribe) {
  auto resp = Run({"punsubscribe", "a*"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("punsubscribe", "a*", IntArg(0)));

  resp = Run({"punsubscribe"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("punsubscribe", ArgType(RespExpr::NIL), IntArg(0)));

  single_response_ = false;
  Run({"psubscribe", "a*", "b*"});

  resp = Run({"punsubscribe", "a*"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("punsubscribe", "a*", IntArg(1)));

  resp = Run({"punsubscribe"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("punsubscribe", "b*", IntArg(0)));
}

TEST_F(DflyEngineTest, Bug468) {
  RespExpr resp = Run({"multi"});
  ASSERT_EQ(resp, "OK");
  resp = Run({"SET", "foo", "bar", "EX", "moo"});
  ASSERT_EQ(resp, "QUEUED");

  resp = Run({"exec"});
  ASSERT_THAT(resp, ErrArg("not an integer"));

  ASSERT_FALSE(IsLocked(0, "foo"));

  resp = Run({"eval", "return redis.call('set', 'foo', 'bar', 'EX', 'moo')", "1", "foo"});
  ASSERT_THAT(resp, ErrArg("not an integer"));

  ASSERT_FALSE(IsLocked(0, "foo"));
}

TEST_F(DflyEngineTest, Bug496) {
  shard_set->RunBlockingInParallel([](EngineShard* shard) {
    auto& db = namespaces->GetDefaultNamespace().GetDbSlice(shard->shard_id());

    int cb_hits = 0;
    uint32_t cb_id =
        db.RegisterOnChange([&cb_hits](DbIndex, const DbSlice::ChangeReq&) { cb_hits++; });

    {
      auto res = *db.AddOrFind({}, "key-1");
      EXPECT_TRUE(res.is_new);
      EXPECT_EQ(cb_hits, 1);
    }

    {
      auto res = *db.AddOrFind({}, "key-1");
      EXPECT_FALSE(res.is_new);
      EXPECT_EQ(cb_hits, 2);
    }

    {
      auto res = *db.AddOrFind({}, "key-2");
      EXPECT_TRUE(res.is_new);
      EXPECT_EQ(cb_hits, 3);
    }

    db.UnregisterOnChange(cb_id);
  });
}

TEST_F(DflyEngineTest, Issue607) {
  // https://github.com/dragonflydb/dragonfly/issues/607

  Run({"SET", "key", "value1"});
  EXPECT_EQ(Run({"GET", "key"}), "value1");

  Run({"SET", "key", "value2"});
  EXPECT_EQ(Run({"GET", "key"}), "value2");

  Run({"EXPIRE", "key", "1000"});

  Run({"SET", "key", "value3"});
  EXPECT_EQ(Run({"GET", "key"}), "value3");
}

TEST_F(DflyEngineTest, Issue679) {
  // https://github.com/dragonflydb/dragonfly/issues/679

  Run({"HMSET", "a", "key", "val"});
  Run({"EXPIRE", "a", "1000"});
  Run({"HMSET", "a", "key", "vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv"});
  Run({"EXPIRE", "a", "1001"});
}

TEST_F(DflyEngineTest, Issue742) {
  // https://github.com/dragonflydb/dragonfly/issues/607
  // The stack was not cleaned in case of an error and it blew up.
  for (int i = 0; i < 3'000; i++) {
    Run({"EVAL", "redis.get(KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5])", "5", "k1", "k2", "k3",
         "k4", "k5"});
  }
}

TEST_F(DefragDflyEngineTest, TestDefragOption) {
  if (pp_->GetNextProactor()->GetKind() == util::ProactorBase::EPOLL) {
    GTEST_SKIP() << "Defragmentation via idle task is only supported in io uring";
  }

  // mem_defrag_threshold is based on RSS statistic, but we don't count it in the test
  absl::SetFlag(&FLAGS_mem_defrag_threshold, 0.0);
  absl::SetFlag(&FLAGS_mem_defrag_check_sec_interval, 0);
  absl::SetFlag(&FLAGS_mem_defrag_waste_threshold, 0.1);

  //  Fill data into dragonfly and then check if we have
  //  any location in memory to defrag. See issue #448 for details about this.
  constexpr size_t kMaxMemoryForTest = 1'100'000;
  constexpr int kNumberOfKeys = 1'000;  // this fill the memory
  constexpr int kKeySize = 637;
  constexpr int kMaxDefragTriesForTests = 30;
  constexpr int kFactor = 4;

  max_memory_limit = kMaxMemoryForTest;  // control memory size so no need for too many keys
  std::vector<std::string> keys2delete;
  keys2delete.push_back("del");

  // create keys that we would like to remove, try to make it none adjusting locations
  for (int i = 0; i < kNumberOfKeys; i += kFactor) {
    keys2delete.push_back("key-name:" + std::to_string(i));
  }

  std::vector<std::string_view> keys(keys2delete.begin(), keys2delete.end());

  Run({"SELECT", "2"});

  RespExpr resp = Run(
      {"DEBUG", "POPULATE", std::to_string(kNumberOfKeys), "key-name", std::to_string(kKeySize)});
  ASSERT_EQ(resp, "OK");
  auto r = CheckedInt({"DBSIZE"});

  ASSERT_EQ(r, kNumberOfKeys);

  shard_set->pool()->AwaitFiberOnAll([&](unsigned index, ProactorBase* base) {
    EngineShard* shard = EngineShard::tlocal();
    ASSERT_FALSE(shard == nullptr);  // we only have one and its should not be empty!
    ThisFiber::SleepFor(100ms);

    // make sure that the task that collect memory usage from all shard ran
    // for at least once, and that no defrag was done yet.
    auto stats = shard->stats();
    for (int i = 0; i < 3; i++) {
      ThisFiber::SleepFor(100ms);
      EXPECT_EQ(stats.defrag_realloc_total, 0);
    }
  });

  ArgSlice delete_cmd(keys);
  r = CheckedInt(delete_cmd);
  LOG(INFO) << "finish deleting memory entries " << r;
  // the first element in this is the command del so size is one less
  ASSERT_EQ(r, keys2delete.size() - 1);
  // At this point we need to see whether we did running the task and whether the task did something
  shard_set->pool()->AwaitFiberOnAll([&](unsigned index, ProactorBase* base) {
    EngineShard* shard = EngineShard::tlocal();
    ASSERT_TRUE(shard != nullptr);  // we only have one and its should not be empty!
    // a "busy wait" to ensure that memory defragmentations was successful:
    // the task ran and did it work
    auto stats = shard->stats();
    for (int i = 0; i < kMaxDefragTriesForTests && stats.defrag_realloc_total == 0; i++) {
      stats = shard->stats();
      ThisFiber::SleepFor(220ms);
    }
    // make sure that we successfully found places to defrag in memory
    EXPECT_GT(stats.defrag_realloc_total, 0);
    EXPECT_GE(stats.defrag_attempt_total, stats.defrag_realloc_total);
  });
}

TEST_F(DflyEngineTest, Issue752) {
  // https://github.com/dragonflydb/dragonfly/issues/752
  // local_result_ member was not reset between commands
  Run({"multi"});
  auto resp = Run({"llen", kKey1});
  ASSERT_EQ(resp, "QUEUED");
  resp = Run({"del", kKey1, kKey2});
  ASSERT_EQ(resp, "QUEUED");
  resp = Run({"exec"});
  ASSERT_THAT(resp, ArrLen(2));
  ASSERT_THAT(resp.GetVec(), ElementsAre(IntArg(0), IntArg(0)));
}

TEST_F(DflyEngineTest, Latency) {
  Run({"latency", "latest"});
}

TEST_F(DflyEngineTest, EvalBug2664) {
  absl::FlagSaver fs;
  absl::SetFlag(&FLAGS_lua_resp2_legacy_float, true);

  auto resp = Run({"eval", "return 42.9", "0"});
  EXPECT_THAT(resp, IntArg(42));
  resp = Run({"eval", "return -3.8", "0"});
  EXPECT_THAT(resp, IntArg(-3));

  resp = Run({"hello", "3"});
  ASSERT_THAT(resp, ArrLen(14));

  resp = Run({"eval", "return 42.9", "0"});
  EXPECT_THAT(resp, DoubleArg(42.9));
}

TEST_F(DflyEngineTest, MemoryUsage) {
  for (unsigned i = 0; i < 1000; ++i) {
    Run({"rpush", "l1", StrCat("val", i)});
  }

  for (unsigned i = 0; i < 1000; ++i) {
    Run({"rpush", "l2", StrCat(string(200, 'a'), i)});
  }
  auto resp = Run({"memory", "usage", "l1"});
  EXPECT_GT(*resp.GetInt(), 8000);

  resp = Run({"memory", "usage", "l2"});
  EXPECT_GT(*resp.GetInt(), 100000);
}

TEST_F(DflyEngineTest, DebugObject) {
  Run({"set", "key", "value"});
  Run({"lpush", "l1", "a", "b"});
  Run({"sadd", "s1", "1", "2", "3"});
  Run({"sadd", "s2", "a", "b", "c"});
  Run({"zadd", "z1", "1", "a", "2", "b", "3", "c"});
  Run({"hset", "h1", "a", "1", "b", "2", "c", "3"});
  auto resp = Run({"debug", "object", "key"});
  EXPECT_THAT(resp.GetString(), HasSubstr("encoding:raw"));
  resp = Run({"debug", "object", "l1"});
  EXPECT_THAT(resp.GetString(), HasSubstr("encoding:quicklist"));
  resp = Run({"debug", "object", "s1"});
  EXPECT_THAT(resp.GetString(), HasSubstr("encoding:intset"));
  resp = Run({"debug", "object", "s2"});
  EXPECT_THAT(resp.GetString(), HasSubstr("encoding:dense_set"));
  resp = Run({"debug", "object", "z1"});
  EXPECT_THAT(resp.GetString(), HasSubstr("encoding:listpack"));
}

TEST_F(DflyEngineTest, StreamMemInfo) {
  for (int i = 1; i < 2; ++i) {
    Run({"XADD", "test", std::to_string(i), "var", "val" + std::to_string(i)});
  }

  int64_t stream_mem_first = GetMetrics().db_stats[0].memory_usage_by_type[OBJ_STREAM];
  EXPECT_GT(stream_mem_first, 0);

  auto dump = Run({"dump", "test"});
  Run({"del", "test"});
  Run({"restore", "test", "0", facade::ToSV(dump.GetBuf())});

  int64_t stream_mem_second = GetMetrics().db_stats[0].memory_usage_by_type[OBJ_STREAM];

  // stream_mem_first != stream_mem_second due to a preallocation in XADD command (see
  // STREAM_LISTPACK_MAX_PRE_ALLOCATE)
  EXPECT_GT(stream_mem_second, 0);
}

TEST_F(DflyEngineTest, ReplicaofRejectOnLoad) {
  service_->SwitchState(GlobalState::ACTIVE, GlobalState::LOADING);

  RespExpr res = Run({"REPLICAOF", "localhost", "3779"});

  ASSERT_THAT(res, ErrArg("LOADING Dragonfly is loading the dataset in memory"));
}

// TODO: to test transactions with a single shard since then all transactions become local.
// To consider having a parameter in dragonfly engine controlling number of shards
// unconditionally from number of cpus. TO TEST BLPOP under multi for single/multi argument case.

// Parse Double benchmarks
static void BM_ParseFastFloat(benchmark::State& state) {
  std::vector<std::string> args(100);
  std::random_device rd;

  for (auto& arg : args) {
    arg = std::to_string(std::uniform_real_distribution<double>(0, 1e5)(rd));
  }
  double res;
  while (state.KeepRunning()) {
    for (const auto& arg : args) {
      fast_float::from_chars(arg.data(), arg.data() + arg.size(), res);
    }
  }
}
BENCHMARK(BM_ParseFastFloat);

static void BM_ParseDoubleAbsl(benchmark::State& state) {
  std::vector<std::string> args(100);
  std::random_device rd;
  for (auto& arg : args) {
    arg = std::to_string(std::uniform_real_distribution<double>(0, 1e5)(rd));
  }

  double res;
  while (state.KeepRunning()) {
    for (const auto& arg : args) {
      absl::from_chars(arg.data(), arg.data() + arg.size(), res);
    }
  }
}
BENCHMARK(BM_ParseDoubleAbsl);

}  // namespace dfly
