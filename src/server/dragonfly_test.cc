// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

extern "C" {
#include "redis/sds.h"
#include "redis/zmalloc.h"
}

#include <absl/strings/ascii.h>
#include <absl/strings/str_join.h>
#include <absl/strings/strip.h>
#include <gmock/gmock.h>

#include "base/gtest.h"
#include "base/logging.h"
#include "facade/facade_test.h"
#include "server/conn_context.h"
#include "server/main_service.h"
#include "server/test_utils.h"

namespace dfly {

using namespace std;
using namespace util;
using absl::StrCat;
using ::io::Result;
using testing::ElementsAre;
using testing::HasSubstr;
namespace this_fiber = boost::this_fiber;

namespace {

constexpr unsigned kPoolThreadCount = 4;

const char kKey1[] = "x";
const char kKey2[] = "b";
const char kKey3[] = "c";
const char kKey4[] = "y";

}  // namespace

// This test is responsible for server and main service
// (connection, transaction etc) families.
class DflyEngineTest : public BaseFamilyTest {
 protected:
  DflyEngineTest() : BaseFamilyTest() {
    num_threads_ = kPoolThreadCount;
  }
};

class DefragDflyEngineTest : public DflyEngineTest {
 protected:
  DefragDflyEngineTest() : DflyEngineTest() {
    num_threads_ = 1;
  }
};

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

TEST_F(DflyEngineTest, MultiAndEval) {
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

TEST_F(DflyEngineTest, Multi) {
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

TEST_F(DflyEngineTest, MultiEmpty) {
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

TEST_F(DflyEngineTest, MultiSeq) {
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

TEST_F(DflyEngineTest, MultiConsistent) {
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
    this_fiber::sleep_for(1ms);

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

  mset_fb.join();
  fb.join();
  ASSERT_FALSE(service_->IsLocked(0, kKey1));
  ASSERT_FALSE(service_->IsLocked(0, kKey4));
  ASSERT_FALSE(service_->IsShardSetLocked());
}

TEST_F(DflyEngineTest, MultiWeirdCommands) {
  // FIXME: issue https://github.com/dragonflydb/dragonfly/issues/457
  // once we would have fix for supporting EVAL from within transaction
  Run({"multi"});
  EXPECT_THAT(Run({"eval", "return 42", "0"}),
              ErrArg("'EVAL' Dragonfly does not allow execution of a server-side Lua script inside "
                     "transaction block"));
}

TEST_F(DflyEngineTest, MultiRename) {
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

TEST_F(DflyEngineTest, MultiHop) {
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

  p1_fb.join();
  p2_fb.join();
}

TEST_F(DflyEngineTest, FlushDb) {
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

  fb0.join();

  ASSERT_FALSE(service_->IsLocked(0, kKey1));
  ASSERT_FALSE(service_->IsLocked(0, kKey4));
  ASSERT_FALSE(service_->IsShardSetLocked());
}

TEST_F(DflyEngineTest, Eval) {
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
  EXPECT_EQ(2, GetDebugInfo().shards_count);
  EXPECT_THAT(resp, IntArg(0));
}

TEST_F(DflyEngineTest, EvalResp) {
  auto resp = Run({"eval", "return 43", "0"});
  EXPECT_THAT(resp, IntArg(43));

  resp = Run({"eval", "return {5, 'foo', 17.5}", "0"});
  ASSERT_THAT(resp, ArrLen(3));
  EXPECT_THAT(resp.GetVec(), ElementsAre(IntArg(5), "foo", "17.5"));
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

TEST_F(DflyEngineTest, Hello) {
  auto resp = Run({"hello"});
  ASSERT_THAT(resp, ArrLen(12));
  resp = Run({"hello", "2"});
  ASSERT_THAT(resp, ArrLen(12));

  EXPECT_THAT(resp.GetVec(), ElementsAre("server", "redis", "version", ArgType(RespExpr::STRING),
                                         "proto", IntArg(2), "id", ArgType(RespExpr::INT64), "mode",
                                         "standalone", "role", "master"));

  // These are valid arguments to HELLO, however as they are not yet supported the implementation
  // is degraded to 'unknown command'.
  EXPECT_THAT(Run({"hello", "3"}),
              ErrArg("ERR unknown command 'HELLO' with args beginning with: `3`"));
  EXPECT_THAT(
      Run({"hello", "2", "AUTH", "uname", "pwd"}),
      ErrArg("ERR unknown command 'HELLO' with args beginning with: `2`, `AUTH`, `uname`, `pwd`"));
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
      this_fiber::yield();
    }
  });

  fb0.join();
  fb1.join();
}

TEST_F(DflyEngineTest, OOM) {
  shard_set->TEST_EnableHeartBeat();
  max_memory_limit = 0;
  size_t i = 0;
  RespExpr resp;
  for (; i < 5000; i += 3) {
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
      run_args[1] = StrCat("key", cmd, i);
      resp = Run(run_args);

      if (resp.type == RespExpr::ERROR)
        break;

      ASSERT_THAT(resp, testing::AnyOf(IntArg(1), "OK")) << cmd;
    }
    EXPECT_THAT(resp, ErrArg("Out of mem"));
  }
}

/// Reproduces the case where items with expiry data were evicted,
/// and then written with the same key.
TEST_F(DflyEngineTest, Bug207) {
  shard_set->TEST_EnableHeartBeat();
  shard_set->TEST_EnableCacheMode();

  max_memory_limit = 0;

  ssize_t i = 0;
  RespExpr resp;
  for (; i < 5000; ++i) {
    resp = Run({"setex", StrCat("key", i), "30", "bar"});
    // we evict some items because 5000 is too much when max_memory_limit is zero.
    ASSERT_EQ(resp, "OK");
  }

  for (; i > 0; --i) {
    resp = Run({"setex", StrCat("key", i), "30", "bar"});
  }
}

TEST_F(DflyEngineTest, StickyEviction) {
  shard_set->TEST_EnableHeartBeat();
  shard_set->TEST_EnableCacheMode();
  max_memory_limit = 300000;

  string tmp_val(100, '.');

  ssize_t failed = -1;
  for (ssize_t i = 0; i < 5000; ++i) {
    string key = StrCat("volatile", i);
    ASSERT_EQ("OK", Run({"set", key, tmp_val}));
  }

  for (ssize_t i = 0; i < 5000; ++i) {
    string key = StrCat("key", i);
    auto set_resp = Run({"set", key, tmp_val});
    auto stick_resp = Run({"stick", key});

    if (set_resp != "OK") {
      failed = i;
      break;
    }
    ASSERT_THAT(stick_resp, IntArg(1)) << i;
  }

  ASSERT_GE(failed, 0);
  // Make sure neither of the sticky values was evicted
  for (ssize_t i = 0; i < failed; ++i) {
    ASSERT_THAT(Run({"exists", StrCat("key", i)}), IntArg(1));
  }
}

TEST_F(DflyEngineTest, PSubscribe) {
  single_response_ = false;
  auto resp = pp_->at(1)->Await([&] { return Run({"psubscribe", "a*", "b*"}); });
  EXPECT_THAT(resp, ArrLen(3));
  resp = pp_->at(0)->Await([&] { return Run({"publish", "ab", "foo"}); });
  EXPECT_THAT(resp, IntArg(1));

  ASSERT_EQ(1, SubscriberMessagesLen("IO1"));

  facade::Connection::PubMessage msg = GetPublishedMessage("IO1", 0);
  EXPECT_EQ("foo", *msg.message);
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

TEST_F(DflyEngineTest, Watch) {
  auto kExecFail = ArgType(RespExpr::NIL);
  auto kExecSuccess = ArgType(RespExpr::ARRAY);

  // Check watch doesn't run in multi.
  Run({"multi"});
  ASSERT_THAT(Run({"watch", "a"}), ErrArg("WATCH inside MULTI is not allowed"));
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

TEST_F(DflyEngineTest, Bug468) {
  RespExpr resp = Run({"multi"});
  ASSERT_EQ(resp, "OK");
  resp = Run({"SET", "foo", "bar", "EX", "moo"});
  ASSERT_EQ(resp, "QUEUED");

  resp = Run({"exec"});
  ASSERT_THAT(resp, ErrArg("not an integer"));
  ASSERT_FALSE(service_->IsLocked(0, "foo"));

  resp = Run({"eval", "return redis.call('set', 'foo', 'bar', 'EX', 'moo')", "1", "foo"});
  ASSERT_THAT(resp, ErrArg("not an integer"));

  ASSERT_FALSE(service_->IsLocked(0, "foo"));
}

TEST_F(DflyEngineTest, Bug496) {
  shard_set->pool()->AwaitFiberOnAll([&](unsigned index, ProactorBase* base) {
    EngineShard* shard = EngineShard::tlocal();
    if (shard == nullptr)
      return;

    auto& db = shard->db_slice();

    int cb_hits = 0;
    uint32_t cb_id =
        db.RegisterOnChange([&cb_hits](DbIndex, const DbSlice::ChangeReq&) { cb_hits++; });

    auto [_, added] = db.AddOrFind({}, "key-1");
    EXPECT_TRUE(added);
    EXPECT_EQ(cb_hits, 1);

    tie(_, added) = db.AddOrFind({}, "key-1");
    EXPECT_FALSE(added);
    EXPECT_EQ(cb_hits, 1);

    tie(_, added) = db.AddOrFind({}, "key-2");
    EXPECT_TRUE(added);
    EXPECT_EQ(cb_hits, 2);

    db.UnregisterOnChange(cb_id);
  });
}

TEST_F(DefragDflyEngineTest, TestDefragOption) {
  // Fill data into dragonfly and then check if we have
  // any location in memory to defrag. See issue #448 for details about this.
  max_memory_limit = 300'000;             // control memory size so no need for too many keys
  constexpr int kNumberOfKeys = 100'000;  // this fill the memory
  constexpr int kKeySize = 137;
  constexpr int kMaxDefragTriesForTests = 10;

  std::vector<std::string> keys2delete;
  keys2delete.push_back("del");

  // Generate a list of keys that would be deleted
  // The keys that we will delete are all in the form of "key-name:1<other digits>"
  // This is because we are populating keys that has this format, but we don't want
  // to delete all keys, only some random keys so we deleting those that start with 1
  constexpr int kFactor = 10;
  int kMaxNumKeysToDelete = 10'000;
  int current_step = kFactor;
  for (int i = 1; i < kMaxNumKeysToDelete; current_step *= kFactor) {
    for (; i < current_step; i++) {
      int j = i - 1 + current_step;
      keys2delete.push_back("key-name:" + std::to_string(j));
    }
  }

  std::vector<std::string_view> keys(keys2delete.begin(), keys2delete.end());

  RespExpr resp = Run(
      {"DEBUG", "POPULATE", std::to_string(kNumberOfKeys), "key-name", std::to_string(kKeySize)});
  ASSERT_EQ(resp, "OK");
  resp = Run({"DBSIZE"});
  EXPECT_THAT(resp, IntArg(kNumberOfKeys));

  shard_set->pool()->AwaitFiberOnAll([&](unsigned index, ProactorBase* base) {
    EngineShard* shard = EngineShard::tlocal();
    ASSERT_FALSE(shard == nullptr);  // we only have one and its should not be empty!
    this_fiber::sleep_for(100ms);
    EXPECT_EQ(shard->GetDefragStats().success_count, 0);
    // we are expecting to have at least one try by now
    EXPECT_GT(shard->GetDefragStats().tries, 0);
  });

  ArgSlice delete_cmd(keys);
  auto r = CheckedInt(delete_cmd);
  // the first element in this is the command del so size is one less
  ASSERT_EQ(r, keys2delete.size() - 1);

  // At this point we need to see whether we did running the task and whether the task did something
  shard_set->pool()->AwaitFiberOnAll([&](unsigned index, ProactorBase* base) {
    EngineShard* shard = EngineShard::tlocal();
    ASSERT_FALSE(shard == nullptr);  // we only have one and its should not be empty!
    // a "busy wait" to ensure that memory defragmentations was successful:
    // the task ran and did it work
    auto stats = shard->GetDefragStats();
    for (int i = 0; i < kMaxDefragTriesForTests; i++) {
      stats = shard->GetDefragStats();
      if (stats.success_count > 0) {
        break;
      }
      this_fiber::sleep_for(220ms);
    }
    // make sure that we successfully found places to defrag in memory
    EXPECT_GT(stats.success_count, 0);
  });
}

// TODO: to test transactions with a single shard since then all transactions become local.
// To consider having a parameter in dragonfly engine controlling number of shards
// unconditionally from number of cpus. TO TEST BLPOP under multi for single/multi argument case.

}  // namespace dfly
