// Copyright 2022, Roman Gershman.  All rights reserved.
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
#include "util/uring/uring_pool.h"

namespace dfly {

using namespace absl;
using namespace boost;
using namespace std;
using namespace util;
using ::io::Result;
using testing::ElementsAre;
using testing::HasSubstr;

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
  Run({"multi"});
  ASSERT_EQ(Run({"eval", "return 42", "0"}), "QUEUED");
  EXPECT_THAT(Run({"exec"}), IntArg(42));
}

TEST_F(DflyEngineTest, MultiRename) {
  RespExpr resp = Run({"multi"});
  ASSERT_EQ(resp, "OK");
  Run({"set", kKey1, "1"});

  resp = Run({"rename", kKey1, kKey4});
  ASSERT_EQ(resp, "QUEUED");
  resp = Run({"exec"});

  ASSERT_THAT(resp, ArrLen(2));
  EXPECT_THAT(resp.GetVec(), ElementsAre("OK", "OK"));
  ASSERT_FALSE(service_->IsLocked(0, kKey1));
  ASSERT_FALSE(service_->IsLocked(0, kKey4));
  ASSERT_FALSE(service_->IsShardSetLocked());
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
  auto resp = Run({"incrby", "foo", "42"});
  EXPECT_THAT(resp, IntArg(42));

  resp = Run({"eval", "return redis.call('get', 'foo')", "0"});
  EXPECT_THAT(resp, ErrArg("undeclared"));

  resp = Run({"eval", "return redis.call('get', 'foo')", "1", "bar"});
  EXPECT_THAT(resp, ErrArg("undeclared"));

  ASSERT_FALSE(service_->IsLocked(0, "foo"));

  resp = Run({"eval", "return redis.call('get', 'foo')", "1", "foo"});
  EXPECT_THAT(resp, "42");

  resp = Run({"eval", "return redis.call('get', KEYS[1])", "1", "foo"});
  EXPECT_THAT(resp, "42");

  ASSERT_FALSE(service_->IsLocked(0, "foo"));
  ASSERT_FALSE(service_->IsShardSetLocked());
}

TEST_F(DflyEngineTest, EvalResp) {
  auto resp = Run({"eval", "return 43", "0"});
  EXPECT_THAT(resp, IntArg(43));

  resp = Run({"eval", "return {5, 'foo', 17.5}", "0"});
  ASSERT_THAT(resp, ArrLen(3));
  EXPECT_THAT(resp.GetVec(), ElementsAre(IntArg(5), "foo", "17.5"));
}

TEST_F(DflyEngineTest, EvalSha) {
  auto resp = Run({"script", "load", "return 5"});
  EXPECT_THAT(resp, ArgType(RespExpr::STRING));

  string sha{ToSV(resp.GetBuf())};

  resp = Run({"evalsha", sha, "0"});
  EXPECT_THAT(resp, IntArg(5));

  resp = Run({"script", "load", " return 5  "});
  EXPECT_EQ(resp, sha);

  absl::AsciiStrToUpper(&sha);
  resp = Run({"evalsha", sha, "0"});
  EXPECT_THAT(resp, IntArg(5));

  resp = Run({"evalsha", "foobar", "0"});
  EXPECT_THAT(resp, ErrArg("No matching"));
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
  for (; i < 10000; i += 3) {
    resp = Run({"mset", StrCat("key", i), "bar", StrCat("key", i + 1), "bar", StrCat("key", i + 2),
                "bar"});
    if (resp != "OK")
      break;
    ASSERT_EQ(resp, "OK");
  }

  EXPECT_THAT(resp, ErrArg("Out of mem"));
  for (; i < 10000; ++i) {
    resp = Run({"set", StrCat("key", i), "bar"});
    if (resp != "OK")
      break;
  }
  EXPECT_THAT(resp, ErrArg("Out of mem"));

  for (; i < 10000; ++i) {
    resp = Run({"rpush", StrCat("list", i), "bar"});

    if (resp.type == RespExpr::ERROR)
      break;
    ASSERT_THAT(resp, IntArg(1));
  }
  EXPECT_THAT(resp, ErrArg("Out of mem"));
}

// TODO: to test transactions with a single shard since then all transactions become local.
// To consider having a parameter in dragonfly engine controlling number of shards
// unconditionally from number of cpus. TO TEST BLPOP under multi for single/multi argument case.

}  // namespace dfly
