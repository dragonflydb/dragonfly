// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/generic_family.h"

#include "base/gtest.h"
#include "base/logging.h"
#include "facade/facade_test.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/engine_shard_set.h"
#include "server/string_family.h"
#include "server/test_utils.h"
#include "server/transaction.h"

using namespace testing;
using namespace std;
using namespace util;
using namespace boost;
using absl::StrCat;

namespace dfly {

class GenericFamilyTest : public BaseFamilyTest {};

TEST_F(GenericFamilyTest, Expire) {
  Run({"set", "key", "val"});
  auto resp = Run({"expire", "key", "1"});

  EXPECT_THAT(resp, IntArg(1));
  AdvanceTime(1000);
  resp = Run({"get", "key"});
  EXPECT_THAT(resp, ArgType(RespExpr::NIL));

  Run({"set", "key", "val"});
  resp = Run({"pexpireat", "key", absl::StrCat(TEST_current_time_ms + 2000)});
  EXPECT_THAT(resp, IntArg(1));

  // override
  resp = Run({"pexpireat", "key", absl::StrCat(TEST_current_time_ms + 3000)});
  EXPECT_THAT(resp, IntArg(1));

  AdvanceTime(2999);
  resp = Run({"get", "key"});
  EXPECT_THAT(resp, "val");

  AdvanceTime(1);
  resp = Run({"get", "key"});
  EXPECT_THAT(resp, ArgType(RespExpr::NIL));
}

TEST_F(GenericFamilyTest, Del) {
  for (size_t i = 0; i < 1000; ++i) {
    Run({"set", StrCat("foo", i), "1"});
    Run({"set", StrCat("bar", i), "1"});
  }

  ASSERT_EQ(2000, CheckedInt({"dbsize"}));

  auto exist_fb = pp_->at(0)->LaunchFiber([&] {
    for (size_t i = 0; i < 1000; ++i) {
      int64_t resp = CheckedInt({"exists", StrCat("foo", i), StrCat("bar", i)});
      ASSERT_TRUE(2 == resp || resp == 0) << resp << " " << i;
    }
  });

  auto del_fb = pp_->at(2)->LaunchFiber([&] {
    for (size_t i = 0; i < 1000; ++i) {
      auto resp = CheckedInt({"del", StrCat("foo", i), StrCat("bar", i)});
      ASSERT_EQ(2, resp);
    }
  });

  exist_fb.join();
  del_fb.join();
}

TEST_F(GenericFamilyTest, TTL) {
  EXPECT_EQ(-2, CheckedInt({"ttl", "foo"}));
  EXPECT_EQ(-2, CheckedInt({"pttl", "foo"}));
  Run({"set", "foo", "bar"});
  EXPECT_EQ(-1, CheckedInt({"ttl", "foo"}));
  EXPECT_EQ(-1, CheckedInt({"pttl", "foo"}));
}

TEST_F(GenericFamilyTest, Exists) {
  Run({"mset", "x", "0", "y", "1"});
  auto resp = Run({"exists", "x", "y", "x"});
  EXPECT_THAT(resp, IntArg(3));
}

TEST_F(GenericFamilyTest, Rename) {
  RespExpr resp;
  string b_val(32, 'b');
  string x_val(32, 'x');

  resp = Run({"mset", "x", x_val, "b", b_val});
  ASSERT_EQ(resp, "OK");
  ASSERT_EQ(2, last_cmd_dbg_info_.shards_count);

  resp = Run({"rename", "z", "b"});
  ASSERT_THAT(resp, ErrArg("no such key"));

  resp = Run({"rename", "x", "b"});
  ASSERT_EQ(resp, "OK");

  int64_t val = CheckedInt({"get", "x"});
  ASSERT_EQ(kint64min, val);  // does not exist

  ASSERT_EQ(Run({"get", "b"}), x_val);  // swapped.

  EXPECT_EQ(CheckedInt({"exists", "x", "b"}), 1);

  const char* keys[2] = {"b", "x"};
  auto ren_fb = pp_->at(0)->LaunchFiber([&] {
    for (size_t i = 0; i < 200; ++i) {
      int j = i % 2;
      auto resp = Run({"rename", keys[j], keys[1 - j]});
      ASSERT_EQ(resp, "OK");
    }
  });

  auto exist_fb = pp_->at(2)->LaunchFiber([&] {
    for (size_t i = 0; i < 300; ++i) {
      int64_t resp = CheckedInt({"exists", "x", "b"});
      ASSERT_EQ(1, resp);
    }
  });

  exist_fb.join();
  ren_fb.join();
}

TEST_F(GenericFamilyTest, RenameNonString) {
  EXPECT_EQ(1, CheckedInt({"lpush", "x", "elem"}));
  auto resp = Run({"rename", "x", "b"});
  ASSERT_EQ(resp, "OK");
  ASSERT_EQ(2, last_cmd_dbg_info_.shards_count);

  EXPECT_EQ(0, CheckedInt({"del", "x"}));
  EXPECT_EQ(1, CheckedInt({"del", "b"}));
}

TEST_F(GenericFamilyTest, RenameBinary) {
  const char kKey1[] = "\x01\x02\x03\x04";
  const char kKey2[] = "\x05\x06\x07\x08";

  Run({"set", kKey1, "bar"});
  Run({"rename", kKey1, kKey2});
  EXPECT_THAT(Run({"get", kKey1}), ArgType(RespExpr::NIL));
  EXPECT_EQ(Run({"get", kKey2}), "bar");
}

TEST_F(GenericFamilyTest, RenameNx) {
  // Set two keys
  string b_val(32, 'b');
  string x_val(32, 'x');
  Run({"mset", "x", x_val, "b", b_val});

  ASSERT_THAT(Run({"renamenx", "z", "b"}), ErrArg("no such key"));
  ASSERT_THAT(Run({"renamenx", "x", "b"}), IntArg(0));  // b already exists
  ASSERT_THAT(Run({"renamenx", "x", "y"}), IntArg(1));
  ASSERT_EQ(Run({"get", "y"}), x_val);
}

TEST_F(GenericFamilyTest, Stick) {
  // check stick returns zero on non-existent keys
  ASSERT_THAT(Run({"stick", "a", "b"}), IntArg(0));

  for (auto key : {"a", "b", "c", "d"}) {
    Run({"set", key, "."});
  }

  // check stick is applied only once
  ASSERT_THAT(Run({"stick", "a", "b"}), IntArg(2));
  ASSERT_THAT(Run({"stick", "a", "b"}), IntArg(0));
  ASSERT_THAT(Run({"stick", "a", "c"}), IntArg(1));
  ASSERT_THAT(Run({"stick", "b", "d"}), IntArg(1));
  ASSERT_THAT(Run({"stick", "c", "d"}), IntArg(0));

  // check stickyness persists during writes
  Run({"set", "a", "new"});
  ASSERT_THAT(Run({"stick", "a"}), IntArg(0));
  Run({"append", "a", "-value"});
  ASSERT_THAT(Run({"stick", "a"}), IntArg(0));

  // check rename persists stickyness
  Run({"rename", "a", "k"});
  ASSERT_THAT(Run({"stick", "k"}), IntArg(0));

  // check rename persists stickyness on multiple shards
  Run({"del", "b"});
  string b_val(32, 'b');
  string x_val(32, 'x');
  Run({"mset", "b", b_val, "x", x_val});
  ASSERT_EQ(2, last_cmd_dbg_info_.shards_count);
  Run({"stick", "x"});
  Run({"rename", "x", "b"});
  ASSERT_THAT(Run({"stick", "b"}), IntArg(0));
}

TEST_F(GenericFamilyTest, Move) {
  // Check MOVE returns 0 on non-existent keys
  ASSERT_THAT(Run({"move", "a", "1"}), IntArg(0));

  // Check MOVE catches non-existent database indices
  ASSERT_THAT(Run({"move", "a", "-1"}), ArgType(RespExpr::ERROR));
  ASSERT_THAT(Run({"move", "a", "100500"}), ArgType(RespExpr::ERROR));

  // Check MOVE moves value & expiry & stickyness
  Run({"set", "a", "test"});
  Run({"expire", "a", "1000"});
  Run({"stick", "a"});
  ASSERT_THAT(Run({"move", "a", "1"}), IntArg(1));
  Run({"select", "1"});
  ASSERT_THAT(Run({"get", "a"}), "test");
  ASSERT_THAT(Run({"ttl", "a"}), testing::Not(IntArg(-1)));
  ASSERT_THAT(Run({"stick", "a"}), IntArg(0));

  // Check MOVE doesn't move if key exists
  Run({"select", "1"});
  Run({"set", "a", "test"});
  Run({"select", "0"});
  Run({"set", "a", "another test"});
  ASSERT_THAT(Run({"move", "a", "1"}), IntArg(0));  // exists from test case above
  Run({"select", "1"});
  ASSERT_THAT(Run({"get", "a"}), "test");

  // Check MOVE awakes blocking operations
  auto fb_blpop = pp_->at(0)->LaunchFiber(fibers::launch::dispatch, [&] {
    Run({"select", "1"});
    auto resp = Run({"blpop", "l", "0"});
    ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
    EXPECT_THAT(resp.GetVec(), ElementsAre("l", "TestItem"));
  });

  WaitUntilLocked(1, "l");

  pp_->at(1)->Await([&] {
    Run({"select", "0"});
    Run({"lpush", "l", "TestItem"});
    Run({"move", "l", "1"});
  });

  fb_blpop.join();
}

using testing::AnyOf;
using testing::Each;
using testing::StartsWith;

TEST_F(GenericFamilyTest, Scan) {
  for (unsigned i = 0; i < 10; ++i)
    Run({"set", absl::StrCat("key", i), "bar"});

  for (unsigned i = 0; i < 10; ++i)
    Run({"set", absl::StrCat("str", i), "bar"});

  for (unsigned i = 0; i < 10; ++i)
    Run({"sadd", absl::StrCat("set", i), "bar"});

  for (unsigned i = 0; i < 10; ++i)
    Run({"zadd", absl::StrCat("zset", i), "0", "bar"});

  auto resp = Run({"scan", "0", "count", "20", "type", "string"});
  EXPECT_THAT(resp, ArrLen(2));
  auto vec = StrArray(resp.GetVec()[1]);
  EXPECT_GT(vec.size(), 10);
  EXPECT_THAT(vec, Each(AnyOf(StartsWith("str"), StartsWith("key"))));

  resp = Run({"scan", "0", "count", "20", "match", "zset*"});
  vec = StrArray(resp.GetVec()[1]);
  EXPECT_EQ(10, vec.size());
  EXPECT_THAT(vec, Each(StartsWith("zset")));
}

TEST_F(GenericFamilyTest, Sort) {
  // Test list sort with params
  Run({"del", "list-1"});
  Run({"lpush", "list-1", "3.5", "1.2", "10.1", "2.20", "200"});
  // numeric
  ASSERT_THAT(Run({"sort", "list-1"}).GetVec(), ElementsAre("1.2", "2.20", "3.5", "10.1", "200"));
  // string
  ASSERT_THAT(Run({"sort", "list-1", "ALPHA"}).GetVec(), ElementsAre("1.2", "10.1", "2.20", "200", "3.5"));
  // desc numeric
  ASSERT_THAT(Run({"sort", "list-1", "DESC"}).GetVec(), ElementsAre("200", "10.1", "3.5", "2.20", "1.2"));
  // desc strig
  ASSERT_THAT(Run({"sort", "list-1", "DESC", "ALPHA"}).GetVec(), ElementsAre("3.5", "200", "2.20", "10.1", "1.2"));
  // limits
  ASSERT_THAT(Run({"sort", "list-1", "LIMIT", "0", "5"}).GetVec(), ElementsAre("1.2", "2.20", "3.5", "10.1", "200"));
  ASSERT_THAT(Run({"sort", "list-1", "LIMIT", "0", "10"}).GetVec(), ElementsAre("1.2", "2.20", "3.5", "10.1", "200"));
  ASSERT_THAT(Run({"sort", "list-1", "LIMIT", "2", "2"}).GetVec(), ElementsAre("3.5", "10.1"));
  ASSERT_THAT(Run({"sort", "list-1", "LIMIT", "1", "1"}), "2.20");
  ASSERT_THAT(Run({"sort", "list-1", "LIMIT", "4", "2"}), "200");
  ASSERT_THAT(Run({"sort", "list-1", "LIMIT", "5", "2"}), ArrLen(0));
  // limits desc
  ASSERT_THAT(Run({"sort", "list-1", "DESC", "LIMIT", "0", "5"}).GetVec(), ElementsAre("200", "10.1", "3.5", "2.20", "1.2"));
  ASSERT_THAT(Run({"sort", "list-1", "DESC", "LIMIT", "2", "2"}).GetVec(), ElementsAre("3.5", "2.20"));
  ASSERT_THAT(Run({"sort", "list-1", "DESC", "LIMIT", "1", "1"}), "10.1");
  ASSERT_THAT(Run({"sort", "list-1", "DESC", "LIMIT", "5", "2"}), ArrLen(0));

  // Test set sort
  Run({"del", "set-1"});
  Run({"sadd", "set-1", "5.3", "4.4", "60", "99.9", "100", "9"});
  ASSERT_THAT(Run({"sort", "set-1"}).GetVec(), ElementsAre("4.4", "5.3", "9", "60", "99.9", "100"));
  ASSERT_THAT(Run({"sort", "set-1", "ALPHA"}).GetVec(), ElementsAre("100", "4.4", "5.3", "60", "9", "99.9"));
  ASSERT_THAT(Run({"sort", "set-1", "DESC"}).GetVec(), ElementsAre("100", "99.9", "60", "9", "5.3", "4.4"));
  ASSERT_THAT(Run({"sort", "set-1", "DESC", "ALPHA"}).GetVec(), ElementsAre("99.9", "9", "60", "5.3", "4.4", "100"));

  // Test intset sort
  Run({"del", "intset-1"});
  Run({"sadd", "intset-1", "5", "4", "3", "2", "1"});
  ASSERT_THAT(Run({"sort", "intset-1"}).GetVec(), ElementsAre("1", "2", "3", "4", "5"));

  // Test sorted set sort
  Run({"del", "zset-1"});
  Run({"zadd", "zset-1", "0", "3.3", "0", "30.1", "0", "8.2"});
  ASSERT_THAT(Run({"sort", "zset-1"}).GetVec(), ElementsAre("3.3", "8.2", "30.1"));
  ASSERT_THAT(Run({"sort", "zset-1", "ALPHA"}).GetVec(), ElementsAre("3.3", "30.1", "8.2"));
  ASSERT_THAT(Run({"sort", "zset-1", "DESC"}).GetVec(), ElementsAre("30.1", "8.2", "3.3"));
  ASSERT_THAT(Run({"sort", "zset-1", "DESC", "ALPHA"}).GetVec(), ElementsAre("8.2", "30.1", "3.3"));

  // Test sort with non existent key
  Run({"del", "list-2"});
  ASSERT_THAT(Run({"sort", "list-2"}), ArrLen(0));

  // Test not convertible to double
  Run({"lpush", "list-2", "NOTADOUBLE"});
  ASSERT_THAT(Run({"sort", "list-2"}), ErrArg("One or more scores can't be converted into double"));
}

TEST_F(GenericFamilyTest, Time) {
  auto resp = Run({"time"});
  EXPECT_THAT(resp, ArrLen(2));
  EXPECT_THAT(resp.GetVec()[0], ArgType(RespExpr::INT64));
  EXPECT_THAT(resp.GetVec()[1], ArgType(RespExpr::INT64));

  // Check that time is the same inside a transaction.
  Run({"multi"});
  Run({"time"});
  usleep(2000);
  Run({"time"});
  resp = Run({"exec"});
  EXPECT_THAT(resp, ArrLen(2));

  ASSERT_THAT(resp.GetVec()[0], ArrLen(2));
  ASSERT_THAT(resp.GetVec()[1], ArrLen(2));

  for (int i = 0; i < 2; ++i) {
    int64_t val0 = get<int64_t>(resp.GetVec()[0].GetVec()[i].u);
    int64_t val1 = get<int64_t>(resp.GetVec()[1].GetVec()[i].u);
    EXPECT_EQ(val0, val1);
  }
}

}  // namespace dfly
