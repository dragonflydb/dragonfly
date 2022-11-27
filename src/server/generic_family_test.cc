// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/generic_family.h"

extern "C" {
#include "redis/rdb.h"
}

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

  // pexpire test
  Run({"set", "key", "val"});
  resp = Run({"pexpire", "key", absl::StrCat(2000)});
  EXPECT_THAT(resp, IntArg(1));

  // expire time override
  resp = Run({"pexpire", "key", absl::StrCat(3000)});
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

TEST_F(GenericFamilyTest, Touch) {
  RespExpr resp;

  Run({"mset", "x", "0", "y", "1"});
  resp = Run({"touch", "x", "y", "x"});
  EXPECT_THAT(resp, IntArg(3));

  resp = Run({"touch", "z", "x", "w"});
  EXPECT_THAT(resp, IntArg(1));
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
  ASSERT_THAT(Run({"sort", "list-1", "ALPHA"}).GetVec(),
              ElementsAre("1.2", "10.1", "2.20", "200", "3.5"));
  // desc numeric
  ASSERT_THAT(Run({"sort", "list-1", "DESC"}).GetVec(),
              ElementsAre("200", "10.1", "3.5", "2.20", "1.2"));
  // desc strig
  ASSERT_THAT(Run({"sort", "list-1", "DESC", "ALPHA"}).GetVec(),
              ElementsAre("3.5", "200", "2.20", "10.1", "1.2"));
  // limits
  ASSERT_THAT(Run({"sort", "list-1", "LIMIT", "0", "5"}).GetVec(),
              ElementsAre("1.2", "2.20", "3.5", "10.1", "200"));
  ASSERT_THAT(Run({"sort", "list-1", "LIMIT", "0", "10"}).GetVec(),
              ElementsAre("1.2", "2.20", "3.5", "10.1", "200"));
  ASSERT_THAT(Run({"sort", "list-1", "LIMIT", "2", "2"}).GetVec(), ElementsAre("3.5", "10.1"));
  ASSERT_THAT(Run({"sort", "list-1", "LIMIT", "1", "1"}), "2.20");
  ASSERT_THAT(Run({"sort", "list-1", "LIMIT", "4", "2"}), "200");
  ASSERT_THAT(Run({"sort", "list-1", "LIMIT", "5", "2"}), ArrLen(0));
  // limits desc
  ASSERT_THAT(Run({"sort", "list-1", "DESC", "LIMIT", "0", "5"}).GetVec(),
              ElementsAre("200", "10.1", "3.5", "2.20", "1.2"));
  ASSERT_THAT(Run({"sort", "list-1", "DESC", "LIMIT", "2", "2"}).GetVec(),
              ElementsAre("3.5", "2.20"));
  ASSERT_THAT(Run({"sort", "list-1", "DESC", "LIMIT", "1", "1"}), "10.1");
  ASSERT_THAT(Run({"sort", "list-1", "DESC", "LIMIT", "5", "2"}), ArrLen(0));

  // Test set sort
  Run({"del", "set-1"});
  Run({"sadd", "set-1", "5.3", "4.4", "60", "99.9", "100", "9"});
  ASSERT_THAT(Run({"sort", "set-1"}).GetVec(), ElementsAre("4.4", "5.3", "9", "60", "99.9", "100"));
  ASSERT_THAT(Run({"sort", "set-1", "ALPHA"}).GetVec(),
              ElementsAre("100", "4.4", "5.3", "60", "9", "99.9"));
  ASSERT_THAT(Run({"sort", "set-1", "DESC"}).GetVec(),
              ElementsAre("100", "99.9", "60", "9", "5.3", "4.4"));
  ASSERT_THAT(Run({"sort", "set-1", "DESC", "ALPHA"}).GetVec(),
              ElementsAre("99.9", "9", "60", "5.3", "4.4", "100"));

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

TEST_F(GenericFamilyTest, Persist) {
  auto resp = Run({"set", "mykey", "somevalue"});
  EXPECT_EQ(resp, "OK");
  // Key without expiration time - return 1
  EXPECT_EQ(1, CheckedInt({"persist", "mykey"}));
  // set expiration time and try again
  resp = Run({"EXPIRE", "mykey", "10"});
  EXPECT_EQ(10, CheckedInt({"TTL", "mykey"}));
  EXPECT_EQ(1, CheckedInt({"persist", "mykey"}));
  EXPECT_EQ(-1, CheckedInt({"TTL", "mykey"}));
}

TEST_F(GenericFamilyTest, Dump) {
  // The following would only work for RDB version 9
  // The format was changed at version 10
  // The expected results were taken from running the same with Redis branch 6.2
  ASSERT_THAT(RDB_VERSION, 9);
  uint8_t EXPECTED_STRING_DUMP[13] = {0x00, 0xc0, 0x13, 0x09, 0x00, 0x23, 0x13,
                                      0x6f, 0x4d, 0x68, 0xf6, 0x35, 0x6e};
  uint8_t EXPECTED_HASH_DUMP[] = {0x0d, 0x12, 0x12, 0x00, 0x00, 0x00, 0x0d, 0x00, 0x00, 0x00,
                                  0x02, 0x00, 0x00, 0xfe, 0x13, 0x03, 0xc0, 0xd2, 0x04, 0xff,
                                  0x09, 0x00, 0xb1, 0x0b, 0xae, 0x6c, 0x23, 0x5d, 0x17, 0xaa};
  uint8_t EXPECTED_LIST_DUMP[] = {0x0e, 0x01, 0x0e, 0x0e, 0x00, 0x00, 0x00, 0x0a, 0x00,
                                  0x00, 0x00, 0x01, 0x00, 0x00, 0xfe, 0x14, 0xff, 0x09,
                                  0x00, 0xba, 0x1e, 0xa9, 0x6b, 0xba, 0xfe, 0x2d, 0x3f};

  // Check string dump
  auto resp = Run({"set", "z", "19"});
  EXPECT_EQ(resp, "OK");
  resp = Run({"dump", "z"});
  auto dump = resp.GetBuf();
  CHECK_EQ(ToSV(dump), ToSV(EXPECTED_STRING_DUMP));

  // Check list dump
  EXPECT_EQ(1, CheckedInt({"rpush", "l", "20"}));
  resp = Run({"dump", "l"});
  dump = resp.GetBuf();
  CHECK_EQ(ToSV(dump), ToSV(EXPECTED_LIST_DUMP));

  // Check for hash dump
  EXPECT_EQ(1, CheckedInt({"hset", "z2", "19", "1234"}));
  resp = Run({"dump", "z2"});
  dump = resp.GetBuf();
  CHECK_EQ(ToSV(dump), ToSV(EXPECTED_HASH_DUMP));

  // Check that when running with none existing key we're getting nil
  resp = Run({"dump", "foo"});
  EXPECT_EQ(resp.type, RespExpr::NIL);
}

TEST_F(GenericFamilyTest, Restore) {
  using std::chrono::duration_cast;
  using std::chrono::milliseconds;
  using std::chrono::seconds;
  using std::chrono::system_clock;

  uint8_t STRING_DUMP_REDIS[] = {0x00, 0xc1, 0xd2, 0x04, 0x09, 0x00, 0xd0,
                                 0x75, 0x59, 0x6d, 0x10, 0x04, 0x3f, 0x5c};

  auto resp = Run({"set", "exiting-key", "1234"});
  EXPECT_EQ(resp, "OK");
  // try to restore into existing key - this should failed
  ASSERT_THAT(Run({"restore", "exiting-key", "0", ToSV(STRING_DUMP_REDIS)}),
              ArgType(RespExpr::ERROR));

  // Try restore while setting expiration into the pass
  // note that value for expiration is just some valid unix time stamp from the pass
  resp = Run(
      {"restore", "exiting-key", "1665476212900", ToSV(STRING_DUMP_REDIS), "ABSTTL", "REPLACE"});
  CHECK_EQ(resp, "OK");
  resp = Run({"get", "exiting-key"});
  EXPECT_EQ(resp.type, RespExpr::NIL);  // it was deleted as a result of restore action

  // Test for string that we can successfully load the dumped data and read it back
  resp = Run({"restore", "new-key", "0", ToSV(STRING_DUMP_REDIS)});
  EXPECT_EQ(resp, "OK");
  resp = Run({"get", "new-key"});
  EXPECT_EQ("1234", resp);
  resp = Run({"dump", "new-key"});
  auto dump = resp.GetBuf();
  CHECK_EQ(ToSV(dump), ToSV(STRING_DUMP_REDIS));

  // test for list
  EXPECT_EQ(1, CheckedInt({"rpush", "orig-list", "20"}));
  resp = Run({"dump", "orig-list"});
  dump = resp.GetBuf();
  resp = Run({"restore", "new-list", "10", ToSV(dump)});
  EXPECT_EQ(resp, "OK");
  resp = Run({"lpop", "new-list"});
  EXPECT_EQ("20", resp);

  // run with hash type
  EXPECT_EQ(1, CheckedInt({"hset", "orig-hash", "123", "45678"}));
  resp = Run({"dump", "orig-hash"});
  dump = resp.GetBuf();
  resp = Run({"restore", "new-hash", "1", ToSV(dump)});
  EXPECT_EQ(resp, "OK");
  EXPECT_EQ(1, CheckedInt({"hexists", "new-hash", "123"}));

  // test with replace and no TTL
  resp = Run({"set", "string-key", "hello world"});
  EXPECT_EQ(resp, "OK");
  resp = Run({"dump", "string-key"});
  dump = resp.GetBuf();
  // this will change the value from "hello world" to "1234"
  resp = Run({"restore", "string-key", "7", ToSV(STRING_DUMP_REDIS), "REPLACE"});
  resp = Run({"get", "string-key"});
  EXPECT_EQ("1234", resp);
  // check TTL validity
  EXPECT_EQ(CheckedInt({"ttl", "string-key"}), 7);

  // Make check about ttl with abs time, restoring back to "hello world"
  resp = Run({"restore", "string-key", absl::StrCat(TEST_current_time_ms + 2000), ToSV(dump),
              "ABSTTL", "REPLACE"});
  resp = Run({"get", "string-key"});
  EXPECT_EQ("hello world", resp);
  EXPECT_EQ(CheckedInt({"pttl", "string-key"}), 2000);

  // Last but not least - just make sure that we are good without TTL as well
  resp = Run({"restore", "string-key", "0", ToSV(STRING_DUMP_REDIS), "REPLACE"});
  resp = Run({"get", "string-key"});
  EXPECT_EQ("1234", resp);
  EXPECT_EQ(CheckedInt({"ttl", "string-key"}), -1);
}

}  // namespace dfly
