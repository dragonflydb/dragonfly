// Copyright 2022, Roman Gershman.  All rights reserved.
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
#include "util/uring/uring_pool.h"

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
  UpdateTime(expire_now_ + 1000);
  resp = Run({"get", "key"});
  EXPECT_THAT(resp, ArgType(RespExpr::NIL));

  Run({"set", "key", "val"});
  resp = Run({"pexpireat", "key", absl::StrCat(expire_now_ + 2000)});
  EXPECT_THAT(resp, IntArg(1));

  // override
  resp = Run({"pexpireat", "key", absl::StrCat(expire_now_ + 3000)});
  EXPECT_THAT(resp, IntArg(1));

  UpdateTime(expire_now_ + 2999);
  resp = Run({"get", "key"});
  EXPECT_THAT(resp, "val");

  UpdateTime(expire_now_ + 3000);
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
  ASSERT_THAT(Run({"renamenx", "x", "b"}), IntArg(0)); // b already exists
  ASSERT_THAT(Run({"renamenx", "x", "y"}), IntArg(1));
  ASSERT_EQ(Run({"get", "y"}), x_val);
}

TEST_F(GenericFamilyTest, Stick) {
  // check stick returns zero on non-existent keys
  ASSERT_THAT(Run({"stick", "a", "b"}), IntArg(0));

  for (auto key: {"a", "b", "c", "d"}) {
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

}  // namespace dfly
