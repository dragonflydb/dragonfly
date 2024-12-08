// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/server_family.h"

#include <absl/strings/match.h>

#include "absl/strings/str_cat.h"
#include "base/gtest.h"
#include "base/logging.h"
#include "facade/facade_test.h"
#include "server/test_utils.h"

using namespace testing;
using namespace std;
using namespace util;
using namespace boost;

namespace dfly {

class ServerFamilyTest : public BaseFamilyTest {
 protected:
};

TEST_F(ServerFamilyTest, SlowLogArgsCountTruncation) {
  auto resp = Run({"config", "set", "slowlog_max_len", "3"});
  EXPECT_THAT(resp.GetString(), "OK");
  resp = Run({"config", "set", "slowlog_log_slower_than", "0"});
  EXPECT_THAT(resp.GetString(), "OK");
  // allow exactly 32 arguments (lpush <key> + 30 arguments)
  // no truncation should happen
  std::vector<std::string> cmd_args = {"LPUSH", "mykey"};
  for (int i = 1; i <= 30; ++i) {
    cmd_args.push_back(std::to_string(i));
  }
  absl::Span<std::string> span(cmd_args);
  resp = Run(span);
  EXPECT_THAT(resp.GetInt(), 30);
  resp = Run({"slowlog", "get"});
  auto slowlog = resp.GetVec();
  auto commands = slowlog[0].GetVec()[3].GetVec();
  EXPECT_THAT(commands, ElementsAreArray(cmd_args));

  // now add one more argument, and truncation SHOULD happen
  std::vector<std::string> cmd_args_one_too_many = {"LPUSH", "mykey"};
  for (int i = 1; i <= 31; ++i) {
    cmd_args_one_too_many.push_back(std::to_string(i));
  }
  absl::Span<std::string> span2(cmd_args_one_too_many);
  resp = Run(span2);
  EXPECT_THAT(resp.GetInt(), 30 + 31);

  resp = Run({"slowlog", "get"});
  slowlog = resp.GetVec();
  auto expected_args = cmd_args;
  expected_args[31] = "... (2 more arguments)";

  commands = slowlog[0].GetVec()[3].GetVec();
  EXPECT_THAT(commands, ElementsAreArray(expected_args));
}

TEST_F(ServerFamilyTest, SlowLogArgsLengthTruncation) {
  auto resp = Run({"config", "set", "slowlog_max_len", "3"});
  EXPECT_THAT(resp.GetString(), "OK");
  resp = Run({"config", "set", "slowlog_log_slower_than", "0"});
  EXPECT_THAT(resp.GetString(), "OK");

  std::string at_limit = std::string(128, 'A');
  resp = Run({"lpush", "mykey", at_limit});
  EXPECT_THAT(resp.GetInt(), 1);

  resp = Run({"slowlog", "get"});
  auto slowlog = resp.GetVec();
  auto key_value = slowlog[0].GetVec()[3].GetVec()[2].GetString();
  EXPECT_THAT(key_value, at_limit);

  std::string over_limit_by_one = std::string(129, 'A');
  std::string expected_value = std::string(110, 'A') + "... (1 more bytes)";
  resp = Run({"lpush", "mykey2", over_limit_by_one});
  EXPECT_THAT(resp.GetInt(), 1);
  resp = Run({"slowlog", "get"});
  slowlog = resp.GetVec();
  key_value = slowlog[0].GetVec()[3].GetVec()[2].GetString();
  EXPECT_THAT(key_value, expected_value);
}

TEST_F(ServerFamilyTest, SlowLogHelp) {
  auto resp = Run({"slowlog", "help"});

  EXPECT_THAT(
      resp.GetVec(),
      ElementsAre(
          "SLOWLOG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:", "GET [<count>]",
          "    Return top <count> entries from the slowlog (default: 10, -1 mean all).",
          "    Entries are made of:",
          "    id, timestamp, time in microseconds, arguments array, client IP and port,",
          "    client name", "LEN", "    Return the length of the slowlog.", "RESET",
          "    Reset the slowlog.", "HELP", "    Prints this help."));
}

TEST_F(ServerFamilyTest, SlowLogMaxLengthZero) {
  auto resp = Run({"config", "set", "slowlog_max_len", "0"});
  EXPECT_THAT(resp.GetString(), "OK");
  resp = Run({"config", "set", "slowlog_log_slower_than", "0"});
  EXPECT_THAT(resp.GetString(), "OK");
  Run({"slowlog", "reset"});

  // issue an arbitrary command
  resp = Run({"set", "foo", "bar"});
  EXPECT_THAT(resp.GetString(), "OK");
  resp = Run({"slowlog", "get"});

  // slowlog should be empty since max_len is 0
  EXPECT_THAT(resp.GetVec().size(), 0);
}

TEST_F(ServerFamilyTest, SlowLogGetZero) {
  auto resp = Run({"config", "set", "slowlog_max_len", "3"});
  EXPECT_THAT(resp.GetString(), "OK");
  resp = Run({"config", "set", "slowlog_log_slower_than", "0"});
  EXPECT_THAT(resp.GetString(), "OK");

  resp = Run({"lpush", "mykey", "1"});
  EXPECT_THAT(resp.GetInt(), 1);
  resp = Run({"slowlog", "get", "0"});
  EXPECT_THAT(resp.GetVec().size(), 0);
}

TEST_F(ServerFamilyTest, SlowLogGetMinusOne) {
  auto resp = Run({"config", "set", "slowlog_max_len", "3"});
  EXPECT_THAT(resp.GetString(), "OK");
  resp = Run({"config", "set", "slowlog_log_slower_than", "0"});
  EXPECT_THAT(resp.GetString(), "OK");

  for (int i = 1; i < 4; ++i) {
    resp = Run({"lpush", "mykey", std::to_string(i)});
    EXPECT_THAT(resp.GetInt(), i);
  }

  // -1 should return the whole slowlog
  resp = Run({"slowlog", "get", "-1"});
  EXPECT_THAT(resp.GetVec().size(), 3);
}

TEST_F(ServerFamilyTest, SlowLogGetLessThanMinusOne) {
  auto resp = Run({"slowlog", "get", "-2"});
  EXPECT_THAT(resp.GetString(), "ERR count should be greater than or equal to -1");
}

TEST_F(ServerFamilyTest, SlowLogLen) {
  auto resp = Run({"config", "set", "slowlog_max_len", "3"});
  EXPECT_THAT(resp.GetString(), "OK");
  resp = Run({"config", "set", "slowlog_log_slower_than", "0"});
  EXPECT_THAT(resp.GetString(), "OK");
  Run({"slowlog", "reset"});

  for (int i = 1; i < 4; ++i) {
    resp = Run({"lpush", "mykey", std::to_string(i)});
    EXPECT_THAT(resp.GetInt(), i);
  }

  resp = Run({"slowlog", "len"});
  EXPECT_THAT(resp.GetInt(), 3);
}

TEST_F(ServerFamilyTest, SlowLogMinusOneDisabled) {
  auto resp = Run({"config", "set", "slowlog_max_len", "3"});
  EXPECT_THAT(resp.GetString(), "OK");
  resp = Run({"config", "set", "slowlog_log_slower_than", "-1"});
  EXPECT_THAT(resp.GetString(), "OK");
  Run({"slowlog", "reset"});

  // issue some commands
  for (int i = 1; i < 4; ++i) {
    resp = Run({"lpush", "mykey", std::to_string(i)});
    EXPECT_THAT(resp.GetInt(), i);
  }

  // slowlog is still empty
  resp = Run({"slowlog", "get"});
  EXPECT_THAT(resp.GetVec().size(), 0);
  resp = Run({"slowlog", "len"});
  EXPECT_THAT(resp.GetInt(), 0);
}

TEST_F(ServerFamilyTest, ClientPause) {
  auto start = absl::Now();
  Run({"CLIENT", "PAUSE", "50"});

  Run({"get", "key"});
  EXPECT_GT((absl::Now() - start), absl::Milliseconds(50));

  start = absl::Now();

  Run({"CLIENT", "PAUSE", "50", "WRITE"});

  Run({"get", "key"});
  EXPECT_LT((absl::Now() - start), absl::Milliseconds(10));
  Run({"set", "key", "value2"});
  EXPECT_GT((absl::Now() - start), absl::Milliseconds(50));
}

TEST_F(ServerFamilyTest, ClientTrackingOnAndOff) {
  // case 1. can't use the feature for resp2
  auto resp = Run({"CLIENT", "TRACKING", "ON"});
  EXPECT_THAT(resp.GetString(),
              "ERR Client tracking is currently not supported for RESP2. Please use RESP3.");

  // case 2. allows when resp3 is used
  Run({"HELLO", "3"});
  resp = Run({"CLIENT", "TRACKING", "ON"});
  EXPECT_THAT(resp.GetString(), "OK");

  resp = Run({"CLIENT", "CACHING", "YES"});
  EXPECT_THAT(
      resp, ErrArg("ERR CLIENT CACHING YES is only valid when tracking is enabled in OPTIN mode"));

  resp = Run({"CLIENT", "CACHING", "NO"});
  EXPECT_THAT(
      resp, ErrArg("ERR CLIENT CACHING NO is only valid when tracking is enabled in OPTOUT mode"));

  // case 3. turn off client tracking
  resp = Run({"CLIENT", "TRACKING", "OFF"});
  EXPECT_THAT(resp.GetString(), "OK");
}

TEST_F(ServerFamilyTest, ClientTrackingReadKey) {
  // case 1. only read the keys doesn't trigger any notification.
  Run({"HELLO", "3"});
  Run({"CLIENT", "TRACKING", "ON"});

  Run({"SET", "FOO", "10"});
  Run({"GET", "FOO"});
  EXPECT_EQ(InvalidationMessagesLen("IO0"), 0);

  Run({"GET", "BAR"});
  EXPECT_EQ(InvalidationMessagesLen("IO0"), 0);
}

TEST_F(ServerFamilyTest, ClientTrackingOptin) {
  Run({"HELLO", "3"});
  Run({"CLIENT", "TRACKING", "ON", "OPTIN"});

  Run({"GET", "FOO"});
  Run({"SET", "FOO", "10"});
  EXPECT_EQ(InvalidationMessagesLen("IO0"), 0);
  Run({"GET", "FOO"});
  EXPECT_EQ(InvalidationMessagesLen("IO0"), 0);

  Run({"CLIENT", "CACHING", "YES"});
  // Start tracking once
  Run({"GET", "FOO"});
  Run({"SET", "FOO", "20"});
  Run({"GET", "FOO"});
  EXPECT_EQ(InvalidationMessagesLen("IO0"), 1);

  Run({"GET", "BAR"});
  Run({"SET", "BAR", "20"});
  Run({"GET", "BAR"});
  EXPECT_EQ(InvalidationMessagesLen("IO0"), 1);

  // Start tracking once
  Run({"CLIENT", "CACHING", "YES"});
  Run({"GET", "BAR"});
  Run({"SET", "BAR", "20"});
  Run({"GET", "BAR"});
  EXPECT_EQ(InvalidationMessagesLen("IO0"), 2);
}

TEST_F(ServerFamilyTest, ClientTrackingMulti) {
  Run({"HELLO", "3"});
  Run({"CLIENT", "TRACKING", "ON"});
  Run({"MULTI"});
  Run({"GET", "FOO"});
  Run({"SET", "TMP", "10"});
  Run({"GET", "FOOBAR"});
  Run({"EXEC"});

  Run({"SET", "FOO", "10"});
  Run({"SET", "FOOBAR", "10"});
  EXPECT_EQ(InvalidationMessagesLen("IO0"), 2);
}

TEST_F(ServerFamilyTest, ClientTrackingCompatibilityMulti) {
  // Compatibility Test, all CLIENT commands should be allowed in MULTI
  Run({"HELLO", "3"});
  Run({"MULTI"});
  auto resp = Run({"CLIENT", "TRACKING", "ON"});
  EXPECT_THAT(resp.GetString(), "QUEUED");
  // Used by sentinel in MULTI/EXEC blocks
  resp = Run({"CLIENT", "KILL", "127.0.0.1:6380"});
  EXPECT_THAT(resp.GetString(), "QUEUED");
  resp = Run({"CLIENT", "SETNAME", "YO"});
  EXPECT_THAT(resp.GetString(), "QUEUED");
  resp = Run({"CLIENT", "GETNAME"});
  EXPECT_THAT(resp.GetString(), "QUEUED");
  Run({"EXEC"});

  Run({"GET", "FOO"});
  Run({"SET", "FOO", "10"});
  EXPECT_EQ(InvalidationMessagesLen("IO0"), 1);

  Run({"MULTI"});
  resp = Run({"CLIENT", "PAUSE", "0", "WRITE"});
  EXPECT_THAT(resp.GetString(), "QUEUED");
  Run({"EXEC"});
}

TEST_F(ServerFamilyTest, ClientTrackingMultiOptin) {
  Run({"HELLO", "3"});
  // Check stickiness
  Run({"CLIENT", "TRACKING", "ON", "OPTIN"});
  Run({"CLIENT", "CACHING", "YES"});
  Run({"MULTI"});
  Run({"GET", "FOO"});
  Run({"SET", "TMP", "10"});
  Run({"GET", "FOOBAR"});
  Run({"DISCARD"});

  Run({"SET", "FOO", "10"});
  EXPECT_EQ(InvalidationMessagesLen("IO0"), 0);

  Run({"CLIENT", "CACHING", "YES"});
  Run({"MULTI"});
  Run({"GET", "FOO"});
  Run({"SET", "TMP", "10"});
  Run({"GET", "FOOBAR"});
  Run({"EXEC"});

  Run({"SET", "FOO", "10"});
  Run({"SET", "FOOBAR", "10"});
  EXPECT_EQ(InvalidationMessagesLen("IO0"), 2);

  // CACHING enclosed in MULTI
  Run({"MULTI"});
  Run({"GET", "TMP"});
  Run({"GET", "TMP_TMP"});
  Run({"SET", "TMP", "10"});
  Run({"CLIENT", "CACHING", "YES"});
  Run({"GET", "FOO"});
  Run({"GET", "FOOBAR"});
  Run({"EXEC"});

  EXPECT_EQ(InvalidationMessagesLen("IO0"), 2);
  Run({"SET", "TMP", "10"});
  EXPECT_EQ(InvalidationMessagesLen("IO0"), 2);
  Run({"SET", "FOO", "10"});
  EXPECT_EQ(InvalidationMessagesLen("IO0"), 3);
  Run({"SET", "FOOBAR", "10"});
  EXPECT_EQ(InvalidationMessagesLen("IO0"), 4);

  // CACHING enclosed in MULTI, ON/OFF
  Run({"MULTI"});
  Run({"GET", "TMP"});
  Run({"SET", "TMP", "10"});
  Run({"CLIENT", "CACHING", "YES"});
  Run({"GET", "FOO"});
  Run({"GET", "BAR"});
  Run({"EXEC"});

  EXPECT_EQ(InvalidationMessagesLen("IO0"), 4);
  Run({"SET", "FOO", "10"});
  Run({"GET", "FOO"});
  EXPECT_EQ(InvalidationMessagesLen("IO0"), 5);
  Run({"SET", "BAR", "10"});
  Run({"GET", "BAR"});
  EXPECT_EQ(InvalidationMessagesLen("IO0"), 6);
}

TEST_F(ServerFamilyTest, ClientTrackingOptout) {
  Run({"HELLO", "3"});
  // Check stickiness
  Run({"CLIENT", "TRACKING", "ON", "OPTOUT"});
  Run({"GET", "FOO"});
  Run({"SET", "FOO", "BAR"});
  Run({"GET", "BAR"});
  Run({"SET", "BAR", "FOO"});
  EXPECT_EQ(InvalidationMessagesLen("IO0"), 2);

  // Switch off tracking for a single command
  Run({"CLIENT", "CACHING", "NO"});
  Run({"GET", "FOO"});
  Run({"SET", "FOO", "BAR"});
  EXPECT_EQ(InvalidationMessagesLen("IO0"), 2);
}

TEST_F(ServerFamilyTest, ClientTrackingMultiOptout) {
  Run({"HELLO", "3"});
  // Check stickiness
  Run({"CLIENT", "TRACKING", "ON", "OPTOUT"});

  Run({"MULTI"});
  Run({"GET", "FOO"});
  Run({"SET", "TMP", "10"});
  Run({"GET", "FOOBAR"});
  Run({"EXEC"});

  Run({"SET", "FOO", "10"});
  Run({"SET", "FOOBAR", "10"});
  EXPECT_EQ(InvalidationMessagesLen("IO0"), 2);

  // CACHING enclosed in MULTI
  Run({"MULTI"});
  Run({"CLIENT", "CACHING", "NO"});
  Run({"GET", "TMP"});
  Run({"GET", "TMP_TMP"});
  Run({"SET", "TMP", "10"});
  Run({"SET", "TMP_TMP", "10"});
  Run({"EXEC"});

  EXPECT_EQ(InvalidationMessagesLen("IO0"), 2);
}

TEST_F(ServerFamilyTest, ClientTrackingUpdateKey) {
  Run({"HELLO", "3"});
  Run({"CLIENT", "TRACKING", "ON"});

  Run({"GET", "FOO"});
  Run({"SET", "FOO", "10"});
  const auto& msg = GetInvalidationMessage("IO0", 0);
  EXPECT_EQ(msg.key, "FOO");

  // make sure invalidation message only gets sent once.
  Run({"GET", "FOO"});
  EXPECT_EQ(InvalidationMessagesLen("IO0"), 1);

  // update string from another connection
  // need to do another read to re-initialize the tracking of the key.
  Run({"GET", "FOO"});
  pp_->at(1)->Await([&] { return Run({"SET", "FOO", "30"}); });
  pp_->AwaitFiberOnAll([](ProactorBase* pb) {});
  const auto& msg2 = GetInvalidationMessage("IO0", 1);
  EXPECT_EQ(msg2.key, "FOO");

  // case 4. test multi command
  Run({"MGET", "X1", "X2", "X3", "X4", "Y1", "Y2", "Y3", "Y4", "Z1", "Z2", "Z3", "Z4"});
  pp_->at(1)->Await([&] { return Run({"MSET", "X1", "1", "Y3", "2", "Z2", "3", "Z4", "5"}); });
  pp_->AwaitFiberOnAll([](ProactorBase* pb) {});
  EXPECT_EQ(InvalidationMessagesLen("IO0"), 6);
  std::vector<std::string_view> keys_invalidated;
  for (unsigned int i = 2; i < 6; ++i)
    keys_invalidated.push_back(GetInvalidationMessage("IO0", i).key);
  ASSERT_THAT(keys_invalidated, ElementsAre("X1", "Y3", "Z2", "Z4"));

  // The following doesn't work correctly as we currently can't mock listener.
  // flushdb command
  // Run({"FLUSHDB"});
}

TEST_F(ServerFamilyTest, ClientTrackingDeleteKey) {
  Run({"HELLO", "3"});
  Run({"CLIENT", "TRACKING", "ON"});
  Run({"SET", "FOO", "10"});
  Run({"GET", "FOO"});
  pp_->at(1)->Await([&] { return Run({"DEL", "FOO"}); });
  pp_->AwaitFiberOnAll([](ProactorBase* pb) {});
  EXPECT_EQ(GetInvalidationMessage("IO0", 0).key, "FOO");
}

TEST_F(ServerFamilyTest, ClientTrackingRenameKey) {
  Run({"HELLO", "3"});
  Run({"CLIENT", "TRACKING", "ON"});
  Run({"SET", "FOO", "10"});
  Run({"GET", "FOO"});
  pp_->at(1)->Await([&] { return Run({"RENAME", "FOO", "BAR"}); });
  pp_->AwaitFiberOnAll([](ProactorBase* pb) {});
  EXPECT_EQ(GetInvalidationMessage("IO0", 0).key, "FOO");
}

TEST_F(ServerFamilyTest, ClientTrackingExpireKey) {
  Run({"HELLO", "3"});
  Run({"CLIENT", "TRACKING", "ON"});
  Run({"SET", "C", "10"});
  Run({"GET", "C"});
  Run({"EXPIRE", "C", "1"});
  AdvanceTime(1000);
  auto resp = Run({"GET", "C"});
  EXPECT_THAT(resp, ArgType(RespExpr::NIL));
  EXPECT_EQ(InvalidationMessagesLen("IO0"), 1);
  EXPECT_EQ(GetInvalidationMessage("IO0", 0).key, "C");
}

TEST_F(ServerFamilyTest, ClientTrackingSelectDB) {
  Run({"HELLO", "3"});
  Run({"CLIENT", "TRACKING", "ON"});
  Run({"SET", "C", "10"});
  Run({"GET", "C"});
  pp_->at(1)->Await([&] { return Run({"SELECT", "2"}); });
  pp_->at(1)->Await([&] { return Run({"SET", "C", "1000"}); });
  pp_->AwaitFiberOnAll([](ProactorBase* pb) {});
  EXPECT_EQ(InvalidationMessagesLen("IO0"), 1);
  EXPECT_EQ(GetInvalidationMessage("IO0", 0).key, "C");
}

TEST_F(ServerFamilyTest, ClientTrackingNonTransactionalBug) {
  Run({"HELLO", "3"});
  Run({"CLIENT", "TRACKING", "ON"});

  Run({"CLUSTER", "SLOTS"});
}

TEST_F(ServerFamilyTest, ClientTrackingLuaBug) {
  Run({"HELLO", "3"});
  // Check stickiness
  Run({"CLIENT", "TRACKING", "ON"});
  using namespace std::string_literals;
  std::string eval = R"(redis.call('get', 'foo'); redis.call('set', 'foo', 'bar'); )";
  Run({"EVAL", absl::StrCat(eval, "return 1"), "1", "foo"});
  Run({"PING"});

  EXPECT_EQ(InvalidationMessagesLen("IO0"), 1);
  absl::StrAppend(&eval, R"(redis.call('get', 'oof'); redis.call('set', 'oof', 'bar'); return 1)");
  Run({"EVAL", eval, "2", "foo", "oof"});
  Run({"PING"});
  EXPECT_EQ(InvalidationMessagesLen("IO0"), 3);
}

TEST_F(ServerFamilyTest, ConfigNormalization) {
  // TODO: Ideally we'd also test that INFO REPLICATION returns the value set in the config, but
  // there is no way currently to setup a mock replica in unit tests.

  absl::FlagSaver fs;  // Restores the flag to default value after test finishes

  // Default value
  EXPECT_THAT(Run({"config", "get", "replica-priority"}),
              RespArray(ElementsAre("replica_priority", "100")));
  EXPECT_THAT(Run({"config", "get", "replica_priority"}),
              RespArray(ElementsAre("replica_priority", "100")));

  // Set with dash
  EXPECT_THAT(Run({"config", "set", "replica-priority", "7"}), "OK");

  EXPECT_THAT(Run({"config", "get", "replica-priority"}),
              RespArray(ElementsAre("replica_priority", "7")));
  EXPECT_THAT(Run({"config", "get", "replica_priority"}),
              RespArray(ElementsAre("replica_priority", "7")));

  // Set with underscore
  EXPECT_THAT(Run({"config", "set", "replica_priority", "13"}), "OK");

  EXPECT_THAT(Run({"config", "get", "replica-priority"}),
              RespArray(ElementsAre("replica_priority", "13")));
  EXPECT_THAT(Run({"config", "get", "replica_priority"}),
              RespArray(ElementsAre("replica_priority", "13")));
}

TEST_F(ServerFamilyTest, CommandDocsOk) {
  EXPECT_THAT(Run({"command", "docs"}), "OK");
}

}  // namespace dfly
