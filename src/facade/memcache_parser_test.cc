// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "facade/memcache_parser.h"

#include <gmock/gmock.h>

#include "absl/strings/str_cat.h"
#include "base/gtest.h"
#include "base/logging.h"
#include "facade/facade_test.h"

using namespace testing;
using namespace std;

namespace facade {

class MCParserTest : public testing::Test {
 protected:
  MCParserTest() {
    cmd_.backed_args = &backed_args_;
  }
  MemcacheParser::Result Parse(string_view input) {
    parser_.Reset();
    return parser_.Parse(input, &consumed_, &cmd_);
  }

  vector<string_view> ToArgs() const {
    return {cmd_.backed_args->begin(), cmd_.backed_args->end()};
  }

  MemcacheParser parser_;
  cmn::BackedArguments backed_args_;
  MemcacheParser::Command cmd_;
  uint32_t consumed_;
};

TEST_F(MCParserTest, Basic) {
  MemcacheParser::Result st = Parse("set a 1 20 3\r\n");
  EXPECT_EQ(MemcacheParser::INPUT_PENDING, st);
  EXPECT_EQ("a", cmd_.key());
  EXPECT_EQ(1, cmd_.flags);
  EXPECT_EQ(20, cmd_.expire_ts);
  EXPECT_EQ(3, cmd_.value().size());
  EXPECT_EQ(MemcacheParser::SET, cmd_.type);

  st = Parse("quit\r\n");
  EXPECT_EQ(MemcacheParser::OK, st);
  EXPECT_EQ(MemcacheParser::QUIT, cmd_.type);
}

TEST_F(MCParserTest, Incr) {
  MemcacheParser::Result st = Parse("incr a\r\n");
  EXPECT_EQ(MemcacheParser::PARSE_ERROR, st);

  st = Parse("incr a 1\r\n");
  EXPECT_EQ(MemcacheParser::OK, st);
  EXPECT_EQ(MemcacheParser::INCR, cmd_.type);
  EXPECT_EQ("a", cmd_.key());
  EXPECT_EQ(1, cmd_.delta);
  EXPECT_FALSE(cmd_.no_reply);

  st = Parse("incr a -1\r\n");
  EXPECT_EQ(MemcacheParser::BAD_DELTA, st);

  st = Parse("decr b 10 noreply\r\n");
  EXPECT_EQ(MemcacheParser::OK, st);
  EXPECT_EQ(MemcacheParser::DECR, cmd_.type);
  EXPECT_EQ(10, cmd_.delta);
}

TEST_F(MCParserTest, Stats) {
  MemcacheParser::Result st = Parse("stats foo\r\n");
  EXPECT_EQ(MemcacheParser::OK, st);
  EXPECT_EQ(consumed_, 11);
  EXPECT_EQ(cmd_.type, MemcacheParser::STATS);
  EXPECT_EQ("foo", cmd_.key());

  st = Parse("stats  \r\n");
  EXPECT_EQ(MemcacheParser::OK, st);
  EXPECT_EQ(consumed_, 9);
  EXPECT_EQ(cmd_.type, MemcacheParser::STATS);
  EXPECT_EQ(0, cmd_.size());

  st = Parse("stats  fpp bar\r\n");
  EXPECT_EQ(MemcacheParser::PARSE_ERROR, st);
}

TEST_F(MCParserTest, NoreplyBasic) {
  MemcacheParser::Result st = Parse("set mykey 1 2 3 noreply\r\n");

  EXPECT_EQ(MemcacheParser::INPUT_PENDING, st);
  EXPECT_EQ("mykey", cmd_.key());
  EXPECT_EQ(1, cmd_.flags);
  EXPECT_EQ(2, cmd_.expire_ts);
  EXPECT_EQ(3, cmd_.value().size());
  EXPECT_EQ(MemcacheParser::SET, cmd_.type);
  EXPECT_TRUE(cmd_.no_reply);

  st = Parse("set mykey2 4 5 6\r\n");

  EXPECT_EQ(MemcacheParser::INPUT_PENDING, st);
  EXPECT_EQ("mykey2", cmd_.key());
  EXPECT_EQ(4, cmd_.flags);
  EXPECT_EQ(5, cmd_.expire_ts);
  EXPECT_EQ(6, cmd_.value().size());
  EXPECT_EQ(MemcacheParser::SET, cmd_.type);
  EXPECT_FALSE(cmd_.no_reply);
}

TEST_F(MCParserTest, Meta) {
  MemcacheParser::Result st = Parse("ms key1 ");
  EXPECT_EQ(MemcacheParser::INPUT_PENDING, st);
  EXPECT_EQ(8, consumed_);
  st = parser_.Parse("6 T1 F2\r\naaaaaa\r\n", &consumed_, &cmd_);
  EXPECT_EQ(MemcacheParser::OK, st);
  EXPECT_EQ(17, consumed_);
  EXPECT_EQ(MemcacheParser::SET, cmd_.type);
  EXPECT_EQ("key1", cmd_.key());
  EXPECT_EQ(2, cmd_.flags);
  EXPECT_EQ(1, cmd_.expire_ts);
  st = Parse("ms 16nXnNeV150= 5 b ME\r\nbbbbb");
  EXPECT_EQ(MemcacheParser::INPUT_PENDING, st);
  EXPECT_EQ(29, consumed_);
  EXPECT_EQ(MemcacheParser::ADD, cmd_.type);
  EXPECT_EQ("שלום", cmd_.key());
  EXPECT_EQ(5, cmd_.value().size());

  st = Parse("mg 16nXnNeV150= b\r\n");
  EXPECT_EQ(MemcacheParser::OK, st);
  EXPECT_EQ(19, consumed_);
  EXPECT_EQ(MemcacheParser::GET, cmd_.type);
  EXPECT_EQ("שלום", cmd_.key());

  st = Parse("ma val b\r\n");
  EXPECT_EQ(MemcacheParser::OK, st);
  EXPECT_EQ(10, consumed_);
  EXPECT_EQ(MemcacheParser::INCR, cmd_.type);

  st = Parse("ma val M- D10\r\n");
  EXPECT_EQ(MemcacheParser::OK, st);
  EXPECT_EQ(15, consumed_);
  EXPECT_EQ(MemcacheParser::DECR, cmd_.type);
  EXPECT_EQ(10, cmd_.delta);

  st = Parse("mg key f v t l h\r\n");
  EXPECT_EQ(MemcacheParser::OK, st);
  EXPECT_EQ(18, consumed_);
  EXPECT_EQ(MemcacheParser::GET, cmd_.type);
  EXPECT_EQ("key", cmd_.key());
  EXPECT_TRUE(cmd_.return_flags);
  EXPECT_TRUE(cmd_.return_value);
  EXPECT_TRUE(cmd_.return_ttl);
  EXPECT_TRUE(cmd_.return_access_time);
  EXPECT_TRUE(cmd_.return_hit);
}

TEST_F(MCParserTest, Gat) {
  auto res = Parse("gat 1000 foo bar baz\r\n");
  EXPECT_EQ(MemcacheParser::OK, res);
  EXPECT_EQ(consumed_, 22);
  EXPECT_EQ(cmd_.type, MemcacheParser::GAT);
  EXPECT_THAT(ToArgs(), ElementsAre("foo", "bar", "baz"));
  EXPECT_EQ(cmd_.expire_ts, 1000);

  res = Parse("gat foo bar\r\n");
  EXPECT_EQ(MemcacheParser::BAD_INT, res);

  res = Parse("gats 1000 foo bar baz\r\n");
  EXPECT_EQ(MemcacheParser::OK, res);
  EXPECT_EQ(consumed_, 23);
  EXPECT_EQ(cmd_.type, MemcacheParser::GATS);
  EXPECT_THAT(ToArgs(), ElementsAre("foo", "bar", "baz"));
  EXPECT_EQ(cmd_.expire_ts, 1000);
}

TEST_F(MCParserTest, ValueState) {
  auto st = Parse("ms key1 6\r\nabc");
  EXPECT_EQ(MemcacheParser::INPUT_PENDING, st);
  EXPECT_EQ(consumed_, 14);
  st = parser_.Parse("de", &consumed_, &cmd_);
  EXPECT_EQ(MemcacheParser::INPUT_PENDING, st);
  EXPECT_EQ(consumed_, 2);

  st = parser_.Parse("f\r", &consumed_, &cmd_);
  EXPECT_EQ(MemcacheParser::INPUT_PENDING, st);
  EXPECT_EQ(consumed_, 2);
  EXPECT_EQ(cmd_.value(), "abcdef");

  st = parser_.Parse("\n", &consumed_, &cmd_);
  EXPECT_EQ(MemcacheParser::OK, st);
  EXPECT_EQ(consumed_, 1);
}

TEST_F(MCParserTest, ParseError) {
  EXPECT_EQ(MemcacheParser::PARSE_ERROR, Parse("ms key1 3\r\nabcd"));
  EXPECT_EQ(MemcacheParser::INPUT_PENDING, Parse("ms key1 3\r\nabc"));
  EXPECT_EQ(MemcacheParser::PARSE_ERROR, parser_.Parse("\ra", &consumed_, &cmd_));
  EXPECT_EQ(MemcacheParser::INPUT_PENDING, Parse("ms key1 3\r\nabc\r"));
  EXPECT_EQ(MemcacheParser::PARSE_ERROR, parser_.Parse("\r", &consumed_, &cmd_));
}

class MCParserNoreplyTest : public MCParserTest {
 protected:
  void RunTest(string_view str, bool noreply,
               MemcacheParser::Result expected_res = MemcacheParser::OK) {
    MemcacheParser::Result st = Parse(str);

    EXPECT_EQ(expected_res, st);
    EXPECT_EQ(cmd_.no_reply, noreply);
  }
};

TEST_F(MCParserNoreplyTest, StoreCommands) {
  RunTest("set mykey 0 0 3 noreply\r\n", true, MemcacheParser::INPUT_PENDING);
  RunTest("set mykey 0 0 3\r\n", false, MemcacheParser::INPUT_PENDING);
  RunTest("add mykey 0 0 3\r\n", false, MemcacheParser::INPUT_PENDING);
  RunTest("replace mykey 0 0 3\r\n", false, MemcacheParser::INPUT_PENDING);
  RunTest("append mykey 0 0 3\r\n", false, MemcacheParser::INPUT_PENDING);
  RunTest("prepend mykey 0 0 3\r\n", false, MemcacheParser::INPUT_PENDING);
}

TEST_F(MCParserNoreplyTest, Other) {
  RunTest("quit\r\n", false);
  RunTest("delete mykey\r\n", false);
  RunTest("incr mykey 1\r\n", false);
  RunTest("decr mykey 1\r\n", false);
  RunTest("flush_all\r\n", false);
}

TEST_F(MCParserNoreplyTest, LargeGetRequest) {
  std::string large_request = "get";
  for (size_t i = 0; i < 100; ++i) {
    absl::StrAppend(&large_request, " mykey", i, " ");
  }
  absl::StrAppend(&large_request, "\r\n");

  RunTest(large_request, false);

  EXPECT_EQ(cmd_.type, MemcacheParser::CmdType::GET);
  auto keys = ToArgs();
  EXPECT_TRUE(std::all_of(keys.begin(), keys.end(), [i = 0u](const auto& elem) mutable {
    return elem == absl::StrCat("mykey", i++);
  }));
}

}  // namespace facade
