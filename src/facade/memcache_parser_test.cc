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
  MemcacheParser::Result Parse(string_view input) {
    return parser_.Parse(input, &consumed_, &cmd_);
  }

  vector<string_view> ToArgs() {
    vector<string_view> res;
    ParsedArgs{cmd_}.ToSlice(&res);
    return res;
  }

  MemcacheParser parser_;
  MemcacheParser::Command cmd_;
  uint32_t consumed_;
};

TEST_F(MCParserTest, Basic) {
  MemcacheParser::Result st = Parse("set a 1 20 3\r\n");
  EXPECT_EQ(MemcacheParser::OK, st);
  EXPECT_EQ("a", cmd_.key());
  EXPECT_EQ(1, cmd_.flags);
  EXPECT_EQ(20, cmd_.expire_ts);
  EXPECT_EQ(3, cmd_.bytes_len);
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

  EXPECT_EQ(MemcacheParser::OK, st);
  EXPECT_EQ("mykey", cmd_.key());
  EXPECT_EQ(1, cmd_.flags);
  EXPECT_EQ(2, cmd_.expire_ts);
  EXPECT_EQ(3, cmd_.bytes_len);
  EXPECT_EQ(MemcacheParser::SET, cmd_.type);
  EXPECT_TRUE(cmd_.no_reply);

  st = Parse("set mykey2 4 5 6\r\n");

  EXPECT_EQ(MemcacheParser::OK, st);
  EXPECT_EQ("mykey2", cmd_.key());
  EXPECT_EQ(4, cmd_.flags);
  EXPECT_EQ(5, cmd_.expire_ts);
  EXPECT_EQ(6, cmd_.bytes_len);
  EXPECT_EQ(MemcacheParser::SET, cmd_.type);
  EXPECT_FALSE(cmd_.no_reply);
}

TEST_F(MCParserTest, Meta) {
  MemcacheParser::Result st = Parse("ms key1 ");
  EXPECT_EQ(MemcacheParser::INPUT_PENDING, st);
  EXPECT_EQ(0, consumed_);
  st = Parse("ms key1 6 T1 F2\r\n");
  EXPECT_EQ(MemcacheParser::OK, st);
  EXPECT_EQ(17, consumed_);
  EXPECT_EQ(MemcacheParser::SET, cmd_.type);
  EXPECT_EQ("key1", cmd_.key());
  EXPECT_EQ(2, cmd_.flags);
  EXPECT_EQ(1, cmd_.expire_ts);
  st = Parse("ms 16nXnNeV150= 5 b ME\r\n");
  EXPECT_EQ(MemcacheParser::OK, st);
  EXPECT_EQ(24, consumed_);
  EXPECT_EQ(MemcacheParser::ADD, cmd_.type);
  EXPECT_EQ("שלום", cmd_.key());
  EXPECT_EQ(5, cmd_.bytes_len);

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

class MCParserNoreplyTest : public MCParserTest {
 protected:
  void RunTest(string_view str, bool noreply) {
    MemcacheParser::Result st = Parse(str);

    EXPECT_EQ(MemcacheParser::OK, st);
    EXPECT_EQ(cmd_.no_reply, noreply);
  }
};

TEST_F(MCParserNoreplyTest, StoreCommands) {
  RunTest("set mykey 0 0 3 noreply\r\n", true);
  RunTest("set mykey 0 0 3\r\n", false);
  RunTest("add mykey 0 0 3\r\n", false);
  RunTest("replace mykey 0 0 3\r\n", false);
  RunTest("append mykey 0 0 3\r\n", false);
  RunTest("prepend mykey 0 0 3\r\n", false);
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
