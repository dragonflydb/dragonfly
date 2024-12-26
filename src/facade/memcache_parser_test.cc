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
  MemcacheParser parser_;
  MemcacheParser::Command cmd_;
  uint32_t consumed_;

  unique_ptr<uint8_t[]> stash_;
};

TEST_F(MCParserTest, Basic) {
  MemcacheParser::Result st = parser_.Parse("set a 1 20 3\r\n", &consumed_, &cmd_);
  EXPECT_EQ(MemcacheParser::OK, st);
  EXPECT_EQ("a", cmd_.key);
  EXPECT_EQ(1, cmd_.flags);
  EXPECT_EQ(20, cmd_.expire_ts);
  EXPECT_EQ(3, cmd_.bytes_len);
  EXPECT_EQ(MemcacheParser::SET, cmd_.type);

  st = parser_.Parse("quit\r\n", &consumed_, &cmd_);
  EXPECT_EQ(MemcacheParser::OK, st);
  EXPECT_EQ(MemcacheParser::QUIT, cmd_.type);
}

TEST_F(MCParserTest, Incr) {
  MemcacheParser::Result st = parser_.Parse("incr a\r\n", &consumed_, &cmd_);
  EXPECT_EQ(MemcacheParser::PARSE_ERROR, st);

  st = parser_.Parse("incr a 1\r\n", &consumed_, &cmd_);
  EXPECT_EQ(MemcacheParser::OK, st);
  EXPECT_EQ(MemcacheParser::INCR, cmd_.type);
  EXPECT_EQ("a", cmd_.key);
  EXPECT_EQ(1, cmd_.delta);
  EXPECT_FALSE(cmd_.no_reply);

  st = parser_.Parse("incr a -1\r\n", &consumed_, &cmd_);
  EXPECT_EQ(MemcacheParser::BAD_DELTA, st);

  st = parser_.Parse("decr b 10 noreply\r\n", &consumed_, &cmd_);
  EXPECT_EQ(MemcacheParser::OK, st);
  EXPECT_EQ(MemcacheParser::DECR, cmd_.type);
  EXPECT_EQ(10, cmd_.delta);
}

TEST_F(MCParserTest, Stats) {
  MemcacheParser::Result st = parser_.Parse("stats foo\r\n", &consumed_, &cmd_);
  EXPECT_EQ(MemcacheParser::OK, st);
  EXPECT_EQ(consumed_, 11);
  EXPECT_EQ(cmd_.type, MemcacheParser::STATS);
  EXPECT_EQ("foo", cmd_.key);

  cmd_ = MemcacheParser::Command{};
  st = parser_.Parse("stats  \r\n", &consumed_, &cmd_);
  EXPECT_EQ(MemcacheParser::OK, st);
  EXPECT_EQ(consumed_, 9);
  EXPECT_EQ(cmd_.type, MemcacheParser::STATS);
  EXPECT_EQ("", cmd_.key);

  cmd_ = MemcacheParser::Command{};
  st = parser_.Parse("stats  fpp bar\r\n", &consumed_, &cmd_);
  EXPECT_EQ(MemcacheParser::PARSE_ERROR, st);
}

TEST_F(MCParserTest, NoreplyBasic) {
  MemcacheParser::Result st = parser_.Parse("set mykey 1 2 3 noreply\r\n", &consumed_, &cmd_);

  EXPECT_EQ(MemcacheParser::OK, st);
  EXPECT_EQ("mykey", cmd_.key);
  EXPECT_EQ(1, cmd_.flags);
  EXPECT_EQ(2, cmd_.expire_ts);
  EXPECT_EQ(3, cmd_.bytes_len);
  EXPECT_EQ(MemcacheParser::SET, cmd_.type);
  EXPECT_TRUE(cmd_.no_reply);

  cmd_ = MemcacheParser::Command{};
  st = parser_.Parse("set mykey2 4 5 6\r\n", &consumed_, &cmd_);

  EXPECT_EQ(MemcacheParser::OK, st);
  EXPECT_EQ("mykey2", cmd_.key);
  EXPECT_EQ(4, cmd_.flags);
  EXPECT_EQ(5, cmd_.expire_ts);
  EXPECT_EQ(6, cmd_.bytes_len);
  EXPECT_EQ(MemcacheParser::SET, cmd_.type);
  EXPECT_FALSE(cmd_.no_reply);
}

TEST_F(MCParserTest, Meta) {
  MemcacheParser::Result st = parser_.Parse("ms key1 ", &consumed_, &cmd_);
  EXPECT_EQ(MemcacheParser::INPUT_PENDING, st);
  EXPECT_EQ(0, consumed_);
  st = parser_.Parse("ms key1 6 T1 F2\r\n", &consumed_, &cmd_);
  EXPECT_EQ(MemcacheParser::OK, st);
  EXPECT_EQ(17, consumed_);
  EXPECT_EQ(MemcacheParser::SET, cmd_.type);
  EXPECT_EQ("key1", cmd_.key);
  EXPECT_EQ(2, cmd_.flags);
  EXPECT_EQ(1, cmd_.expire_ts);

  st = parser_.Parse("ms 16nXnNeV150= 5 b ME\r\n", &consumed_, &cmd_);
  EXPECT_EQ(MemcacheParser::OK, st);
  EXPECT_EQ(24, consumed_);
  EXPECT_EQ(MemcacheParser::ADD, cmd_.type);
  EXPECT_EQ("שלום", cmd_.key);
  EXPECT_EQ(5, cmd_.bytes_len);

  st = parser_.Parse("mg 16nXnNeV150= b\r\n", &consumed_, &cmd_);
  EXPECT_EQ(MemcacheParser::OK, st);
  EXPECT_EQ(19, consumed_);
  EXPECT_EQ(MemcacheParser::GET, cmd_.type);
  EXPECT_EQ("שלום", cmd_.key);

  st = parser_.Parse("ma val b\r\n", &consumed_, &cmd_);
  EXPECT_EQ(MemcacheParser::OK, st);
  EXPECT_EQ(10, consumed_);
  EXPECT_EQ(MemcacheParser::INCR, cmd_.type);

  st = parser_.Parse("ma val M- D10\r\n", &consumed_, &cmd_);
  EXPECT_EQ(MemcacheParser::OK, st);
  EXPECT_EQ(15, consumed_);
  EXPECT_EQ(MemcacheParser::DECR, cmd_.type);
  EXPECT_EQ(10, cmd_.delta);

  st = parser_.Parse("mg key f v t l h\r\n", &consumed_, &cmd_);
  EXPECT_EQ(MemcacheParser::OK, st);
  EXPECT_EQ(18, consumed_);
  EXPECT_EQ(MemcacheParser::GET, cmd_.type);
  EXPECT_EQ("key", cmd_.key);
  EXPECT_TRUE(cmd_.return_flags);
  EXPECT_TRUE(cmd_.return_value);
  EXPECT_TRUE(cmd_.return_ttl);
  EXPECT_TRUE(cmd_.return_access_time);
  EXPECT_TRUE(cmd_.return_hit);
}

class MCParserNoreplyTest : public MCParserTest {
 protected:
  void RunTest(string_view str, bool noreply) {
    MemcacheParser::Result st = parser_.Parse(str, &consumed_, &cmd_);

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
  EXPECT_EQ(cmd_.key, "mykey0");
  const auto& keys = cmd_.keys_ext;
  EXPECT_TRUE(std::all_of(keys.begin(), keys.end(), [](const auto& elem) {
    static size_t i = 1;
    return elem == absl::StrCat("mykey", i++);
  }));
}

}  // namespace facade
