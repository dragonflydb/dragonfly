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

}  // namespace facade
