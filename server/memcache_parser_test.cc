// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/memcache_parser.h"

#include <gmock/gmock.h>

#include "absl/strings/str_cat.h"
#include "base/gtest.h"
#include "base/logging.h"
#include "server/test_utils.h"

using namespace testing;
using namespace std;
namespace dfly {

class MCParserTest : public testing::Test {
 protected:
  RedisParser::Result Parse(std::string_view str);

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

}  // namespace dfly