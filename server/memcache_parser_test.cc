// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
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

}  // namespace dfly