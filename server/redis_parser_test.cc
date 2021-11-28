// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/redis_parser.h"

#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>

#include "absl/strings/str_cat.h"
#include "base/gtest.h"
#include "base/logging.h"
#include "server/test_utils.h"

using namespace testing;
using namespace std;
namespace dfly {

MATCHER_P(ArrArg, expected, absl::StrCat(negation ? "is not" : "is", " equal to:\n", expected)) {
  if (arg.type != RespExpr::ARRAY) {
    *result_listener << "\nWrong type: " << arg.type;
    return false;
  }
  size_t exp_sz = expected;
  size_t actual = get<RespVec*>(arg.u)->size();

  if (exp_sz != actual) {
    *result_listener << "\nActual size: " << actual;
    return false;
  }
  return true;
}

class RedisParserTest : public testing::Test {
 protected:
  RedisParser::Result Parse(std::string_view str);

  RedisParser parser_;
  RespExpr::Vec args_;
  uint32_t consumed_;

  unique_ptr<uint8_t[]> stash_;
};

RedisParser::Result RedisParserTest::Parse(std::string_view str) {
  stash_.reset(new uint8_t[str.size()]);
  auto* ptr = stash_.get();
  memcpy(ptr, str.data(), str.size());
  return parser_.Parse(RedisParser::Buffer{ptr, str.size()}, &consumed_, &args_);
}

TEST_F(RedisParserTest, Inline) {
  RespExpr e{RespExpr::STRING};
  ASSERT_EQ(RespExpr::STRING, e.type);

  const char kCmd1[] = "KEY   VAL\r\n";

  ASSERT_EQ(RedisParser::OK, Parse(kCmd1));
  EXPECT_EQ(strlen(kCmd1), consumed_);
  EXPECT_THAT(args_, ElementsAre(StrArg("KEY"), StrArg("VAL")));

  ASSERT_EQ(RedisParser::INPUT_PENDING, Parse("KEY"));
  EXPECT_EQ(3, consumed_);
  ASSERT_EQ(RedisParser::INPUT_PENDING, Parse(" FOO "));
  EXPECT_EQ(5, consumed_);
  ASSERT_EQ(RedisParser::INPUT_PENDING, Parse(" BAR"));
  EXPECT_EQ(4, consumed_);
  ASSERT_EQ(RedisParser::OK, Parse(" \r\n "));
  EXPECT_EQ(3, consumed_);
  EXPECT_THAT(args_, ElementsAre(StrArg("KEY"), StrArg("FOO"), StrArg("BAR")));

  ASSERT_EQ(RedisParser::INPUT_PENDING, Parse(" 1 2"));
  EXPECT_EQ(4, consumed_);
  ASSERT_EQ(RedisParser::INPUT_PENDING, Parse(" 45"));
  EXPECT_EQ(3, consumed_);
  ASSERT_EQ(RedisParser::OK, Parse("\r\n"));
  EXPECT_EQ(2, consumed_);
  EXPECT_THAT(args_, ElementsAre(StrArg("1"), StrArg("2"), StrArg("45")));

  // Empty queries return RESP_OK.
  EXPECT_EQ(RedisParser::OK, Parse("\r\n"));
  EXPECT_EQ(2, consumed_);
}

TEST_F(RedisParserTest, InlineEscaping) {
  LOG(ERROR)  << "TBD: to be compliant with sdssplitargs";  // TODO:
}

TEST_F(RedisParserTest, Multi1) {
  ASSERT_EQ(RedisParser::INPUT_PENDING, Parse("*1\r\n$"));
  EXPECT_EQ(4, consumed_);

  ASSERT_EQ(RedisParser::INPUT_PENDING, Parse("$4\r\nMSET"));
  EXPECT_EQ(4, consumed_);

  ASSERT_EQ(RedisParser::OK, Parse("MSET\r\n*2\r\n"));
  EXPECT_EQ(6, consumed_);

  ASSERT_EQ(RedisParser::INPUT_PENDING, Parse("*2\r\n$3\r\nKEY\r\n$3\r\nVAL"));
  EXPECT_EQ(17, consumed_);

  ASSERT_EQ(RedisParser::OK, Parse("VAL\r\n"));
  EXPECT_EQ(5, consumed_);
  EXPECT_THAT(args_, ElementsAre("KEY", "VAL"));
}

TEST_F(RedisParserTest, Multi2) {
  const char kFirst[] = "*3\r\n$3\r\nSET\r\n$16\r\nkey:";
  const char kSecond[] = "key:000002273458\r\n$3\r\nVXK";
  ASSERT_EQ(RedisParser::INPUT_PENDING, Parse(kFirst));
  ASSERT_EQ(strlen(kFirst) - 4, consumed_);
  ASSERT_EQ(RedisParser::INPUT_PENDING, Parse(kSecond));
  ASSERT_EQ(strlen(kSecond) - 3, consumed_);
  ASSERT_EQ(RedisParser::OK, Parse("VXK\r\n*3\r\n$3\r\nSET"));
  EXPECT_THAT(args_, ElementsAre("SET", "key:000002273458", "VXK"));
}

TEST_F(RedisParserTest, InvalidMult1) {
  ASSERT_EQ(RedisParser::BAD_BULKLEN, Parse("*2\r\n$3\r\nFOO\r\nBAR\r\n"));
}

TEST_F(RedisParserTest, Empty) {
  ASSERT_EQ(RedisParser::OK, Parse("*2\r\n$0\r\n\r\n$0\r\n\r\n"));
}

}  // namespace dfly
