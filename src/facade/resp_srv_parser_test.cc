// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "facade/resp_srv_parser.h"

#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>

#include "base/gtest.h"
#include "base/logging.h"

using namespace testing;
using namespace std;
namespace facade {

// Custom printer for RespSrvParser::Result to make test output more readable
void PrintTo(const RespSrvParser::Result& result, std::ostream* os) {
  switch (result) {
    case RespSrvParser::OK:
      *os << "OK";
      break;
    case RespSrvParser::INPUT_PENDING:
      *os << "INPUT_PENDING";
      break;
    case RespSrvParser::BAD_ARRAYLEN:
      *os << "BAD_ARRAYLEN";
      break;
    case RespSrvParser::BAD_BULKLEN:
      *os << "BAD_BULKLEN";
      break;
    case RespSrvParser::BAD_STRING:
      *os << "BAD_STRING";
      break;
    default:
      *os << "UNKNOWN(" << static_cast<int>(result) << ")";
      break;
  }
}

class RespSrvParserTest : public testing::Test {
 protected:
  RespSrvParser::Result Parse(std::string_view str);

  RespSrvParser parser_;
  cmn::BackedArguments args_;
  uint32_t consumed_;
};

RespSrvParser::Result RespSrvParserTest::Parse(std::string_view str) {
  RespSrvParser::Buffer buf{reinterpret_cast<const uint8_t*>(str.data()), str.size()};
  return parser_.Parse(buf, &consumed_, &args_);
}

TEST_F(RespSrvParserTest, Inline) {
  const char kCmd1[] = "KEY   VAL\r\n";

  ASSERT_EQ(RespSrvParser::OK, Parse(kCmd1));
  EXPECT_EQ(strlen(kCmd1), consumed_);
  EXPECT_THAT(args_, ElementsAre("KEY", "VAL"));

  ASSERT_EQ(RespSrvParser::INPUT_PENDING, Parse("KEY"));
  EXPECT_EQ(3, consumed_);
  ASSERT_EQ(RespSrvParser::INPUT_PENDING, Parse(" FOO "));
  EXPECT_EQ(5, consumed_);
  ASSERT_EQ(RespSrvParser::INPUT_PENDING, Parse(" BAR"));
  EXPECT_EQ(4, consumed_);
  ASSERT_EQ(RespSrvParser::OK, Parse(" \r\n "));
  EXPECT_EQ(3, consumed_);
  EXPECT_THAT(args_, ElementsAre("KEY", "FOO", "BAR"));

  ASSERT_EQ(RespSrvParser::INPUT_PENDING, Parse(" 1 2"));
  EXPECT_EQ(4, consumed_);
  ASSERT_EQ(RespSrvParser::INPUT_PENDING, Parse(" 45"));
  EXPECT_EQ(3, consumed_);
  ASSERT_EQ(RespSrvParser::OK, Parse("\r\n"));
  EXPECT_EQ(2, consumed_);
  EXPECT_THAT(args_, ElementsAre("1", "2", "45"));

  // Empty queries return INPUT_PENDING.
  EXPECT_EQ(RespSrvParser::INPUT_PENDING, Parse("\r\n"));
  EXPECT_EQ(2, consumed_);

  ASSERT_EQ(RespSrvParser::OK, Parse("_\r\n"));
  EXPECT_THAT(args_, ElementsAre("_"));
}

TEST_F(RespSrvParserTest, Multi1) {
  ASSERT_EQ(RespSrvParser::INPUT_PENDING, Parse("*1\r\n"));
  EXPECT_EQ(4, consumed_);
  EXPECT_EQ(0, parser_.parselen_hint());

  ASSERT_EQ(RespSrvParser::INPUT_PENDING, Parse("$4\r\n"));
  EXPECT_EQ(4, consumed_);
  EXPECT_EQ(4, parser_.parselen_hint());

  ASSERT_EQ(RespSrvParser::OK, Parse("PING\r\n"));
  EXPECT_EQ(6, consumed_);
  EXPECT_EQ(0, parser_.parselen_hint());
  EXPECT_THAT(args_, ElementsAre("PING"));
}

TEST_F(RespSrvParserTest, Multi2) {
  ASSERT_EQ(RespSrvParser::INPUT_PENDING, Parse("*1\r\n$"));
  EXPECT_EQ(5, consumed_);

  ASSERT_EQ(RespSrvParser::INPUT_PENDING, Parse("4\r\nMSET"));
  EXPECT_EQ(7, consumed_);

  ASSERT_EQ(RespSrvParser::OK, Parse("\r\n*2\r\n"));
  EXPECT_EQ(2, consumed_);

  ASSERT_EQ(RespSrvParser::INPUT_PENDING, Parse("*2\r\n$3\r\nKEY\r\n$3\r\nVAL"));
  EXPECT_EQ(20, consumed_);

  ASSERT_EQ(RespSrvParser::OK, Parse("\r\n"));
  EXPECT_EQ(2, consumed_);
  EXPECT_THAT(args_, ElementsAre("KEY", "VAL"));
}

TEST_F(RespSrvParserTest, Multi3) {
  const char kFirst[] = "*3\r\n$3\r\nSET\r\n$16\r\nkey:";
  const char kSecond[] = "000002273458\r\n$3\r\nVXK";
  ASSERT_EQ(RespSrvParser::INPUT_PENDING, Parse(kFirst));
  ASSERT_EQ(strlen(kFirst), consumed_);
  ASSERT_EQ(RespSrvParser::INPUT_PENDING, Parse(kSecond));
  ASSERT_EQ(strlen(kSecond), consumed_);
  ASSERT_EQ(RespSrvParser::OK, Parse("\r\n*3\r\n$3\r\nSET"));
  ASSERT_EQ(2, consumed_);
  EXPECT_THAT(args_, ElementsAre("SET", "key:000002273458", "VXK"));
}

TEST_F(RespSrvParserTest, InvalidMult1) {
  ASSERT_EQ(RespSrvParser::BAD_BULKLEN, Parse("*2\r\n$3\r\nFOO\r\nBAR\r\n"));
}

TEST_F(RespSrvParserTest, Empty) {
  ASSERT_EQ(RespSrvParser::OK, Parse("*2\r\n$0\r\n\r\n$0\r\n\r\n"));
}

TEST_F(RespSrvParserTest, LargeBulk) {
  string_view prefix("*1\r\n$1024\r\n");

  ASSERT_EQ(RespSrvParser::INPUT_PENDING, Parse(prefix));
  ASSERT_EQ(prefix.size(), consumed_);
  ASSERT_GE(parser_.parselen_hint(), 1024);

  string half(512, 'a');
  ASSERT_EQ(RespSrvParser::INPUT_PENDING, Parse(half));
  ASSERT_EQ(512, consumed_);
  ASSERT_GE(parser_.parselen_hint(), 512);
  ASSERT_EQ(RespSrvParser::INPUT_PENDING, Parse(half));
  ASSERT_EQ(512, consumed_);
  ASSERT_EQ(RespSrvParser::INPUT_PENDING, Parse("\r"));
  ASSERT_EQ(1, consumed_);
  ASSERT_EQ(RespSrvParser::OK, Parse("\n"));
  EXPECT_EQ(1, consumed_);

  string part1 = absl::StrCat(prefix, half);
  ASSERT_EQ(RespSrvParser::INPUT_PENDING, Parse(part1));
  ASSERT_EQ(RespSrvParser::INPUT_PENDING, Parse(half));
  ASSERT_EQ(RespSrvParser::OK, Parse("\r\n"));

  prefix = "*1\r\n$27000000\r\n";
  ASSERT_EQ(RespSrvParser::INPUT_PENDING, Parse(prefix));
  ASSERT_EQ(prefix.size(), consumed_);
  string chunk(1000000, 'a');
  for (unsigned i = 0; i < 27; ++i) {
    ASSERT_EQ(RespSrvParser::INPUT_PENDING, Parse(chunk));
    ASSERT_EQ(chunk.size(), consumed_);
  }
  ASSERT_EQ(RespSrvParser::OK, Parse("\r\n"));
  ASSERT_EQ(args_.size(), 1);
  EXPECT_EQ(27000000u, args_[0].size());
}

TEST_F(RespSrvParserTest, Eol) {
  ASSERT_EQ(RespSrvParser::INPUT_PENDING, Parse("*1\r"));
  EXPECT_EQ(3, consumed_);
  ASSERT_EQ(RespSrvParser::INPUT_PENDING, Parse("\n$5\r\n"));
  EXPECT_EQ(5, consumed_);
}

TEST_F(RespSrvParserTest, BulkSplit) {
  ASSERT_EQ(RespSrvParser::INPUT_PENDING, Parse("*1\r\n$4\r\nSADD\r"));
  ASSERT_EQ(13, consumed_);
  ASSERT_EQ(RespSrvParser::OK, Parse("\n"));
}

TEST_F(RespSrvParserTest, InlineSplit) {
  ASSERT_EQ(RespSrvParser::INPUT_PENDING, Parse("\n"));
  EXPECT_EQ(1, consumed_);
  ASSERT_EQ(RespSrvParser::OK, Parse("\nPING\n\n"));
  EXPECT_EQ(6, consumed_);
  ASSERT_EQ(RespSrvParser::INPUT_PENDING, Parse("\n"));
  EXPECT_EQ(1, consumed_);
  ASSERT_EQ(RespSrvParser::INPUT_PENDING, Parse("P"));
  ASSERT_EQ(RespSrvParser::OK, Parse("ING\n"));
}

TEST_F(RespSrvParserTest, InlineReset) {
  ASSERT_EQ(RespSrvParser::INPUT_PENDING, Parse("\t \r\n"));
  EXPECT_EQ(4, consumed_);
  ASSERT_EQ(RespSrvParser::OK, Parse("*1\r\n$3\r\nfoo\r\n"));
  EXPECT_EQ(13, consumed_);
}

}  // namespace facade
