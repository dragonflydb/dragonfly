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

  auto Vec() {
    vector<string_view> out;
    ranges::copy(args_.view(), back_inserter(out));
    return out;
  }

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
  EXPECT_THAT(Vec(), ElementsAre("KEY", "VAL"));

  ASSERT_EQ(RespSrvParser::INPUT_PENDING, Parse("KEY"));
  EXPECT_EQ(3, consumed_);
  ASSERT_EQ(RespSrvParser::INPUT_PENDING, Parse(" FOO "));
  EXPECT_EQ(5, consumed_);
  ASSERT_EQ(RespSrvParser::INPUT_PENDING, Parse(" BAR"));
  EXPECT_EQ(4, consumed_);
  ASSERT_EQ(RespSrvParser::OK, Parse(" \r\n "));
  EXPECT_EQ(3, consumed_);
  EXPECT_THAT(Vec(), ElementsAre("KEY", "FOO", "BAR"));

  ASSERT_EQ(RespSrvParser::INPUT_PENDING, Parse(" 1 2"));
  EXPECT_EQ(4, consumed_);
  ASSERT_EQ(RespSrvParser::INPUT_PENDING, Parse(" 45"));
  EXPECT_EQ(3, consumed_);
  ASSERT_EQ(RespSrvParser::OK, Parse("\r\n"));
  EXPECT_EQ(2, consumed_);
  EXPECT_THAT(Vec(), ElementsAre("1", "2", "45"));

  // Empty queries return INPUT_PENDING.
  EXPECT_EQ(RespSrvParser::INPUT_PENDING, Parse("\r\n"));
  EXPECT_EQ(2, consumed_);

  ASSERT_EQ(RespSrvParser::OK, Parse("_\r\n"));
  EXPECT_THAT(Vec(), ElementsAre("_"));
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
  EXPECT_THAT(Vec(), ElementsAre("PING"));
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
  EXPECT_THAT(Vec(), ElementsAre("KEY", "VAL"));
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
  EXPECT_THAT(Vec(), ElementsAre("SET", "key:000002273458", "VXK"));
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

TEST_F(RespSrvParserTest, InlineTooLong) {
  // A single unterminated token.
  string big(40000, 'A');
  ASSERT_EQ(RespSrvParser::INPUT_PENDING, Parse(big));
  EXPECT_EQ(RespSrvParser::BAD_INLINE, Parse(big));  // 80000 > 64KB, still no EOL
}

TEST_F(RespSrvParserTest, InlineManyTokensTooLong) {
  string chunk;
  for (unsigned i = 0; i < 5000; ++i)
    chunk += "aaaaaaa ";  // 40000 bytes of complete tokens
  ASSERT_EQ(RespSrvParser::INPUT_PENDING, Parse(chunk));
  EXPECT_EQ(RespSrvParser::BAD_INLINE, Parse(chunk));
}

TEST_F(RespSrvParserTest, InlineCapFinalFragmentWithEol) {
  // The cap must hold even when the crossing fragment carries the newline.
  string big(63 * 1024, 'A');
  ASSERT_EQ(RespSrvParser::INPUT_PENDING, Parse(big));
  EXPECT_EQ(RespSrvParser::BAD_INLINE, Parse(string(4096, 'A') + "\r\n"));
}

TEST_F(RespSrvParserTest, InlineCapSingleBufferWithEol) {
  // An oversized line completed within one buffer must be rejected as well.
  EXPECT_EQ(RespSrvParser::BAD_INLINE, Parse(string(70 * 1024, 'A') + "\r\n"));
}

TEST_F(RespSrvParserTest, InlineBelowCapOk) {
  string ok_line(63 * 1024, 'B');
  ASSERT_EQ(RespSrvParser::INPUT_PENDING, Parse(ok_line));
  ASSERT_EQ(RespSrvParser::OK, Parse("\r\n"));
  EXPECT_THAT(Vec(), ElementsAre(ok_line));
}

TEST_F(RespSrvParserTest, HugeBulkNoEagerAlloc) {
  // Declaring a huge bulk length must not allocate the full buffer upfront.
  ASSERT_EQ(RespSrvParser::INPUT_PENDING, Parse("*1\r\n$200000000\r\n"));
  EXPECT_LT(args_.HeapMemory() + parser_.UsedMemory(), 64u * 1024);
}

TEST_F(RespSrvParserTest, BulkOverEagerLimitAssembled) {
  const size_t kLen = 2'000'000;
  ASSERT_EQ(RespSrvParser::INPUT_PENDING, Parse(absl::StrCat("*1\r\n$", kLen, "\r\n")));
  EXPECT_LT(args_.HeapMemory() + parser_.UsedMemory(), 64u * 1024);

  string chunk(100'000, 'x');
  chunk.front() = 'F';
  for (unsigned i = 0; i < kLen / chunk.size(); ++i) {
    ASSERT_EQ(RespSrvParser::INPUT_PENDING, Parse(chunk));
    ASSERT_EQ(chunk.size(), consumed_);
  }
  ASSERT_EQ(RespSrvParser::OK, Parse("\r\n"));
  ASSERT_EQ(1u, args_.size());
  ASSERT_EQ(kLen, args_[0].size());
  EXPECT_EQ('F', args_[0][0]);
  EXPECT_EQ('F', args_[0][kLen - chunk.size()]);
  EXPECT_EQ('x', args_[0][kLen - 1]);
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

TEST_F(RespSrvParserTest, EmptyLinesBeforeMultibulk) {
  // redis-cli --pipe prefixes its final ECHO with CRLF. The sentinel is binary data.
  const string sentinel("0123\r\n\0*2\r\nabcdefghi", 20);
  const string echo = absl::StrCat("*2\r\n$4\r\nECHO\r\n$20\r\n", sentinel, "\r\n");
  for (string_view prefix : {"\r\n", "\n", "\r\n\r\n", "\t \r\n\n"}) {
    const string input = absl::StrCat(prefix, echo);
    // Include every two-buffer split, including CRLF and the multibulk marker.
    for (size_t split = 1; split <= input.size(); ++split) {
      SCOPED_TRACE(absl::StrCat("prefix size: ", prefix.size(), ", split: ", split));
      ASSERT_EQ(split == input.size() ? RespSrvParser::OK : RespSrvParser::INPUT_PENDING,
                Parse(string_view(input).substr(0, split)));
      ASSERT_EQ(split, consumed_);
      if (split < input.size()) {
        ASSERT_EQ(RespSrvParser::OK, Parse(string_view(input).substr(split)));
        ASSERT_EQ(input.size() - split, consumed_);
      }
      EXPECT_THAT(Vec(), ElementsAre("ECHO", sentinel));
    }
  }
}

TEST_F(RespSrvParserTest, EmptyLineTooLong) {
  // An empty argument list must not hide a size-limit error.
  EXPECT_EQ(RespSrvParser::BAD_INLINE, Parse(string(70 * 1024, ' ')));
}

TEST_F(RespSrvParserTest, EmptyLinesBetweenPipelinedCommands) {
  const string input = "*1\r\n$4\r\nPING\r\n\r\n*2\r\n$4\r\nECHO\r\n$3\r\nfoo\r\n\nPING\r\n";
  string_view remaining(input);
  ASSERT_EQ(RespSrvParser::OK, Parse(remaining));
  EXPECT_THAT(Vec(), ElementsAre("PING"));
  ASSERT_EQ(14, consumed_);
  remaining.remove_prefix(consumed_);

  ASSERT_EQ(RespSrvParser::OK, Parse(remaining));
  EXPECT_THAT(Vec(), ElementsAre("ECHO", "foo"));
  ASSERT_EQ(25, consumed_);
  remaining.remove_prefix(consumed_);

  ASSERT_EQ(RespSrvParser::OK, Parse(remaining));
  EXPECT_THAT(Vec(), ElementsAre("PING"));
  EXPECT_EQ(remaining.size(), consumed_);
}

static string SetCmd(size_t val_size) {
  const string val(val_size, 'x');
  return absl::StrCat("*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$", val_size, "\r\n", val, "\r\n");
}

TEST_F(RespSrvParserTest, ResetForParseChecksFloor) {
  ASSERT_EQ(RespSrvParser::OK, Parse(SetCmd(10 * 1024)));
  const size_t grown = args_.HeapMemory();
  EXPECT_GE(grown, 10 * 1024);

  ASSERT_EQ(RespSrvParser::OK, Parse("*1\r\n$4\r\nPING\r\n"));
  ASSERT_EQ(RespSrvParser::OK, Parse("*1\r\n$4\r\nPING\r\n"));
  // not reclaimed if below floor
  EXPECT_EQ(grown, args_.HeapMemory());
}

TEST_F(RespSrvParserTest, ResetForParseShrinksAfterSingleCommand) {
  ASSERT_EQ(RespSrvParser::OK, Parse(SetCmd(512 * 1024)));
  EXPECT_GE(args_.HeapMemory(), 512u * 1024);

  ASSERT_EQ(RespSrvParser::OK, Parse("*1\r\n$4\r\nPING\r\n"));
  EXPECT_EQ(0u, args_.HeapMemory());
}

}  // namespace facade
