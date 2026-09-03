// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "facade/resp_srv_parser.h"

#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>

#include <random>

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

// Parsed arguments remain valid after the input is overwritten.
TEST_F(RespSrvParserTest, ParsedArgsSurviveSourceBufferOverwrite) {
  string request = "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";

  ASSERT_EQ(RespSrvParser::OK, Parse(request));
  ASSERT_EQ(request.size(), consumed_);
  fill(request.begin(), request.end(), '\xCC');

  // Verifies that the parsed arguments are still correct.
  // This means the parser copied the argument data instead of keeping
  // references to the original request buffer.
  EXPECT_THAT(Vec(), ElementsAre("SET", "key", "value"));
}

// The parser consumes every fragment of an incomplete command.
TEST_F(RespSrvParserTest, PendingFragmentsAreFullyConsumed) {
  for (string_view fragment : {"*2\r\n", "$4\r\nECHO\r\n", "$5\r\nhe", "llo"}) {
    ASSERT_EQ(RespSrvParser::INPUT_PENDING, Parse(fragment));
    EXPECT_EQ(fragment.size(), consumed_);
  }

  ASSERT_EQ(RespSrvParser::OK, Parse("\r\n"));
  EXPECT_EQ(2, consumed_);
  EXPECT_THAT(Vec(), ElementsAre("ECHO", "hello"));
}

// A large bulk value is correctly assembled from small fragments.
TEST_F(RespSrvParserTest, FragmentedLargeBulkSurvivesSourceBufferReuse) {
  constexpr size_t kValueSize = 1U << 20;
  const string value(kValueSize, 'x');
  const string header = absl::StrCat("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$", kValueSize, "\r\n");
  string shared_buffer;

  auto parse = [this, &shared_buffer](string_view fragment) {
    shared_buffer.assign(fragment);
    ASSERT_EQ(Parse(shared_buffer), RespSrvParser::INPUT_PENDING);
    ASSERT_EQ(consumed_, fragment.size());
    fill(shared_buffer.begin(), shared_buffer.end(), '\xCC');
  };

  parse(header);
  for (size_t offset = 0; offset < value.size(); offset += 257) {
    parse(string_view{value}.substr(offset, min<size_t>(257, value.size() - offset)));
  }

  ASSERT_EQ(Parse("\r\n"), RespSrvParser::OK);
  ASSERT_EQ(consumed_, 2u);
  EXPECT_THAT(Vec(), ElementsAre("SET", "key", value));
}

// Independent parsers can assemble interleaved command fragments.
TEST(RespSrvParserSharedBufferTest, AlternatingParsersSurviveSourceBufferReuse) {
  RespSrvParser first_parser, second_parser;
  cmn::BackedArguments first_args, second_args;
  uint32_t consumed = 0;
  string shared_buffer;

  auto parse = [&shared_buffer, &consumed](RespSrvParser* parser, cmn::BackedArguments* args,
                                           string_view fragment) {
    shared_buffer.assign(fragment);
    RespSrvParser::Buffer buffer{reinterpret_cast<const uint8_t*>(shared_buffer.data()),
                                 shared_buffer.size()};
    auto result = parser->Parse(buffer, &consumed, args);
    EXPECT_EQ(consumed, fragment.size());
    return result;
  };

  EXPECT_EQ(parse(&first_parser, &first_args, "*2\r\n$4\r\nECHO\r\n$5\r\nhe"),
            RespSrvParser::INPUT_PENDING);
  EXPECT_EQ(parse(&second_parser, &second_args, "*2\r\n$4\r\nECHO\r\n$5\r\nwo"),
            RespSrvParser::INPUT_PENDING);
  EXPECT_EQ(parse(&first_parser, &first_args, "llo\r\n"), RespSrvParser::OK);
  EXPECT_EQ(parse(&second_parser, &second_args, "rld\r\n"), RespSrvParser::OK);

  const auto expected = {pair{&first_args, "hello"}, pair{&second_args, "world"}};
  for (const auto& [args, value] : expected) {
    ASSERT_EQ(args->size(), 2u);
    EXPECT_EQ((*args)[0], "ECHO");
    EXPECT_EQ((*args)[1], value);
  }
}

// Network input can split a command at many different places.
// Split 100 commands into repeatable random-sized fragments and check they are rebuilt correctly.
TEST(RespSrvParserSharedBufferTest, SeededFragmentationConsumesAndReassemblesCommands) {
  mt19937 rng{0x5EED};

  for (unsigned iteration = 0; iteration < 100; ++iteration) {
    const string value = absl::StrCat("value-", iteration, "-", string(rng() % 256, 'x'));
    const bool inline_command = (rng() % 2) == 0;
    const string request = inline_command ? absl::StrCat("ECHO ", value, "\r\n")
                                          : absl::StrCat("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$",
                                                         value.size(), "\r\n", value, "\r\n");
    RespSrvParser parser;
    cmn::BackedArguments args;
    uint32_t consumed = 0;

    for (size_t offset = 0; offset < request.size();) {
      const size_t fragment_len = min<size_t>(1 + rng() % 31, request.size() - offset);
      string fragment = request.substr(offset, fragment_len);
      RespSrvParser::Buffer buffer{reinterpret_cast<const uint8_t*>(fragment.data()),
                                   fragment.size()};
      const auto result = parser.Parse(buffer, &consumed, &args);
      EXPECT_EQ(consumed, fragment.size()) << "iteration=" << iteration;
      offset += fragment_len;
      if (offset < request.size())
        EXPECT_EQ(result, RespSrvParser::INPUT_PENDING) << "iteration=" << iteration;
      else
        EXPECT_EQ(result, RespSrvParser::OK) << "iteration=" << iteration;
    }

    ASSERT_EQ(args.size(), inline_command ? 2u : 3u);
    EXPECT_EQ(args[0], inline_command ? "ECHO" : "SET");
    if (inline_command) {
      EXPECT_EQ(args[1], value);
    } else {
      EXPECT_EQ(args[1], "key");
      EXPECT_EQ(args[2], value);
    }
  }
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
