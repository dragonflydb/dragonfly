// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "facade/redis_parser.h"

#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>

#include "absl/strings/str_cat.h"
#include "base/gtest.h"
#include "base/logging.h"
#include "core/heap_size.h"
#include "facade/facade_test.h"

using namespace testing;
using namespace std;
namespace facade {

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
  static void SetUpTestSuite() {
  }

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
  EXPECT_THAT(args_, ElementsAre("KEY", "VAL"));

  ASSERT_EQ(RedisParser::INPUT_PENDING, Parse("KEY"));
  EXPECT_EQ(3, consumed_);
  ASSERT_EQ(RedisParser::INPUT_PENDING, Parse(" FOO "));
  EXPECT_EQ(5, consumed_);
  ASSERT_EQ(RedisParser::INPUT_PENDING, Parse(" BAR"));
  EXPECT_EQ(4, consumed_);
  ASSERT_EQ(RedisParser::OK, Parse(" \r\n "));
  EXPECT_EQ(3, consumed_);
  EXPECT_THAT(args_, ElementsAre("KEY", "FOO", "BAR"));

  ASSERT_EQ(RedisParser::INPUT_PENDING, Parse(" 1 2"));
  EXPECT_EQ(4, consumed_);
  ASSERT_EQ(RedisParser::INPUT_PENDING, Parse(" 45"));
  EXPECT_EQ(3, consumed_);
  ASSERT_EQ(RedisParser::OK, Parse("\r\n"));
  EXPECT_EQ(2, consumed_);
  EXPECT_THAT(args_, ElementsAre("1", "2", "45"));

  // Empty queries return INPUT_PENDING.
  EXPECT_EQ(RedisParser::INPUT_PENDING, Parse("\r\n"));
  EXPECT_EQ(2, consumed_);
}

TEST_F(RedisParserTest, InlineEscaping) {
  LOG(ERROR) << "TBD: to be compliant with sdssplitargs";  // TODO:
}

TEST_F(RedisParserTest, Multi1) {
  ASSERT_EQ(RedisParser::INPUT_PENDING, Parse("*1\r\n"));
  EXPECT_EQ(4, consumed_);
  EXPECT_EQ(0, parser_.parselen_hint());

  ASSERT_EQ(RedisParser::INPUT_PENDING, Parse("$4\r\n"));
  EXPECT_EQ(4, consumed_);
  EXPECT_EQ(4, parser_.parselen_hint());

  ASSERT_EQ(RedisParser::OK, Parse("PING\r\n"));
  EXPECT_EQ(6, consumed_);
  EXPECT_EQ(0, parser_.parselen_hint());
  EXPECT_THAT(args_, ElementsAre("PING"));
}

TEST_F(RedisParserTest, Multi2) {
  ASSERT_EQ(RedisParser::INPUT_PENDING, Parse("*1\r\n$"));
  EXPECT_EQ(5, consumed_);

  ASSERT_EQ(RedisParser::INPUT_PENDING, Parse("4\r\nMSET"));
  EXPECT_EQ(7, consumed_);

  ASSERT_EQ(RedisParser::OK, Parse("\r\n*2\r\n"));
  EXPECT_EQ(2, consumed_);

  ASSERT_EQ(RedisParser::INPUT_PENDING, Parse("*2\r\n$3\r\nKEY\r\n$3\r\nVAL"));
  EXPECT_EQ(20, consumed_);

  ASSERT_EQ(RedisParser::OK, Parse("\r\n"));
  EXPECT_EQ(2, consumed_);
  EXPECT_THAT(args_, ElementsAre("KEY", "VAL"));
}

TEST_F(RedisParserTest, Multi3) {
  const char kFirst[] = "*3\r\n$3\r\nSET\r\n$16\r\nkey:";
  const char kSecond[] = "000002273458\r\n$3\r\nVXK";
  ASSERT_EQ(RedisParser::INPUT_PENDING, Parse(kFirst));
  ASSERT_EQ(strlen(kFirst), consumed_);
  ASSERT_EQ(RedisParser::INPUT_PENDING, Parse(kSecond));
  ASSERT_EQ(strlen(kSecond), consumed_);
  ASSERT_EQ(RedisParser::OK, Parse("\r\n*3\r\n$3\r\nSET"));
  ASSERT_EQ(2, consumed_);
  EXPECT_THAT(args_, ElementsAre("SET", "key:000002273458", "VXK"));
}

TEST_F(RedisParserTest, ClientMode) {
  parser_.SetClientMode();

  ASSERT_EQ(RedisParser::OK, Parse(":-1\r\n"));
  EXPECT_THAT(args_, ElementsAre(IntArg(-1)));

  ASSERT_EQ(RedisParser::OK, Parse("+OK\r\n"));
  EXPECT_EQ(args_[0], "OK");

  ASSERT_EQ(RedisParser::OK, Parse("-ERR foo bar\r\n"));
  EXPECT_THAT(args_, ElementsAre(ErrArg("ERR foo")));

  ASSERT_EQ(RedisParser::INPUT_PENDING, Parse("_"));
  EXPECT_EQ(1, consumed_);
  ASSERT_EQ(RedisParser::INPUT_PENDING, Parse("\r"));
  EXPECT_EQ(1, consumed_);
  ASSERT_EQ(RedisParser::OK, Parse("\n"));
  EXPECT_EQ(1, consumed_);
  EXPECT_THAT(args_, ElementsAre(ArgType(RespExpr::NIL)));
  ASSERT_EQ(RedisParser::OK, Parse("*2\r\n_\r\n_\r\n"));
  ASSERT_EQ(10, consumed_);

  ASSERT_EQ(RedisParser::OK, Parse("*3\r\n+OK\r\n$1\r\n1\r\n*2\r\n$1\r\n1\r\n$-1\r\n"));
  ASSERT_THAT(args_, ElementsAre("OK", "1", ArrLen(2)));
}

TEST_F(RedisParserTest, Hierarchy) {
  parser_.SetClientMode();

  const char* kThirdArg = "*2\r\n$3\r\n100\r\n$3\r\n200\r\n";
  string resp = absl::StrCat("*3\r\n$3\r\n900\r\n$3\r\n800\r\n", kThirdArg);
  ASSERT_EQ(RedisParser::OK, Parse(resp));
  ASSERT_THAT(args_, ElementsAre("900", "800", ArrArg(2)));
  EXPECT_THAT(args_[2].GetVec(), ElementsAre("100", "200"));

  ASSERT_EQ(RedisParser::OK, Parse("*2\r\n*1\r\n$3\r\n1-0\r\n*1\r\n$2\r\nf1\r\n"));
  ASSERT_THAT(args_, ElementsAre(ArrLen(1), ArrLen(1)));
}

TEST_F(RedisParserTest, InvalidMult1) {
  ASSERT_EQ(RedisParser::BAD_BULKLEN, Parse("*2\r\n$3\r\nFOO\r\nBAR\r\n"));
}

TEST_F(RedisParserTest, Empty) {
  ASSERT_EQ(RedisParser::OK, Parse("*2\r\n$0\r\n\r\n$0\r\n\r\n"));
}

TEST_F(RedisParserTest, LargeBulk) {
  string_view prefix("*1\r\n$1024\r\n");

  ASSERT_EQ(RedisParser::INPUT_PENDING, Parse(prefix));
  ASSERT_EQ(prefix.size(), consumed_);
  ASSERT_GE(parser_.parselen_hint(), 1024);

  string half(512, 'a');
  ASSERT_EQ(RedisParser::INPUT_PENDING, Parse(half));
  ASSERT_EQ(512, consumed_);
  ASSERT_GE(parser_.parselen_hint(), 512);
  ASSERT_EQ(RedisParser::INPUT_PENDING, Parse(half));
  ASSERT_EQ(512, consumed_);
  ASSERT_EQ(RedisParser::INPUT_PENDING, Parse("\r"));
  ASSERT_EQ(1, consumed_);
  ASSERT_EQ(RedisParser::OK, Parse("\n"));
  EXPECT_EQ(1, consumed_);

  string part1 = absl::StrCat(prefix, half);
  ASSERT_EQ(RedisParser::INPUT_PENDING, Parse(part1));
  ASSERT_EQ(RedisParser::INPUT_PENDING, Parse(half));
  ASSERT_EQ(RedisParser::OK, Parse("\r\n"));

  prefix = "*1\r\n$270000000\r\n";
  ASSERT_EQ(RedisParser::INPUT_PENDING, Parse(prefix));
  ASSERT_EQ(prefix.size(), consumed_);
  string chunk(1000000, 'a');
  for (unsigned i = 0; i < 270; ++i) {
    ASSERT_EQ(RedisParser::INPUT_PENDING, Parse(chunk));
    ASSERT_EQ(chunk.size(), consumed_);
  }
  ASSERT_EQ(RedisParser::OK, Parse("\r\n"));
  ASSERT_THAT(args_, ElementsAre(ArgType(RespExpr::STRING)));
  EXPECT_EQ(270000000, args_[0].GetBuf().size());
}

TEST_F(RedisParserTest, NILs) {
  ASSERT_EQ(RedisParser::BAD_ARRAYLEN, Parse("_\r\n"));
  parser_.SetClientMode();
  ASSERT_EQ(RedisParser::OK, Parse("_\r\nfooobar"));
  EXPECT_EQ(3, consumed_);
}

TEST_F(RedisParserTest, NestedArray) {
  parser_.SetClientMode();

  // [[['foo'],['bar']],['car']]
  ASSERT_EQ(RedisParser::OK,
            Parse("*2\r\n*2\r\n*1\r\n$3\r\nfoo\r\n*1\r\n$3\r\nbar\r\n*1\r\n$3\r\ncar\r\n"));

  ASSERT_THAT(args_, ElementsAre(ArrArg(2), ArrArg(1)));
  ASSERT_THAT(args_[0].GetVec(), ElementsAre(ArrArg(1), ArrArg(1)));
  ASSERT_THAT(args_[1].GetVec(), ElementsAre("car"));
}

TEST_F(RedisParserTest, UsedMemory) {
  vector<vector<uint8_t>> blobs;
  for (size_t i = 0; i < 100; ++i) {
    blobs.emplace_back(vector<uint8_t>(200));
  }
  EXPECT_GT(dfly::HeapSize(blobs), 20000);

  std::vector<std::unique_ptr<RespVec>> stash;
  RespVec vec;
  for (unsigned i = 0; i < 10; ++i) {
    vec.emplace_back(RespExpr::STRING);
    vec.back().u = RespExpr::Buffer(nullptr, 0);
  }

  for (unsigned i = 0; i < 100; i++) {
    stash.emplace_back(new RespExpr::Vec(vec));
  }
  EXPECT_GT(dfly::HeapSize(stash), 30000);
}

TEST_F(RedisParserTest, Eol) {
  ASSERT_EQ(RedisParser::INPUT_PENDING, Parse("*1\r"));
  EXPECT_EQ(3, consumed_);
  ASSERT_EQ(RedisParser::INPUT_PENDING, Parse("\n$5\r\n"));
  EXPECT_EQ(5, consumed_);
}

TEST_F(RedisParserTest, BulkSplit) {
  ASSERT_EQ(RedisParser::INPUT_PENDING, Parse("*1\r\n$4\r\nSADD\r"));
  ASSERT_EQ(13, consumed_);
  ASSERT_EQ(RedisParser::OK, Parse("\n"));
}

TEST_F(RedisParserTest, InlineSplit) {
  ASSERT_EQ(RedisParser::INPUT_PENDING, Parse("\n"));
  EXPECT_EQ(1, consumed_);
  ASSERT_EQ(RedisParser::OK, Parse("\nPING\n\n"));
  EXPECT_EQ(6, consumed_);
  ASSERT_EQ(RedisParser::INPUT_PENDING, Parse("\n"));
  EXPECT_EQ(1, consumed_);
  ASSERT_EQ(RedisParser::INPUT_PENDING, Parse("P"));
  ASSERT_EQ(RedisParser::OK, Parse("ING\n"));
}

TEST_F(RedisParserTest, InlineReset) {
  ASSERT_EQ(RedisParser::INPUT_PENDING, Parse("\t \r\n"));
  EXPECT_EQ(4, consumed_);
  ASSERT_EQ(RedisParser::OK, Parse("*1\r\n$3\r\nfoo\r\n"));
  EXPECT_EQ(13, consumed_);
}

}  // namespace facade
