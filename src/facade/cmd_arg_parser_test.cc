// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "facade/cmd_arg_parser.h"

#include <absl/base/casts.h>
#include <gmock/gmock.h>

#include "facade/memcache_parser.h"

using namespace testing;
using namespace std;

namespace facade {

class CmdArgParserTest : public testing::Test {
 public:
  CmdArgParser Make(absl::Span<const std::string_view> args) {
    storage_.assign(args.begin(), args.end());
    arg_vec_.clear();
    for (auto& s : storage_)
      arg_vec_.push_back(MutableSlice{s.data(), s.size()});
    return CmdArgParser{absl::MakeSpan(arg_vec_)};
  }

 private:
  CmdArgVec arg_vec_;
  std::vector<std::string> storage_;
};

TEST_F(CmdArgParserTest, BasicTypes) {
  auto parser = Make({"STRING", "VIEW", "11", "22"});

  EXPECT_TRUE(parser.HasNext());

  EXPECT_EQ(absl::implicit_cast<string>(parser.Next()), "STRING"s);
  EXPECT_EQ(absl::implicit_cast<string_view>(parser.Next()), "VIEW"sv);

#ifndef __APPLE__
  EXPECT_EQ(parser.Next().Int<size_t>(), 11u);
  EXPECT_EQ(parser.Next().Int<size_t>(), 22u);
#endif

  EXPECT_FALSE(parser.HasNext());
  EXPECT_FALSE(parser.Error());
}

TEST_F(CmdArgParserTest, BoundError) {
  auto parser = Make({});

  EXPECT_EQ(absl::implicit_cast<string_view>(parser.Next()), ""sv);

  auto err = parser.Error();
  EXPECT_TRUE(err);
  EXPECT_EQ(err->type, CmdArgParser::OUT_OF_BOUNDS);
  EXPECT_EQ(err->index, 0);
}

#ifndef __APPLE__
TEST_F(CmdArgParserTest, IntError) {
  auto parser = Make({"NOTANINT"});

  EXPECT_EQ(parser.Next().Int<size_t>(), 0u);

  auto err = parser.Error();
  EXPECT_TRUE(err);
  EXPECT_EQ(err->type, CmdArgParser::INVALID_INT);
  EXPECT_EQ(err->index, 0);
}
#endif

TEST_F(CmdArgParserTest, Check) {
  auto parser = Make({"TAG", "TAG_2", "22"});

  EXPECT_FALSE(parser.Check("NOT_TAG"));
  EXPECT_TRUE(parser.Check("TAG"));

  EXPECT_FALSE(parser.Check("NOT_TAG_2"));
  EXPECT_TRUE(parser.Check("TAG_2").ExpectTail(1));
}

TEST_F(CmdArgParserTest, CheckTailFail) {
  auto parser = Make({"TAG", "11", "22", "TAG", "33"});

  EXPECT_TRUE(parser.Check("TAG").ExpectTail(2));
  parser.Skip(2);

  EXPECT_FALSE(parser.Check("TAG").ExpectTail(2));

  auto err = parser.Error();
  EXPECT_TRUE(err);
  EXPECT_EQ(err->type, CmdArgParser::SHORT_OPT_TAIL);
  EXPECT_EQ(err->index, 3);
}

TEST_F(CmdArgParserTest, Cases) {
  auto parser = Make({"TWO", "NONE"});

  EXPECT_EQ(int(parser.Next().Case("ONE", 1).Case("TWO", 2)), 2);

  EXPECT_EQ(int(parser.Next().Case("ONE", 1).Case("TWO", 2)), 0);
  auto err = parser.Error();
  EXPECT_TRUE(err);
  EXPECT_EQ(err->type, CmdArgParser::INVALID_CASES);
  EXPECT_EQ(err->index, 1);
}

TEST_F(CmdArgParserTest, NextUpper) {
  auto parser = Make({"hello", "marker", "taail", "world"});

  parser.ToUpper();
  EXPECT_EQ(absl::implicit_cast<string_view>(parser.Next()), "HELLO"sv);

  parser.ToUpper();
  EXPECT_TRUE(parser.Check("MARKER"sv).ExpectTail(1).NextUpper());
  parser.Skip(1);

  EXPECT_EQ(absl::implicit_cast<string_view>(parser.Next()), "WORLD"sv);
}

}  // namespace facade
