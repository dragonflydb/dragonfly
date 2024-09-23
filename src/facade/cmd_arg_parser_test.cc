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
  auto parser = Make({"STRING", "VIEW", "11", "22", "33", "44"});

  EXPECT_TRUE(parser.HasNext());

  EXPECT_EQ(parser.Next<string>(), "STRING"s);
  EXPECT_EQ(parser.Next<string_view>(), "VIEW"sv);

  EXPECT_EQ(parser.Next<size_t>(), 11u);
  EXPECT_EQ(parser.Next<size_t>(), 22u);
  auto [a, b] = parser.Next<size_t, size_t>();
  EXPECT_EQ(a, 33u);
  EXPECT_EQ(b, 44u);

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

  EXPECT_EQ(parser.Next<size_t>(), 0u);

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
  EXPECT_TRUE(parser.Check("TAG_2"));
  EXPECT_EQ(parser.Next<int>(), 22);
}

TEST_F(CmdArgParserTest, NextStatement) {
  auto parser = Make({"TAG", "tag_2", "tag_3"});

  parser.ExpectTag("TAG");
  EXPECT_FALSE(parser.Error());

  parser.ExpectTag("TAG_2");
  EXPECT_FALSE(parser.Error());

  parser.ExpectTag("TAG_2");
  EXPECT_TRUE(parser.Error());
}

TEST_F(CmdArgParserTest, CheckTailFail) {
  auto parser = Make({"TAG", "11", "22", "TAG", "text"});

  int first;
  string_view second;
  EXPECT_TRUE(parser.Check("TAG", &first, &second));
  EXPECT_EQ(first, 11);
  EXPECT_EQ(second, "22");

  EXPECT_FALSE(parser.Check("TAG", &first, &second));
  EXPECT_TRUE(parser.Check("TAG", &first));
  EXPECT_TRUE(parser.Error());
}

TEST_F(CmdArgParserTest, Map) {
  auto parser = Make({"TWO", "NONE"});

  EXPECT_EQ(parser.MapNext("ONE", 1, "TWO", 2), 2);

  EXPECT_EQ(parser.MapNext("ONE", 1, "TWO", 2), 0);
  auto err = parser.Error();
  EXPECT_TRUE(err);
  EXPECT_EQ(err->type, CmdArgParser::INVALID_CASES);
  EXPECT_EQ(err->index, 1);
}

TEST_F(CmdArgParserTest, TryMapNext) {
  auto parser = Make({"TWO", "GREEN"});

  EXPECT_EQ(parser.TryMapNext("ONE", 1, "TWO", 2), std::make_optional(2));

  EXPECT_EQ(parser.TryMapNext("ONE", 1, "TWO", 2), std::nullopt);
  EXPECT_FALSE(parser.HasError());
  EXPECT_EQ(parser.TryMapNext("green", 1, "yellow", 2), std::make_optional(1));
  EXPECT_FALSE(parser.HasError());
}

TEST_F(CmdArgParserTest, IgnoreCase) {
  auto parser = Make({"hello", "marker", "taail", "world"});

  EXPECT_EQ(absl::implicit_cast<string_view>(parser.Next()), "hello"sv);

  EXPECT_TRUE(parser.Check("MARKER"sv));
  parser.Skip(1);

  EXPECT_EQ(absl::implicit_cast<string_view>(parser.Next()), "world"sv);
}

TEST_F(CmdArgParserTest, FixedRangeInt) {
  {
    auto parser = Make({"10", "-10", "12"});

    EXPECT_EQ((parser.Next<FInt<-11, 11>>().value), 10);
    EXPECT_EQ((parser.Next<FInt<-11, 11>>().value), -10);
    EXPECT_EQ((parser.Next<FInt<-11, 11>>().value), 0);

    auto err = parser.Error();
    EXPECT_TRUE(err);
    EXPECT_EQ(err->type, CmdArgParser::INVALID_INT);
    EXPECT_EQ(err->index, 2);
  }

  {
    auto parser = Make({"-12"});
    EXPECT_EQ((parser.Next<FInt<-11, 11>>().value), 0);

    auto err = parser.Error();
    EXPECT_TRUE(err);
    EXPECT_EQ(err->type, CmdArgParser::INVALID_INT);
    EXPECT_EQ(err->index, 0);
  }
}

}  // namespace facade
