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
  EXPECT_FALSE(parser.HasError());
}

TEST_F(CmdArgParserTest, BoundError) {
  auto parser = Make({});

  EXPECT_EQ(absl::implicit_cast<string_view>(parser.Next()), ""sv);

  auto err = parser.TakeError();
  EXPECT_TRUE(err);
  EXPECT_EQ(err.type, CmdArgParser::OUT_OF_BOUNDS);
  EXPECT_EQ(err.index, 0);
}

#ifndef __APPLE__
TEST_F(CmdArgParserTest, IntError) {
  auto parser = Make({"NOTANINT"});

  EXPECT_EQ(parser.Next<size_t>(), 0u);

  auto err = parser.TakeError();
  EXPECT_TRUE(err);
  EXPECT_EQ(err.type, CmdArgParser::INVALID_INT);
  EXPECT_EQ(err.index, 0);
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
  EXPECT_FALSE(parser.TakeError());

  parser.ExpectTag("TAG_2");
  EXPECT_FALSE(parser.TakeError());

  parser.ExpectTag("TAG_2");
  EXPECT_TRUE(parser.TakeError());
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
  EXPECT_TRUE(parser.TakeError());
}

TEST_F(CmdArgParserTest, Map) {
  auto parser = Make({"TWO", "NONE"});

  EXPECT_EQ(parser.MapNext("ONE", 1, "TWO", 2), 2);

  EXPECT_EQ(parser.MapNext("ONE", 1, "TWO", 2), 0);
  auto err = parser.TakeError();
  EXPECT_TRUE(err);
  EXPECT_EQ(err.type, CmdArgParser::INVALID_CASES);
  EXPECT_EQ(err.index, 1);
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

TEST_F(CmdArgParserTest, Apply) {
  // All option shapes: Exist sets a bool, Tag-with-one-field, Tag-with-two-fields.
  {
    auto parser = Make({"FLAG", "COUNT", "5", "LIMIT", "10", "20"});

    bool flag = false;
    uint32_t count = 0;
    uint32_t offset = 0;
    uint32_t limit = 0;

    parser.Apply(Exist("FLAG", &flag), Tag("COUNT", &count), Tag("LIMIT", &offset, &limit));

    EXPECT_TRUE(flag);
    EXPECT_EQ(count, 5u);
    EXPECT_EQ(offset, 10u);
    EXPECT_EQ(limit, 20u);
    EXPECT_FALSE(parser.HasError());
  }

  // Unknown option is left unconsumed (no error). The caller decides what to do next.
  {
    auto parser = Make({"COUNT", "5", "BOGUS"});

    uint32_t count = 0;
    parser.Apply(Tag("COUNT", &count));

    EXPECT_EQ(count, 5u);
    EXPECT_FALSE(parser.HasError());
    EXPECT_TRUE(parser.HasNext());
    EXPECT_EQ(parser.Peek(), "BOGUS");
  }

  // Case-insensitive matching (consistent with Check).
  {
    auto parser = Make({"count", "7"});

    uint32_t count = 0;
    parser.Apply(Tag("COUNT", &count));

    EXPECT_EQ(count, 7u);
    EXPECT_FALSE(parser.HasError());
  }

  // Invalid integer in a Tag arg propagates the error.
  {
    auto parser = Make({"COUNT", "NAN"});

    uint32_t count = 0;
    parser.Apply(Tag("COUNT", &count));

    auto err = parser.TakeError();
    EXPECT_TRUE(err);
    EXPECT_EQ(err.type, CmdArgParser::INVALID_INT);
  }
}

TEST_F(CmdArgParserTest, ApplyOrSkip) {
  // ApplyOrSkip silently skips any unknown arg (1 at a time) and keeps going.
  {
    auto parser = Make({"BOGUS", "COUNT", "5", "MORE_BOGUS", "STUFF"});

    uint32_t count = 0;
    parser.ApplyOrSkip(Tag("COUNT", &count));

    EXPECT_EQ(count, 5u);
    EXPECT_FALSE(parser.HasError());
    EXPECT_FALSE(parser.HasNext());  // everything consumed
  }
  // Empty input — no error, no work.
  {
    auto parser = Make({});
    uint32_t count = 0;
    parser.ApplyOrSkip(Tag("COUNT", &count));
    EXPECT_FALSE(parser.HasError());
    EXPECT_FALSE(parser.HasNext());
  }
  // Trailing unknown at end-of-args: the skip must not trip OUT_OF_BOUNDS.
  {
    auto parser = Make({"BOGUS"});
    uint32_t count = 0;
    parser.ApplyOrSkip(Tag("COUNT", &count));
    EXPECT_FALSE(parser.HasError());
    EXPECT_FALSE(parser.HasNext());
  }
}

TEST_F(CmdArgParserTest, ApplyTagMissingValue) {
  // A matched tag with missing trailing value(s) must surface an error, not be silently skipped.
  // This guards against a subtle interaction with ApplyOrSkip: if TagOpt treated "tag matches,
  // values missing" as "no match", the skip path would swallow the malformed option.
  {
    auto parser = Make({"COUNT"});  // tag matches, value missing
    uint32_t count = 0;
    parser.Apply(Tag("COUNT", &count));
    auto err = parser.TakeError();
    EXPECT_TRUE(err);
    EXPECT_EQ(err.type, CmdArgParser::OUT_OF_BOUNDS);
  }
  {
    auto parser = Make({"COUNT"});
    uint32_t count = 0;
    parser.ApplyOrSkip(Tag("COUNT", &count));
    // Tag must have been consumed (not left for Skip to swallow silently).
    EXPECT_FALSE(parser.HasNext());
    auto err = parser.TakeError();
    EXPECT_TRUE(err);
    EXPECT_EQ(err.type, CmdArgParser::OUT_OF_BOUNDS);
  }
  // Also guard the two-field case: LIMIT with only one trailing value.
  {
    auto parser = Make({"LIMIT", "10"});  // needs offset + limit
    uint32_t offset = 0, limit = 0;
    parser.Apply(Tag("LIMIT", &offset, &limit));
    auto err = parser.TakeError();
    EXPECT_TRUE(err);
    EXPECT_EQ(err.type, CmdArgParser::OUT_OF_BOUNDS);
  }
}

TEST_F(CmdArgParserTest, ReportBeforeAnyNext) {
  // Report(code) at cur_i_ == 0 must clamp the error index to 0 rather than underflow to SIZE_MAX.
  auto parser = Make({"x"});
  parser.Report(CmdArgParser::CUSTOM_ERROR);
  auto err = parser.TakeError();
  EXPECT_TRUE(err);
  EXPECT_EQ(err.index, 0u);
}

TEST_F(CmdArgParserTest, ApplyLambda) {
  // Tag() with a lambda lets callers run custom parsing on match. Useful for side-effectful cases
  // like push_back or toggling a bool to false.
  auto parser = Make({"GET", "p1", "ASC", "GET", "p2"});

  std::vector<std::string_view> patterns;
  bool reversed = true;

  parser.Apply(
      Tag("ASC", [&](CmdArgParser*) { reversed = false; }),
      Tag("GET", [&](CmdArgParser* p) { patterns.push_back(p->Next<std::string_view>()); }));

  EXPECT_FALSE(reversed);
  ASSERT_EQ(patterns.size(), 2u);
  EXPECT_EQ(patterns[0], "p1");
  EXPECT_EQ(patterns[1], "p2");
  EXPECT_FALSE(parser.HasError());
}

TEST_F(CmdArgParserTest, ApplyMap) {
  // Map(&field, tag, value, ...) — matches any tag and writes the corresponding value.
  // Standalone Map allows repeated matches (last wins); wrap in OneOf to require at most one.
  {
    auto parser = Make({"DESC"});
    bool reversed = false;
    parser.Apply(Map(&reversed, "DESC", true, "ASC", false));
    EXPECT_TRUE(reversed);
    EXPECT_FALSE(parser.HasError());
  }
  {
    auto parser = Make({"ASC"});
    bool reversed = true;
    parser.Apply(Map(&reversed, "DESC", true, "ASC", false));
    EXPECT_FALSE(reversed);
    EXPECT_FALSE(parser.HasError());
  }
  // Unrelated tag leaves field untouched and stops Apply.
  {
    auto parser = Make({"OTHER"});
    bool reversed = false;
    parser.Apply(Map(&reversed, "DESC", true, "ASC", false));
    EXPECT_FALSE(reversed);
    EXPECT_TRUE(parser.HasNext());
  }
  // Standalone Map allows repeated matches — last wins, no error. This matches Redis SORT
  // semantics where "ASC DESC" is equivalent to "DESC".
  {
    auto parser = Make({"DESC", "ASC"});
    bool reversed = true;
    parser.Apply(Map(&reversed, "DESC", true, "ASC", false));
    EXPECT_FALSE(reversed);  // ASC came last
    EXPECT_FALSE(parser.HasError());
  }
  {
    auto parser = Make({"ASC", "DESC"});
    bool reversed = false;
    parser.Apply(Map(&reversed, "DESC", true, "ASC", false));
    EXPECT_TRUE(reversed);  // DESC came last
    EXPECT_FALSE(parser.HasError());
  }
  // OneOf + Map — DESC followed by ASC is a mutex violation.
  {
    auto parser = Make({"DESC", "ASC"});
    bool reversed = false;
    parser.Apply(OneOf(Map(&reversed, "DESC", true, "ASC", false)));
    auto err = parser.TakeError();
    EXPECT_TRUE(err);
    EXPECT_EQ(err.type, CmdArgParser::INVALID_CASES);
  }
}

TEST_F(CmdArgParserTest, ApplyTagNested) {
  // Tag(tag, inner_opt) — outer tag matches, then inner option runs against the next arg.
  // If the inner doesn't match, INVALID_CASES is reported (the inner keyword is required).
  enum class Mode { A, B, C };
  {
    auto parser = Make({"MODE", "B"});
    Mode mode = Mode::A;
    parser.Apply(Tag("MODE", Map(&mode, "A", Mode::A, "B", Mode::B, "C", Mode::C)));
    EXPECT_EQ(mode, Mode::B);
    EXPECT_FALSE(parser.HasError());
  }
  // Unknown inner tag -> INVALID_CASES.
  {
    auto parser = Make({"MODE", "BOGUS"});
    Mode mode = Mode::A;
    parser.Apply(Tag("MODE", Map(&mode, "A", Mode::A, "B", Mode::B)));
    auto err = parser.TakeError();
    EXPECT_TRUE(err);
    EXPECT_EQ(err.type, CmdArgParser::INVALID_CASES);
  }
  // Outer tag absent -> no effect, no error.
  {
    auto parser = Make({});
    Mode mode = Mode::A;
    parser.Apply(Tag("MODE", Map(&mode, "A", Mode::A, "B", Mode::B)));
    EXPECT_EQ(mode, Mode::A);
    EXPECT_FALSE(parser.HasError());
  }
}

TEST_F(CmdArgParserTest, ApplyTagIf) {
  // If(cond, opt) behaves like `opt` when cond is true, and never matches when false.
  // Use to gate an option on a runtime flag (e.g. is_read_only).

  // cond=true -> delegate to inner (matches and sets field).
  {
    auto parser = Make({"STORE", "dest"});
    std::string_view store;
    parser.Apply(If(true, Tag("STORE", &store)));
    EXPECT_EQ(store, "dest");
    EXPECT_FALSE(parser.HasError());
  }

  // cond=false -> inner is skipped. Apply stops at the (now unmatched) arg; Finalize reports
  // UNPROCESSED so the caller can surface a syntax error.
  {
    auto parser = Make({"STORE", "dest"});
    std::string_view store;
    parser.Apply(If(false, Tag("STORE", &store)));
    EXPECT_EQ(store, "");
    EXPECT_FALSE(parser.HasError());
    EXPECT_TRUE(parser.HasNext());
    EXPECT_FALSE(parser.Finalize());
    auto err = parser.TakeError();
    EXPECT_TRUE(err);
    EXPECT_EQ(err.type, CmdArgParser::UNPROCESSED);
  }

  // Composes: cond=false + Exist - does not toggle the bool even when the tag is present.
  {
    auto parser = Make({"FLAG"});
    bool flag = false;
    parser.Apply(If(false, Exist("FLAG", &flag)));
    EXPECT_FALSE(flag);
  }
}

TEST_F(CmdArgParserTest, ApplyOneOf) {
  // OneOf groups mutually-exclusive options. Zero or one may match across the Apply loop.
  // A second match reports an error instead of being quietly accepted.

  // Zero matches — fine.
  {
    auto parser = Make({});
    bool nx = false, xx = false;
    parser.Apply(OneOf(Exist("NX", &nx), Exist("XX", &xx)));
    EXPECT_FALSE(nx);
    EXPECT_FALSE(xx);
    EXPECT_FALSE(parser.HasError());
  }

  // Single match — fine.
  {
    auto parser = Make({"NX"});
    bool nx = false, xx = false;
    parser.Apply(OneOf(Exist("NX", &nx), Exist("XX", &xx)));
    EXPECT_TRUE(nx);
    EXPECT_FALSE(xx);
    EXPECT_FALSE(parser.HasError());
  }

  // Two different members of the group match -> error.
  {
    auto parser = Make({"NX", "XX"});
    bool nx = false, xx = false;
    parser.Apply(OneOf(Exist("NX", &nx), Exist("XX", &xx)));
    auto err = parser.TakeError();
    EXPECT_TRUE(err);
    EXPECT_EQ(err.type, CmdArgParser::INVALID_CASES);
  }

  // Same member twice also counts as a second match -> error.
  {
    auto parser = Make({"NX", "NX"});
    bool nx = false, xx = false;
    parser.Apply(OneOf(Exist("NX", &nx), Exist("XX", &xx)));
    auto err = parser.TakeError();
    EXPECT_TRUE(err);
    EXPECT_EQ(err.type, CmdArgParser::INVALID_CASES);
  }

  // OneOf composes with other Apply options. Unrelated tags are not affected.
  {
    auto parser = Make({"NX", "COUNT", "5"});
    bool nx = false, xx = false;
    uint32_t count = 0;
    parser.Apply(OneOf(Exist("NX", &nx), Exist("XX", &xx)), Tag("COUNT", &count));
    EXPECT_TRUE(nx);
    EXPECT_EQ(count, 5u);
    EXPECT_FALSE(parser.HasError());
  }
}

TEST_F(CmdArgParserTest, ApplyOptional) {
  // Tag present -> optional engaged.
  {
    auto parser = Make({"COUNT", "5"});
    std::optional<uint32_t> count;
    parser.Apply(Tag("COUNT", &count));
    ASSERT_TRUE(count.has_value());
    EXPECT_EQ(*count, 5u);
    EXPECT_FALSE(parser.HasError());
  }
  // Tag absent -> optional stays empty.
  {
    auto parser = Make({});
    std::optional<uint32_t> count;
    parser.Apply(Tag("COUNT", &count));
    EXPECT_FALSE(count.has_value());
    EXPECT_FALSE(parser.HasError());
  }
  // Invalid value -> INVALID_INT reported. The optional's state on error is undefined; callers
  // must check for the parse error first.
  {
    auto parser = Make({"COUNT", "NAN"});
    std::optional<uint32_t> count;
    parser.Apply(Tag("COUNT", &count));
    auto err = parser.TakeError();
    EXPECT_TRUE(err);
    EXPECT_EQ(err.type, CmdArgParser::INVALID_INT);
  }
}

TEST_F(CmdArgParserTest, FixedRangeInt) {
  {
    auto parser = Make({"10", "-10", "12"});

    EXPECT_EQ((parser.Next<FInt<-11, 11>>().value), 10);
    EXPECT_EQ((parser.Next<FInt<-11, 11>>().value), -10);
    EXPECT_EQ((parser.Next<FInt<-11, 11>>().value), 0);

    auto err = parser.TakeError();
    EXPECT_TRUE(err);
    EXPECT_EQ(err.type, CmdArgParser::INVALID_INT);
    EXPECT_EQ(err.index, 2);
  }

  {
    auto parser = Make({"-12"});
    EXPECT_EQ((parser.Next<FInt<-11, 11>>().value), 0);

    auto err = parser.TakeError();
    EXPECT_TRUE(err);
    EXPECT_EQ(err.type, CmdArgParser::INVALID_INT);
    EXPECT_EQ(err.index, 0);
  }
}

}  // namespace facade
