// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "facade/cmd_arg_parser.h"

#include <absl/base/casts.h>
#include <gmock/gmock.h>

#include <cmath>

#include "facade/error.h"
#include "facade/memcache_parser.h"

using namespace testing;
using namespace std;

namespace facade {

class CmdArgParserTest : public testing::Test {
 public:
  CmdArgParser Make(absl::Span<const std::string_view> args) {
    storage_.Assign(args.begin(), args.end(), args.size());
    return CmdArgParser{storage_};
  }

 private:
  CmdArgVec arg_vec_;
  cmn::BackedArguments storage_;
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

TEST_F(CmdArgParserTest, ValidatedRules) {
  static constexpr char kOverflow[] = "overflow";
  static constexpr char kNaN[] = "not a number";
  static constexpr char kNonFinite[] = "not finite";

  // NotEq: rejects only the sentinel value, with a custom message.
  using NoMin = Validated<int64_t, NotEq<INT64_MIN, kOverflow>>;
  {
    auto parser = Make({"5"});
    EXPECT_EQ(static_cast<int64_t>(parser.Next<NoMin>()), 5);
    EXPECT_FALSE(parser.HasError());
  }
  {
    auto parser = Make({"-9223372036854775808"});  // INT64_MIN
    parser.Next<NoMin>();
    auto err = parser.TakeError();
    EXPECT_EQ(err.type, CmdArgParser::CUSTOM_ERROR);
    EXPECT_EQ(err.MakeReply().ToSv(), "overflow");
  }
  {  // A malformed number reports the generic INVALID_INT, not the rule message.
    auto parser = Make({"abc"});
    parser.Next<NoMin>();
    EXPECT_EQ(parser.TakeError().type, CmdArgParser::INVALID_INT);
  }

  // NotNan accepts +/-inf but rejects NaN; Finite rejects both.
  {
    auto parser = Make({"inf", "nan"});
    EXPECT_TRUE(std::isinf(static_cast<double>(parser.Next<Validated<double, NotNan<kNaN>>>())));
    EXPECT_FALSE(parser.HasError());
    parser.Next<Validated<double, NotNan<kNaN>>>();
    EXPECT_EQ(parser.TakeError().MakeReply().ToSv(), "not a number");
  }
  {
    auto parser = Make({"inf"});
    parser.Next<Validated<double, Finite<kNonFinite>>>();
    EXPECT_EQ(parser.TakeError().MakeReply().ToSv(), "not finite");
  }

  // Composition: rules run in order, first non-empty message wins (NotNan rejects NaN first).
  {
    auto parser = Make({"nan"});
    parser.Next<Validated<double, NotNan<kNaN>, Finite<kNonFinite>>>();
    EXPECT_EQ(parser.TakeError().MakeReply().ToSv(), "not a number");
  }

  // The rules are reusable to validate a value parsed by other means (e.g. NextWithPrefix).
  {
    auto parser = Make({"#-9223372036854775808"});
    bool prefixed = false;
    int64_t off = parser.NextWithPrefix<int64_t>("#", &prefixed);
    EXPECT_TRUE(prefixed);
    if (auto e = NotEq<INT64_MIN, kOverflow>(off); e.failed)
      parser.ReportCustom(std::string{e.msg});
    EXPECT_EQ(parser.TakeError().MakeReply().ToSv(), "overflow");
  }
}

TEST_F(CmdArgParserTest, BoundedRule) {
  static constexpr char kMsg[] = "out of range";

  // Integer range: in-range passes; below/above report the custom message; a non-integer stays
  // generic INVALID_INT.
  using Pct = Validated<int, Bounded<0, 100, kMsg>>;
  {
    auto parser = Make({"0", "100", "50"});
    EXPECT_EQ(static_cast<int>(parser.Next<Pct>()), 0);
    EXPECT_EQ(static_cast<int>(parser.Next<Pct>()), 100);
    EXPECT_EQ(static_cast<int>(parser.Next<Pct>()), 50);
    EXPECT_FALSE(parser.HasError());
  }
  {
    auto parser = Make({"101"});
    parser.Next<Pct>();
    EXPECT_EQ(parser.TakeError().MakeReply().ToSv(), "out of range");
  }
  {
    auto parser = Make({"abc"});
    parser.Next<Pct>();
    EXPECT_EQ(parser.TakeError().type, CmdArgParser::INVALID_INT);
  }
}

TEST_F(CmdArgParserTest, NonNegativeRule) {
  static constexpr char kNeg[] = "must not be negative";
  using NonNeg = Validated<float, NonNegative<kNeg>>;

  {
    auto parser = Make({"0", "3.5"});
    EXPECT_FLOAT_EQ(static_cast<float>(parser.Next<NonNeg>()), 0.0f);
    EXPECT_FLOAT_EQ(static_cast<float>(parser.Next<NonNeg>()), 3.5f);
    EXPECT_FALSE(parser.HasError());
  }
  {
    auto parser = Make({"-0.1"});
    parser.Next<NonNeg>();
    EXPECT_EQ(parser.TakeError().MakeReply().ToSv(), "must not be negative");
  }
  {  // NaN is accepted (matches a plain `v < 0` guard); a non-float stays generic INVALID_FLOAT.
    auto parser = Make({"nan"});
    parser.Next<NonNeg>();
    EXPECT_FALSE(parser.HasError());
    auto p2 = Make({"abc"});
    p2.Next<NonNeg>();
    EXPECT_EQ(p2.TakeError().type, CmdArgParser::INVALID_FLOAT);
  }
}

TEST_F(CmdArgParserTest, NextWithMessageOverride) {
  static constexpr char kParseMsg[] = "custom parse error";
  static constexpr char kRuleMsg[] = "must not be negative";
  using NonNeg = Validated<float, NonNegative<kRuleMsg>>;

  {  // A non-numeric value reports the caller message instead of the generic type error.
    auto parser = Make({"abc"});
    parser.Next<NonNeg>(kParseMsg);
    EXPECT_EQ(parser.TakeError().MakeReply().ToSv(), "custom parse error");
  }
  {  // A rule violation keeps its own message; the caller message does not override it.
    auto parser = Make({"-1"});
    parser.Next<NonNeg>(kParseMsg);
    EXPECT_EQ(parser.TakeError().MakeReply().ToSv(), "must not be negative");
  }
  {  // FInt out-of-range is a generic error, so the caller message applies to it too.
    auto parser = Make({"5"});
    parser.Next<FInt<0, 3>>(kParseMsg);
    EXPECT_EQ(parser.TakeError().MakeReply().ToSv(), "custom parse error");
  }
  {  // A valid value produces no error.
    auto parser = Make({"2.5"});
    EXPECT_FLOAT_EQ(static_cast<float>(parser.Next<NonNeg>(kParseMsg)), 2.5f);
    EXPECT_FALSE(parser.HasError());
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

// A user-defined validated number: VNum<double> + validate() reporting the generic INVALID_FLOAT.
struct PositiveFinite : VNum<double> {
  static RuleError validate(double v) {
    return {!(v > 0 && std::isfinite(v)), {}};
  }
};

TEST_F(CmdArgParserTest, ValidatedDouble) {
  {
    auto parser = Make({"0.5", "2.5"});
    EXPECT_DOUBLE_EQ((parser.Next<PositiveFinite>().value), 0.5);
    EXPECT_DOUBLE_EQ((parser.Next<PositiveFinite>().value), 2.5);
    EXPECT_FALSE(parser.HasError());
  }
  {
    auto parser = Make({"0"});  // validate() rejects 0
    parser.Next<PositiveFinite>();
    auto err = parser.TakeError();
    EXPECT_TRUE(err);
    EXPECT_EQ(err.type, CmdArgParser::INVALID_FLOAT);
  }
  {
    auto parser = Make({"inf"});  // non-finite rejected
    parser.Next<PositiveFinite>();
    auto err = parser.TakeError();
    EXPECT_TRUE(err);
    EXPECT_EQ(err.type, CmdArgParser::INVALID_FLOAT);
  }
}

TEST_F(CmdArgParserTest, RangeList) {
  using Range = CmdArgParser::Range;
  // NextRange reads [count, e1..eN] and returns a bounded Range; a terminal Range converts to
  // ParsedArgs.
  {
    auto parser = Make({"2", "a", "b"});
    Range fields = parser.NextRange();
    EXPECT_EQ(fields.size(), 2u);
    EXPECT_THAT(std::vector<string_view>(fields.begin(), fields.end()), ElementsAre("a", "b"));
    EXPECT_FALSE(parser.HasError());
    EXPECT_FALSE(parser.HasNext());
    ParsedArgs as_args = fields;  // terminal -> ParsedArgs
    EXPECT_EQ(as_args.size(), 2u);
  }
  // group=2 reads count field/value pairs.
  {
    auto parser = Make({"2", "f1", "v1", "f2", "v2"});
    Range kv = parser.NextRange(2);
    EXPECT_EQ(kv.size(), 4u);
    EXPECT_THAT(std::vector<string_view>(kv.begin(), kv.end()),
                ElementsAre("f1", "v1", "f2", "v2"));
    EXPECT_FALSE(parser.HasError());
  }
  // Bounded (non-terminal): only `count` elements are consumed; the rest stays for the caller.
  {
    auto parser = Make({"2", "k1", "k2", "WEIGHTS", "1", "2"});
    Range keys = parser.NextRange();
    EXPECT_EQ(keys.size(), 2u);
    EXPECT_THAT(std::vector<string_view>(keys.begin(), keys.end()), ElementsAre("k1", "k2"));
    EXPECT_FALSE(parser.HasError());
    EXPECT_EQ(parser.Peek(), "WEIGHTS");  // trailing clause preserved
  }
  // count == 0 -> error.
  {
    auto parser = Make({"0"});
    parser.NextRange();
    EXPECT_TRUE(parser.TakeError());
  }
  // Fewer than count args remain -> INVALID_CASES.
  {
    auto parser = Make({"2", "a"});
    parser.NextRange();
    auto err = parser.TakeError();
    EXPECT_TRUE(err);
    EXPECT_EQ(err.type, CmdArgParser::INVALID_CASES);
  }
  // A bad/zero count falls back to size_err unless count_err is passed.
  {
    auto parser = Make({"0", "a"});  // zero count -> size_err
    parser.NextRange(1, "bad size", false);
    EXPECT_EQ(parser.TakeError().MakeReply().ToSv(), "bad size");
  }
  {
    auto parser = Make({"0", "a"});  // zero count -> count_err when given
    parser.NextRange(1, "bad size", false, "bad count");
    EXPECT_EQ(parser.TakeError().MakeReply().ToSv(), "bad count");
  }
  {
    auto parser = Make({"3", "a"});  // valid count, too few args -> size_err
    parser.NextRange(1, "bad size", false);
    EXPECT_EQ(parser.TakeError().MakeReply().ToSv(), "bad size");
  }
  // consume_all=true: the range must cover all remaining args, so trailing args are a mismatch.
  {
    auto parser = Make({"1", "a", "b"});  // count=1 but 2 args remain
    parser.NextRange(1, "too many", true);
    EXPECT_EQ(parser.TakeError().MakeReply().ToSv(), "too many");
  }
  {
    auto parser = Make({"2", "a", "b"});  // exact match -> ok, all consumed
    Range r = parser.NextRange(1, "size", true);
    EXPECT_FALSE(parser.HasError());
    EXPECT_EQ(r.size(), 2u);
    EXPECT_FALSE(parser.HasNext());
  }
  // consume_all=false: only `count` elements are consumed; the rest stays for the caller.
  {
    auto parser = Make({"1", "a", "b"});
    Range r = parser.NextRange(1, "size", false);
    EXPECT_FALSE(parser.HasError());
    EXPECT_EQ(r.size(), 1u);
    EXPECT_EQ(parser.Peek(), "b");
  }
  // RemainingRange: all remaining args, no leading count.
  {
    auto parser = Make({"a", "b", "c"});
    Range rest = parser.RemainingRange();
    EXPECT_EQ(rest.size(), 3u);
    EXPECT_THAT(std::vector<string_view>(rest.begin(), rest.end()), ElementsAre("a", "b", "c"));
    EXPECT_FALSE(parser.HasNext());
  }
  // RemainingRange with empty_err: reports the message only when no args remain.
  {
    auto parser = Make({"a"});
    Range rest = parser.RemainingRange("need args");
    EXPECT_EQ(rest.size(), 1u);
    EXPECT_FALSE(parser.HasError());
  }
  {
    auto parser = Make({"x"});
    parser.Next();  // consume the only arg
    parser.RemainingRange("need args");
    EXPECT_EQ(parser.TakeError().MakeReply().ToSv(), "need args");
  }
  {  // A prior error is preserved (empty_err does not overwrite it).
    auto parser = Make({"x"});
    parser.Next<int>();  // "x" is not an int -> INVALID_INT
    parser.RemainingRange("need args");
    EXPECT_EQ(parser.TakeError().MakeReply().ToSv(), kInvalidIntErr);
  }
}

TEST_F(CmdArgParserTest, BackedArguments) {
  cmn::BackedArguments bargs;
  string_view args[] = {"SET", "mykey", "42", "EX", "100"};
  bargs.Assign(std::begin(args), std::end(args), 5);

  // Full range
  {
    CmdArgParser parser(bargs);
    EXPECT_EQ(parser.Next(), "SET");
    EXPECT_EQ(parser.Next(), "mykey");
    EXPECT_EQ(parser.Next<int>(), 42);
    EXPECT_TRUE(parser.Check("EX"));
    EXPECT_EQ(parser.Next<int>(), 100);
    EXPECT_TRUE(parser.Finalize());
  }

  // With offset (skip command name)
  {
    CmdArgParser parser(bargs, 1);
    EXPECT_EQ(parser.Next(), "mykey");
    EXPECT_EQ(parser.Next<int>(), 42);
    EXPECT_EQ(parser.UnparsedStart(), 2u);
    EXPECT_TRUE(parser.HasAtLeast(2));
    EXPECT_FALSE(parser.HasAtLeast(3));
    parser.Skip(2);
    EXPECT_TRUE(parser.Finalize());
  }
}

}  // namespace facade
