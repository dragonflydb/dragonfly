// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "facade/cmd_arg_parser.h"

#include <absl/base/casts.h>
#include <benchmark/benchmark.h>
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

TEST_F(CmdArgParserTest, EmptyArgumentNormalization) {
  std::string_view args[] = {{}, {}, {}, {}, {}};
  ASSERT_EQ(args[0].data(), nullptr);

  ParsedArgs parsed{cmn::ArgSlice{args, std::size(args)}};
  CmdArgParser parser{parsed};
  EXPECT_NE(parser.CurrentUnchecked().data(), nullptr);

  std::string_view value;
  EXPECT_TRUE(parser.Check("", &value));
  EXPECT_NE(value.data(), nullptr);

  parser.ExpectTag("");
  parser.ExpectTag("", "expected empty argument");
  EXPECT_NE(parser.ExpectStartsWith("", "expected empty argument").data(), nullptr);
  EXPECT_TRUE(parser.Finalize());

  std::string_view arg;
  CmdArgParser invalid_prefix{ParsedArgs{cmn::ArgSlice{&arg, 1}}};
  EXPECT_NE(invalid_prefix.ExpectStartsWith("x", "missing prefix").data(), nullptr);
  EXPECT_TRUE(invalid_prefix.TakeError());
}

TEST_F(CmdArgParserTest, MultiNextErrorPreservesCursor) {
  auto parser = Make({"1", "bad", "also-bad"});

  auto values = parser.Next<int, int, int>();
  EXPECT_EQ(std::get<0>(values), 1);
  EXPECT_EQ(std::get<1>(values), 0);
  EXPECT_EQ(parser.UnparsedStart(), 3u);
  auto err = parser.TakeError();
  EXPECT_EQ(err.type, CmdArgParser::INVALID_INT);
  EXPECT_EQ(err.index, 1u);
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
  {
    auto parser = Make({"TAG", "11", "22", "TAG", "text"});

    int first;
    string_view second;
    EXPECT_TRUE(parser.Check("TAG", &first, &second));
    EXPECT_EQ(first, 11);
    EXPECT_EQ(second, "22");

    EXPECT_TRUE(parser.Check("TAG", &first, &second));
    auto err = parser.TakeError();
    EXPECT_TRUE(err);
    EXPECT_EQ(err.type, CmdArgParser::INVALID_INT);
    EXPECT_EQ(err.index, 4);
  }
  {
    auto parser = Make({"TAG", "11"});

    int first;
    string_view second;
    EXPECT_TRUE(parser.Check("TAG", &first, &second));

    auto err = parser.TakeError();
    EXPECT_TRUE(err);
    EXPECT_EQ(err.type, CmdArgParser::OUT_OF_BOUNDS);
    EXPECT_EQ(err.index, 2);
  }
  {
    auto parser = Make({"TAG"});

    int first;
    EXPECT_TRUE(parser.Check("TAG", &first));

    auto err = parser.TakeError();
    EXPECT_TRUE(err);
    EXPECT_EQ(err.type, CmdArgParser::OUT_OF_BOUNDS);
    EXPECT_EQ(err.index, 1);
  }
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

TEST_F(CmdArgParserTest, ReportBeforeAnyNext) {
  // Report(code) at cur_i_ == 0 must clamp the error index to 0 rather than underflow to SIZE_MAX.
  auto parser = Make({"x"});
  parser.Report(CmdArgParser::CUSTOM_ERROR);
  auto err = parser.TakeError();
  EXPECT_TRUE(err);
  EXPECT_EQ(err.index, 0u);
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

TEST_F(CmdArgParserTest, PositiveInt) {
  {
    auto parser = Make({"1", "42"});
    EXPECT_EQ(parser.Next<Positive<int64_t>>().value, 1);
    EXPECT_EQ(parser.Next<Positive<int64_t>>().value, 42);
    EXPECT_FALSE(parser.HasError());
  }

  {
    auto parser = Make({"0"});
    parser.Next<Positive<int64_t>>();
    EXPECT_EQ(parser.TakeError().type, CmdArgParser::INVALID_INT);
  }

  {
    auto parser = Make({"256"});
    parser.Next<Positive<uint8_t>>();
    EXPECT_EQ(parser.TakeError().type, CmdArgParser::INVALID_INT);
  }
}

TEST_F(CmdArgParserTest, NonNegativeInt) {
  {
    auto parser = Make({"0", "42"});
    EXPECT_EQ(parser.Next<NonNegativeInt<int64_t>>().value, 0);
    EXPECT_EQ(parser.Next<NonNegativeInt<int64_t>>().value, 42);
    EXPECT_FALSE(parser.HasError());
  }

  {
    auto parser = Make({"-1"});
    parser.Next<NonNegativeInt<int64_t>>();
    EXPECT_EQ(parser.TakeError().type, CmdArgParser::INVALID_INT);
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

// A token parser callable (string_view, RuleError&): converts one arg, custom error on miss.
constexpr char kBadUnit[] = "bad unit";
double ParseUnit(std::string_view sv, RuleError& err) {
  if (sv == "M")
    return 1.0;
  if (sv == "KM")
    return 1000.0;
  err = {true, kBadUnit};
  return -1.0;
}

TEST_F(CmdArgParserTest, ParserFunction) {
  {  // token form: fn(string_view, RuleError&) converts the next arg
    auto parser = Make({"KM", "M"});
    EXPECT_DOUBLE_EQ(parser.Next(ParseUnit), 1000.0);
    EXPECT_DOUBLE_EQ(parser.Next(ParseUnit), 1.0);
    EXPECT_FALSE(parser.HasError());
  }
  {
    auto parser = Make({"YARD"});
    EXPECT_DOUBLE_EQ(parser.Next(ParseUnit), 0.0);  // value-initialized on failure
    auto err = parser.TakeError();
    EXPECT_TRUE(err);
    EXPECT_EQ(err.type, CmdArgParser::CUSTOM_ERROR);
    EXPECT_EQ(err.MakeReply().ToSv(), kBadUnit);
  }
  {
    auto parser = Make({});  // framework surfaces OUT_OF_BOUNDS before calling fn
    EXPECT_DOUBLE_EQ(parser.Next(ParseUnit), 0.0);
    EXPECT_EQ(parser.TakeError().type, CmdArgParser::OUT_OF_BOUNDS);
  }
  {  // full form: fn(CmdArgParser*) can consume several args and return a compound type
    auto parser = Make({"3", "4"});
    auto point = [](CmdArgParser* p) {
      int x =
          p->Next<int>();  // sequence reads explicitly; argument evaluation order is unspecified
      return std::make_pair(x, p->Next<int>());
    };
    auto [x, y] = parser.Next(point);
    EXPECT_EQ(x, 3);
    EXPECT_EQ(y, 4);
    EXPECT_FALSE(parser.HasError());
  }
}

TEST_F(CmdArgParserTest, NumberParser) {
  // The Number<> default parser callable matches the built-in Next<T>() behavior and error kinds.
  {
    auto parser = Make({"42", "3.5"});
    EXPECT_EQ(parser.Next(Number<int>), 42);
    EXPECT_DOUBLE_EQ(parser.Next(Number<double>), 3.5);
    EXPECT_FALSE(parser.HasError());
  }
  {
    auto parser = Make({"notanint"});
    EXPECT_EQ(parser.Next(Number<int>), 0);
    EXPECT_EQ(parser.TakeError().type, CmdArgParser::INVALID_INT);
  }
  {
    auto parser = Make({"notafloat"});
    EXPECT_DOUBLE_EQ(parser.Next(Number<double>), 0.0);
    EXPECT_EQ(parser.TakeError().type, CmdArgParser::INVALID_FLOAT);
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

TEST_F(CmdArgParserTest, FinalizeUnexpected) {
  {  // all consumed -> success
    auto parser = Make({"NX"});
    EXPECT_TRUE(parser.Check("NX"));
    EXPECT_TRUE(parser.Finalize("Unsupported option: "));
    EXPECT_FALSE(parser.HasError());
  }
  {  // leftover + empty prefix -> generic UNPROCESSED
    auto parser = Make({"FOO"});
    EXPECT_FALSE(parser.Finalize());
    EXPECT_EQ(parser.TakeError().type, CmdArgParser::UNPROCESSED);
  }
  {  // leftover + prefix -> custom "<prefix><arg>" (raw case, first leftover arg)
    auto parser = Make({"a", "foo", "bar"});
    EXPECT_EQ(parser.Next(), "a");
    EXPECT_FALSE(parser.Finalize("Unsupported option: "));
    auto err = parser.TakeError();
    EXPECT_EQ(err.type, CmdArgParser::CUSTOM_ERROR);
    EXPECT_EQ(err.MakeReply().ToSv(), "Unsupported option: foo");
    EXPECT_EQ(err.index, 1u);  // points at the first leftover arg, not the consumed one
  }
}

namespace {
struct SetLike {
  std::string_view key;
  std::string_view value;
  uint16_t flags = 0;
  uint32_t mc = 0;
  uint32_t offset = 0;
  uint32_t count = 0;
  int64_t ttl = 0;
  bool ttl_set = false;
};
enum SetLikeFlags : uint16_t { kNx = 1 << 0, kXx = 1 << 1, kGet = 1 << 2 };

consteval auto MakeSetLikeGrammar() {
  return Compile(
      Args(&SetLike::key, &SetLike::value),
      Options(OneOf("NX and XX are incompatible", Flags(&SetLike::flags, "NX", kNx, "XX", kXx)),
              Action(
                  "EX",
                  +[](CmdArgParser* p, SetLike* o) {
                    o->ttl = p->Next<int64_t>();
                    o->ttl_set = true;
                  }),
              Flags(&SetLike::flags, "GET", kGet), Field("MCFLAGS", &SetLike::mc),
              Field("LIMIT", &SetLike::offset, &SetLike::count)));
}
}  // namespace

TEST_F(CmdArgParserTest, CapGrammar) {
  static constexpr auto kGrammar = MakeSetLikeGrammar();
  auto apply = [&](std::initializer_list<std::string_view> args, SetLike* o) {
    auto parser = Make(args);
    kGrammar.Apply(&parser, o);
    parser.Finalize();
    return parser.TakeError();
  };

  {
    SetLike o;
    EXPECT_FALSE(apply({"mykey", "myval", "NX", "GET"}, &o));
    EXPECT_EQ(o.key, "mykey");
    EXPECT_EQ(o.value, "myval");
    EXPECT_EQ(o.flags, kNx | kGet);
  }
  {
    SetLike o;
    EXPECT_FALSE(apply({"k", "v", "EX", "100"}, &o));
    EXPECT_TRUE(o.ttl_set);
    EXPECT_EQ(o.ttl, 100);
  }
  {
    SetLike o;
    EXPECT_FALSE(apply({"k", "v", "MCFLAGS", "42"}, &o));
    EXPECT_EQ(o.mc, 42u);
  }
  {
    SetLike o;
    EXPECT_FALSE(apply({"k", "v", "LIMIT", "2", "10"}, &o));
    EXPECT_EQ(o.offset, 2u);
    EXPECT_EQ(o.count, 10u);
  }
  {
    SetLike o;
    auto err = apply({"k", "v", "NX", "XX"}, &o);
    EXPECT_TRUE(err);
    EXPECT_EQ(err.MakeReply().ToSv(), "NX and XX are incompatible");
  }
  {  // the same alternative twice is also a conflict (OneOf rejects any second match, like Redis)
    SetLike o;
    auto err = apply({"k", "v", "NX", "NX"}, &o);
    EXPECT_TRUE(err);
    EXPECT_EQ(err.MakeReply().ToSv(), "NX and XX are incompatible");
  }
  {
    SetLike o;
    EXPECT_FALSE(apply({"k", "v"}, &o));
    EXPECT_EQ(o.key, "k");
    EXPECT_EQ(o.value, "v");
    EXPECT_EQ(o.flags, 0);
  }
  {
    SetLike o;
    EXPECT_TRUE(apply({"k"}, &o));
  }
  {
    SetLike o;
    auto err = apply({}, &o);
    EXPECT_EQ(err.type, CmdArgParser::OUT_OF_BOUNDS);
  }
  {
    SetLike o;
    auto err = apply({"k", "v", "LIMIT", "2"}, &o);
    EXPECT_EQ(err.type, CmdArgParser::OUT_OF_BOUNDS);
  }
  {
    SetLike o;
    EXPECT_TRUE(apply({"k", "v", "GET", "FOO"}, &o));
    EXPECT_EQ(o.flags, kGet);
  }
  {
    SetLike o;
    EXPECT_TRUE(apply({"k", "v", "EX", "notanint"}, &o));
  }
}

namespace {
struct HeadTail {
  std::string_view head;
  bool flag = false;
  std::string_view tail;
};
consteval auto MakeHeadTailGrammar() {
  return Compile(Args(&HeadTail::head), Options(Exist("F", &HeadTail::flag)),
                 Args(&HeadTail::tail));
}
}  // namespace

TEST_F(CmdArgParserTest, CapGrammarPositionalAfterOptions) {
  static constexpr auto kGrammar = MakeHeadTailGrammar();
  static constexpr auto kSkipGrammar = Compile(Skip(), Args(&HeadTail::tail));
  static constexpr auto kFallbackSkipGrammar =
      Compile(Options(Exist("F", &HeadTail::flag), Skip()));
  auto run = [&](std::initializer_list<std::string_view> args, HeadTail* o) {
    auto parser = Make(args);
    kGrammar.Apply(&parser, o);
    parser.Finalize();
    return parser.TakeError();
  };

  {
    HeadTail o;
    EXPECT_FALSE(run({"h", "F", "t"}, &o));
    EXPECT_EQ(o.head, "h");
    EXPECT_TRUE(o.flag);
    EXPECT_EQ(o.tail, "t");
  }
  {
    HeadTail o;
    EXPECT_FALSE(run({"h", "t"}, &o));
    EXPECT_EQ(o.head, "h");
    EXPECT_FALSE(o.flag);
    EXPECT_EQ(o.tail, "t");
  }
  {
    auto parser = Make({"ignored", "tail"});
    auto target = kSkipGrammar.Apply(&parser);
    EXPECT_TRUE(parser.Finalize());
    EXPECT_EQ(target.tail, "tail");
  }
  {
    auto parser = Make({"unknown", "F", "another"});
    auto target = kFallbackSkipGrammar.Apply(&parser);
    EXPECT_TRUE(parser.Finalize());
    EXPECT_TRUE(target.flag);
  }
}

namespace {
enum class Dir { kUnset, kAsc, kDesc };
struct MapIf {
  Dir dir = Dir::kAsc;
  Dir switched_dir = Dir::kUnset;
  bool read_only = false;
  std::string_view store;
};
consteval auto MakeMapIfGrammar() {
  return Compile(Options(Map(&MapIf::dir, "ASC", Dir::kAsc, "DESC", Dir::kDesc),
                         OneOf("direction conflict", Map(&MapIf::switched_dir, "UP", Dir::kAsc),
                               Map(&MapIf::switched_dir, "DOWN", Dir::kDesc)),
                         IfNot(&MapIf::read_only, Field("STORE", &MapIf::store))));
}
}  // namespace

TEST_F(CmdArgParserTest, CapGrammarMapIf) {
  static constexpr auto kGrammar = MakeMapIfGrammar();
  auto run = [&](std::initializer_list<std::string_view> args, MapIf* o) {
    auto parser = Make(args);
    kGrammar.Apply(&parser, o);
    parser.Finalize();
    return parser.TakeError();
  };

  {
    MapIf o;
    EXPECT_FALSE(run({"DESC"}, &o));
    EXPECT_EQ(o.dir, Dir::kDesc);
  }
  {
    MapIf o;
    EXPECT_FALSE(run({"ASC", "STORE", "dst"}, &o));
    EXPECT_EQ(o.dir, Dir::kAsc);
    EXPECT_EQ(o.store, "dst");
  }
  {
    MapIf o;
    o.read_only = true;
    EXPECT_TRUE(run({"STORE", "dst"}, &o));
    EXPECT_EQ(o.store, "");
  }
  {  // OneOf(Map) selects once and writes the mapped value
    MapIf o;
    EXPECT_FALSE(run({"DOWN"}, &o));
    EXPECT_EQ(o.switched_dir, Dir::kDesc);
  }
  {  // repeating the same option is now a conflict, matching Redis
    MapIf o;
    auto err = run({"DOWN", "DOWN"}, &o);
    EXPECT_EQ(err.MakeReply().ToSv(), "direction conflict");
  }
  {
    MapIf o;
    auto err = run({"DOWN", "UP"}, &o);
    EXPECT_EQ(err.MakeReply().ToSv(), "direction conflict");
  }
}

namespace {
struct IntoOptTarget {
  struct Limit {
    uint32_t offset = 0;
    uint32_t count = 0;
  };
  std::optional<Limit> limit;
  bool alpha = false;
};
consteval auto MakeIntoOptionalGrammar() {
  return Compile(Options(Exist("ALPHA", &IntoOptTarget::alpha),
                         Into(&IntoOptTarget::limit, Field("LIMIT", &IntoOptTarget::Limit::offset,
                                                           &IntoOptTarget::Limit::count))));
}
}  // namespace

TEST_F(CmdArgParserTest, CapGrammarIntoOptional) {
  static constexpr auto kGrammar = MakeIntoOptionalGrammar();
  auto run = [&](std::initializer_list<std::string_view> args, IntoOptTarget* o) {
    auto parser = Make(args);
    kGrammar.Apply(&parser, o);
    parser.Finalize();
    return parser.TakeError();
  };

  {  // keyword absent -> optional stays disengaged
    IntoOptTarget o;
    EXPECT_FALSE(run({"ALPHA"}, &o));
    EXPECT_TRUE(o.alpha);
    EXPECT_FALSE(o.limit.has_value());
  }
  {  // keyword present -> optional engaged and members read
    IntoOptTarget o;
    EXPECT_FALSE(run({"LIMIT", "2", "5"}, &o));
    ASSERT_TRUE(o.limit.has_value());
    EXPECT_EQ(o.limit->offset, 2u);
    EXPECT_EQ(o.limit->count, 5u);
  }
  {  // repeated keyword -> last wins, still a single engaged value
    IntoOptTarget o;
    EXPECT_FALSE(run({"LIMIT", "1", "1", "LIMIT", "3", "4"}, &o));
    ASSERT_TRUE(o.limit.has_value());
    EXPECT_EQ(o.limit->offset, 3u);
    EXPECT_EQ(o.limit->count, 4u);
  }
}

TEST_F(CmdArgParserTest, CapTagMatch) {
  using cap_detail::TagMatch;
  EXPECT_TRUE(TagMatch("EX", "EX"));
  EXPECT_TRUE(TagMatch("ex", "EX"));
  EXPECT_TRUE(TagMatch("Ex", "eX"));
  EXPECT_FALSE(TagMatch("E", "EX"));
  EXPECT_FALSE(TagMatch("EXX", "EX"));
  EXPECT_FALSE(TagMatch("EY", "EX"));
  EXPECT_FALSE(TagMatch("", "EX"));
  EXPECT_TRUE(TagMatch("", ""));
  EXPECT_FALSE(TagMatch(std::string_view{"E\0", 2}, "EX"));
}

namespace {
enum class Agg { kSum, kMin, kMax };
struct ChoiceTarget {
  Agg agg = Agg::kSum;
};
consteval auto MakeChoiceGrammar() {
  return Compile(Options(Choice("AGGREGATE", &ChoiceTarget::agg, "SUM", Agg::kSum, "MIN", Agg::kMin,
                                "MAX", Agg::kMax)));
}
}  // namespace

TEST_F(CmdArgParserTest, CapGrammarChoice) {
  static constexpr auto kGrammar = MakeChoiceGrammar();
  auto run = [&](std::initializer_list<std::string_view> args, ChoiceTarget* o) {
    auto parser = Make(args);
    kGrammar.Apply(&parser, o);
    parser.Finalize();
    return parser.TakeError();
  };

  {
    ChoiceTarget o;
    EXPECT_FALSE(run({"AGGREGATE", "MAX"}, &o));
    EXPECT_EQ(o.agg, Agg::kMax);
  }
  {
    ChoiceTarget o;
    EXPECT_FALSE(run({"aggregate", "min"}, &o));
    EXPECT_EQ(o.agg, Agg::kMin);
  }
  {
    ChoiceTarget o;
    EXPECT_TRUE(run({"AGGREGATE", "BOGUS"}, &o));
  }
  {
    ChoiceTarget o;
    EXPECT_TRUE(run({"AGGREGATE"}, &o));
  }
}

// TagValue: a keyword sets a discriminant member to a compile-time value and reads the next arg
// into another member (e.g. the EX/PX expiry keywords). Wrapped in OneOf for mutual exclusion.
namespace {
enum class Unit { kSec, kMs };
struct TvTarget {
  Unit unit = Unit::kSec;
  std::optional<int64_t> amount;
};
consteval auto MakeTagValueGrammar() {
  return Compile(Options(OneOf("", TagValue("EX", &TvTarget::unit, Unit::kSec, &TvTarget::amount),
                               TagValue("PX", &TvTarget::unit, Unit::kMs, &TvTarget::amount))));
}
}  // namespace

TEST_F(CmdArgParserTest, CapGrammarTagValue) {
  static constexpr auto kGrammar = MakeTagValueGrammar();
  auto run = [&](std::initializer_list<std::string_view> args, TvTarget* o) {
    auto parser = Make(args);
    kGrammar.Apply(&parser, o);
    parser.Finalize();
    return parser.TakeError();
  };

  {  // sets both the unit discriminant and the amount
    TvTarget o;
    EXPECT_FALSE(run({"PX", "1500"}, &o));
    EXPECT_EQ(o.unit, Unit::kMs);
    EXPECT_EQ(o.amount, 1500);
  }
  {  // a second (different) option conflicts
    TvTarget o;
    EXPECT_TRUE(run({"EX", "1", "PX", "2"}, &o));
  }
  {  // repeating the same option also conflicts
    TvTarget o;
    EXPECT_TRUE(run({"EX", "1", "EX", "2"}, &o));
  }
  {  // missing value -> error
    TvTarget o;
    EXPECT_TRUE(run({"EX"}, &o));
  }
}

namespace {

enum BenchFlags : uint16_t { kBNx = 1 << 0, kBXx = 1 << 1, kBGet = 1 << 2 };
enum class BenchUnit : uint8_t { kNone, kSec, kMs };
enum class BenchSelection : uint8_t { kUnset, kFirst, kSecond };

struct BenchNested {
  uint32_t value = 0;
};

// One member per cap rule so the benchmark grammar exercises every primitive exactly once.
struct BenchTarget {
  std::string_view head;
  std::string_view tail;
  bool one = false;
  bool one_alt = false;
  bool if_enabled = true;
  bool if_disabled = false;
  bool if_seen = false;
  bool if_not_seen = false;
  uint16_t flags = 0;
  uint32_t field = 0;
  uint32_t pair_first = 0;
  uint32_t pair_second = 0;
  uint32_t positive = 0;
  uint32_t action = 0;
  int64_t ttl = 0;
  int64_t positive_ttl = 0;
  BenchUnit unit = BenchUnit::kNone;
  BenchUnit positive_unit = BenchUnit::kNone;
  BenchSelection mapped = BenchSelection::kUnset;
  BenchSelection choice = BenchSelection::kUnset;
  BenchNested nested;
  std::optional<BenchNested> opt_nested;
};

void ParseBenchAction(CmdArgParser* parser, BenchTarget* target) {
  target->action += parser->Next<uint32_t>();
}

// Uses every cap rule once: Args, Options with a reserved tail, OneOf, Exist, Field, multi-member
// Field, Field<Parsed>, Action, TagValue, TagValue<Parsed>, Map, Flags, Choice, If, IfNot, Into
// into a plain and an optional member, and a Skip fallback.
consteval auto MakeBenchGrammar() {
  return Compile(
      Args(&BenchTarget::head),
      Options(1,
              OneOf("", Exist("ONE", &BenchTarget::one), Exist("ONE_ALT", &BenchTarget::one_alt)),
              Field("FIELD", &BenchTarget::field),
              Field("PAIR", &BenchTarget::pair_first, &BenchTarget::pair_second),
              Field<Positive<uint32_t>>("POSITIVE", &BenchTarget::positive, "positive"),
              Action("ACTION", ParseBenchAction),
              TagValue("TV", &BenchTarget::unit, BenchUnit::kSec, &BenchTarget::ttl),
              TagValue<Positive<int64_t>>("TVP", &BenchTarget::positive_unit, BenchUnit::kMs,
                                          &BenchTarget::positive_ttl),
              Map(&BenchTarget::mapped, "MAP", BenchSelection::kFirst),
              Flags(&BenchTarget::flags, "FLAG", kBNx),
              Choice("CHOICE", &BenchTarget::choice, "FIRST", BenchSelection::kFirst, "SECOND",
                     BenchSelection::kSecond),
              If(&BenchTarget::if_enabled, Exist("IF", &BenchTarget::if_seen)),
              IfNot(&BenchTarget::if_disabled, Exist("IF_NOT", &BenchTarget::if_not_seen)),
              Into(&BenchTarget::nested, Field("NESTED", &BenchNested::value)),
              Into(&BenchTarget::opt_nested, Field("OPT_NESTED", &BenchNested::value)), Skip()),
      Args(&BenchTarget::tail));
}

cmn::BackedArguments MakeStorage(std::initializer_list<std::string_view> args) {
  std::vector<std::string_view> v(args);
  cmn::BackedArguments s;
  s.Assign(v.begin(), v.end(), v.size());
  return s;
}

// The single input that exercises every rule of MakeBenchGrammar once (SKIP hits the Skip
// fallback).
CmdArgParser BenchArgs() {
  static const cmn::BackedArguments kArgs = MakeStorage(
      {"head",   "ONE", "FIELD",  "1",      "PAIR", "2",          "3",   "POSITIVE", "4",
       "ACTION", "5",   "TV",     "6",      "TVP",  "7",          "MAP", "FLAG",     "CHOICE",
       "FIRST",  "IF",  "IF_NOT", "NESTED", "8",    "OPT_NESTED", "9",   "SKIP",     "tail"});
  return CmdArgParser{kArgs};
}

}  // namespace

TEST_F(CmdArgParserTest, BenchGrammar) {
  static constexpr auto kGrammar = MakeBenchGrammar();
  CmdArgParser parser = BenchArgs();
  BenchTarget t = kGrammar.Apply(&parser);
  EXPECT_TRUE(parser.Finalize());
  EXPECT_EQ(t.head, "head");
  EXPECT_EQ(t.tail, "tail");
  EXPECT_TRUE(t.one);
  EXPECT_EQ(t.field, 1u);
  EXPECT_EQ(t.pair_first, 2u);
  EXPECT_EQ(t.pair_second, 3u);
  EXPECT_EQ(t.positive, 4u);
  EXPECT_EQ(t.action, 5u);
  EXPECT_EQ(t.ttl, 6);
  EXPECT_EQ(t.positive_ttl, 7);
  EXPECT_EQ(t.mapped, BenchSelection::kFirst);
  EXPECT_EQ(t.flags, kBNx);
  EXPECT_EQ(t.choice, BenchSelection::kFirst);
  EXPECT_TRUE(t.if_seen);
  EXPECT_TRUE(t.if_not_seen);
  EXPECT_EQ(t.nested.value, 8u);
  ASSERT_TRUE(t.opt_nested.has_value());
  EXPECT_EQ(t.opt_nested->value, 9u);
}

// The same grammar parsed three ways: the cap grammar, a raw index-based hand parser, and a hand
// parser using the CmdArgParser navigation API. All three produce the same BenchTarget.
static void BM_CapParse(benchmark::State& state) {
  CmdArgParser proto = BenchArgs();
  static constexpr auto kGrammar = MakeBenchGrammar();
  for (auto _ : state) {
    CmdArgParser parser = proto;
    BenchTarget t = kGrammar.Apply(&parser);
    benchmark::DoNotOptimize(parser.Finalize());
    benchmark::DoNotOptimize(t);
  }
}
BENCHMARK(BM_CapParse);

static void BM_ManualParse(benchmark::State& state) {
  CmdArgParser proto = BenchArgs();
  for (auto _ : state) {
    CmdArgParser parser = proto;
    ParsedArgs args = parser.UnparsedArgs();
    BenchTarget t;
    bool ok = true;
    size_t n = args.size();
    t.head = args[0];
    t.tail = args[n - 1];
    for (size_t i = 1; i + 1 < n; ++i) {  // leave the last arg for tail, like Options(1, ...)
      std::string_view a = args[i];
      if (absl::EqualsIgnoreCase(a, "ONE"))
        t.one = true;
      else if (absl::EqualsIgnoreCase(a, "ONE_ALT"))
        t.one_alt = true;
      else if (absl::EqualsIgnoreCase(a, "FIELD"))
        ok = ++i < n && absl::SimpleAtoi(args[i], &t.field);
      else if (absl::EqualsIgnoreCase(a, "PAIR")) {
        ok = i + 2 < n && absl::SimpleAtoi(args[i + 1], &t.pair_first) &&
             absl::SimpleAtoi(args[i + 2], &t.pair_second);
        i += 2;
      } else if (absl::EqualsIgnoreCase(a, "POSITIVE"))
        ok = ++i < n && absl::SimpleAtoi(args[i], &t.positive) && t.positive >= 1;
      else if (absl::EqualsIgnoreCase(a, "ACTION")) {
        uint32_t v = 0;
        ok = ++i < n && absl::SimpleAtoi(args[i], &v);
        t.action += v;
      } else if (absl::EqualsIgnoreCase(a, "TV")) {
        t.unit = BenchUnit::kSec;
        ok = ++i < n && absl::SimpleAtoi(args[i], &t.ttl);
      } else if (absl::EqualsIgnoreCase(a, "TVP")) {
        t.positive_unit = BenchUnit::kMs;
        ok = ++i < n && absl::SimpleAtoi(args[i], &t.positive_ttl) && t.positive_ttl >= 1;
      } else if (absl::EqualsIgnoreCase(a, "MAP"))
        t.mapped = BenchSelection::kFirst;
      else if (absl::EqualsIgnoreCase(a, "FLAG"))
        t.flags |= kBNx;
      else if (absl::EqualsIgnoreCase(a, "CHOICE")) {
        if (++i >= n)
          ok = false;
        else if (absl::EqualsIgnoreCase(args[i], "FIRST"))
          t.choice = BenchSelection::kFirst;
        else if (absl::EqualsIgnoreCase(args[i], "SECOND"))
          t.choice = BenchSelection::kSecond;
        else
          ok = false;
      } else if (t.if_enabled && absl::EqualsIgnoreCase(a, "IF"))
        t.if_seen = true;
      else if (!t.if_disabled && absl::EqualsIgnoreCase(a, "IF_NOT"))
        t.if_not_seen = true;
      else if (absl::EqualsIgnoreCase(a, "NESTED"))
        ok = ++i < n && absl::SimpleAtoi(args[i], &t.nested.value);
      else if (absl::EqualsIgnoreCase(a, "OPT_NESTED")) {
        uint32_t v = 0;
        ok = ++i < n && absl::SimpleAtoi(args[i], &v);
        t.opt_nested = BenchNested{v};
      }
      // else: unknown token skipped, matching the Skip() fallback
    }
    benchmark::DoNotOptimize(ok);
    benchmark::DoNotOptimize(t);
  }
}
BENCHMARK(BM_ManualParse);

static void BM_ManualWithParser(benchmark::State& state) {
  CmdArgParser proto = BenchArgs();
  for (auto _ : state) {
    CmdArgParser parser = proto;
    BenchTarget t;
    t.head = parser.Next();
    while (parser.HasAtLeast(2)) {  // leave one trailing arg for tail, like Options(1, ...)
      if (parser.Check("ONE"))
        t.one = true;
      else if (parser.Check("ONE_ALT"))
        t.one_alt = true;
      else if (parser.Check("FIELD"))
        t.field = parser.Next<uint32_t>();
      else if (parser.Check("PAIR"))
        std::tie(t.pair_first, t.pair_second) = parser.Next<uint32_t, uint32_t>();
      else if (parser.Check("POSITIVE"))
        t.positive = parser.Next<Positive<uint32_t>>("positive");
      else if (parser.Check("ACTION"))
        t.action += parser.Next<uint32_t>();
      else if (parser.Check("TV")) {
        t.unit = BenchUnit::kSec;
        t.ttl = parser.Next<int64_t>();
      } else if (parser.Check("TVP")) {
        t.positive_unit = BenchUnit::kMs;
        t.positive_ttl = parser.Next<Positive<int64_t>>();
      } else if (parser.Check("MAP"))
        t.mapped = BenchSelection::kFirst;
      else if (parser.Check("FLAG"))
        t.flags |= kBNx;
      else if (parser.Check("CHOICE")) {
        std::string_view v = parser.Next();
        if (absl::EqualsIgnoreCase(v, "FIRST"))
          t.choice = BenchSelection::kFirst;
        else if (absl::EqualsIgnoreCase(v, "SECOND"))
          t.choice = BenchSelection::kSecond;
        else
          parser.Report(CmdArgParser::INVALID_CASES);
      } else if (t.if_enabled && parser.Check("IF"))
        t.if_seen = true;
      else if (!t.if_disabled && parser.Check("IF_NOT"))
        t.if_not_seen = true;
      else if (parser.Check("NESTED"))
        t.nested.value = parser.Next<uint32_t>();
      else if (parser.Check("OPT_NESTED"))
        t.opt_nested = BenchNested{parser.Next<uint32_t>()};
      else
        parser.Skip(1);  // Skip() fallback: consume an unknown token
    }
    t.tail = parser.Next();
    benchmark::DoNotOptimize(parser.Finalize());
    benchmark::DoNotOptimize(t);
  }
}
BENCHMARK(BM_ManualWithParser);

}  // namespace facade
