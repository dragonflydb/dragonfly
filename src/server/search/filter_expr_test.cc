// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <cmath>
#include <limits>

#include "base/gtest.h"
#include "server/search/aggregator.h"
#include "server/search/doc_index.h"
#include "server/search/filter_driver.h"
#include "server/search/filter_eval.h"

namespace dfly::aggregate {

using namespace std::string_literals;

// Helpers

// Parse an expression and evaluate it against a single document.
// Returns the raw Value result.
Value Eval(std::string_view expr, const DocValues& doc) {
  auto parsed = ParseFilterExpr(expr);
  EXPECT_TRUE(std::holds_alternative<FilterExpr>(parsed))
      << "Parse error: "
      << (std::holds_alternative<std::string>(parsed) ? std::get<std::string>(parsed) : "");
  if (!std::holds_alternative<FilterExpr>(parsed))
    return {};
  return EvalFilterExpr(*std::get<FilterExpr>(parsed), doc);
}

// Convenience: evaluate to bool.
bool EvalBool(std::string_view expr, const DocValues& doc) {
  return IsTruthy(Eval(expr, doc));
}

// Convenience: returns true if parsing succeeds.
bool ParseOk(std::string_view expr) {
  return std::holds_alternative<FilterExpr>(ParseFilterExpr(expr));
}

// Parser tests

TEST(FilterExprTest, ParseError) {
  // Missing right-hand side
  EXPECT_FALSE(ParseOk("@a =="));
  // Single '='
  EXPECT_FALSE(ParseOk("@a = 1"));
  // Single '&'
  EXPECT_FALSE(ParseOk("@a & @b"));
  // Unclosed paren
  EXPECT_FALSE(ParseOk("(@a == 1"));
  // Empty input should fail
  EXPECT_FALSE(ParseOk(""));
}

TEST(FilterExprTest, ParseSuccess) {
  EXPECT_TRUE(ParseOk("@a == 1"));
  EXPECT_TRUE(ParseOk("@a < 0.8 && @b == 'x'"));
  EXPECT_TRUE(ParseOk("(@a || @b) && @c"));
  EXPECT_TRUE(ParseOk("!@a"));
  EXPECT_TRUE(ParseOk("lower(@name) == 'hello'"));
  EXPECT_TRUE(ParseOk("@a ^ 2 < 10"));
}

TEST(FilterExprTest, InfKeyword) {
  DocValues doc{{"val", 0.5}};
  // lowercase inf
  EXPECT_TRUE(EvalBool("@val < inf", doc));
  EXPECT_FALSE(EvalBool("@val > inf", doc));
  // uppercase INF -- must also be recognised as number, not ident
  EXPECT_TRUE(EvalBool("@val < INF", doc));
  // mixed case
  EXPECT_TRUE(EvalBool("@val < Inf", doc));
  // negative infinity via unary minus
  EXPECT_FALSE(EvalBool("@val < -inf", doc));
}

TEST(FilterExprTest, EscapedStringLiterals) {
  DocValues doc{{"msg", "hello\nworld"}};
  // \n inside single-quoted string
  EXPECT_TRUE(EvalBool("@msg == 'hello\\nworld'", doc));
  // double-quoted string
  EXPECT_TRUE(EvalBool("@msg == \"hello\\nworld\"", doc));
  // escaped backslash
  DocValues doc2{{"path", "a\\b"}};
  EXPECT_TRUE(EvalBool("@path == 'a\\\\b'", doc2));
}

// Evaluator: leaf nodes

TEST(FilterExprTest, FieldRef) {
  DocValues doc{{"score", 0.5}, {"tag", "sports"}};
  EXPECT_EQ(Eval("@score", doc), Value(0.5));
  EXPECT_EQ(Eval("@tag", doc), Value("sports"s));
  // Missing field -> monostate -> falsy
  EXPECT_FALSE(EvalBool("@missing", doc));
}

TEST(FilterExprTest, NumericLiteral) {
  DocValues doc;
  EXPECT_EQ(Eval("3.14", doc), Value(3.14));
  EXPECT_EQ(Eval("-1", doc), Value(-1.0));
  EXPECT_EQ(Eval("0", doc), Value(0.0));
  // Verify non-trivial numeric literals are parsed correctly (not silently becoming 0.0).
  EXPECT_EQ(Eval("42", doc), Value(42.0));
  EXPECT_EQ(Eval("1e5", doc), Value(100000.0));
  EXPECT_EQ(Eval(".5", doc), Value(0.5));
  EXPECT_EQ(Eval("1.23e2", doc), Value(123.0));
}

TEST(FilterExprTest, StringLiteral) {
  DocValues doc;
  EXPECT_EQ(Eval("'hello'", doc), Value("hello"s));
  EXPECT_EQ(Eval("\"world\"", doc), Value("world"s));
}

TEST(FilterExprTest, NullLiteral) {
  DocValues doc{{"f", 1.0}};
  EXPECT_FALSE(EvalBool("NULL", doc));
  EXPECT_TRUE(EvalBool("@missing == NULL", doc));
  EXPECT_FALSE(EvalBool("@f == NULL", doc));
}

TEST(FilterExprTest, NullComparisons) {
  DocValues doc;
  // NULL == NULL -> true (both monostate)
  EXPECT_TRUE(EvalBool("NULL == NULL", doc));
  // NULL != NULL -> false (symmetric with ==)
  EXPECT_FALSE(EvalBool("NULL != NULL", doc));
  // NULL != non-null -> true (mixed types)
  EXPECT_TRUE(EvalBool("NULL != 0", doc));
  EXPECT_TRUE(EvalBool("NULL != ''", doc));
  // NULL == non-null -> false
  EXPECT_FALSE(EvalBool("NULL == 0", doc));
  EXPECT_FALSE(EvalBool("NULL == ''", doc));
  // NULL ordering comparisons -> false
  EXPECT_FALSE(EvalBool("NULL < 1", doc));
  EXPECT_FALSE(EvalBool("NULL > 1", doc));
  // NULL <= NULL and NULL >= NULL -> true
  EXPECT_TRUE(EvalBool("NULL <= NULL", doc));
  EXPECT_TRUE(EvalBool("NULL >= NULL", doc));
  // NULL < NULL and NULL > NULL -> still false
  EXPECT_FALSE(EvalBool("NULL < NULL", doc));
  EXPECT_FALSE(EvalBool("NULL > NULL", doc));
}

TEST(FilterExprTest, NullCaseInsensitive) {
  // NULL keyword is case-insensitive -- all variants must be accepted.
  DocValues doc;
  EXPECT_FALSE(EvalBool("null", doc));  // lowercase
  EXPECT_FALSE(EvalBool("Null", doc));  // title case
  EXPECT_FALSE(EvalBool("NULL", doc));  // uppercase (original)
  EXPECT_TRUE(EvalBool("@missing == null", doc));
  EXPECT_TRUE(EvalBool("@missing == Null", doc));
  EXPECT_TRUE(EvalBool("@missing == NULL", doc));
}

// Evaluator: comparison operators

TEST(FilterExprTest, StringEquality) {
  DocValues doc{{"route", "sports"}};
  EXPECT_TRUE(EvalBool("@route == 'sports'", doc));
  EXPECT_FALSE(EvalBool("@route == 'technology'", doc));
  EXPECT_TRUE(EvalBool("@route != 'cooking'", doc));
}

TEST(FilterExprTest, NumericComparisons) {
  DocValues doc{{"distance", 0.5}};
  EXPECT_TRUE(EvalBool("@distance < 0.8", doc));
  EXPECT_FALSE(EvalBool("@distance > 0.8", doc));
  EXPECT_TRUE(EvalBool("@distance <= 0.5", doc));
  EXPECT_TRUE(EvalBool("@distance >= 0.5", doc));
  EXPECT_TRUE(EvalBool("@distance == 0.5", doc));
  EXPECT_FALSE(EvalBool("@distance != 0.5", doc));
}

TEST(FilterExprTest, TypeMismatch) {
  // Comparing string field with number -> false (not equal types)
  DocValues doc{{"tag", "hello"}};
  EXPECT_FALSE(EvalBool("@tag < 1.0", doc));
  EXPECT_FALSE(EvalBool("@tag == 1.0", doc));
  EXPECT_TRUE(EvalBool("@tag != 1.0", doc));
}

TEST(FilterExprTest, CmpOperatorPrecedence) {
  // Relational operators (<, <=, >, >=) bind tighter than equality (==, !=),
  // matching C semantics.
  //   "1 < 2 == 1"  parses as  (1 < 2) == 1  ->  1.0 == 1.0  ->  true
  DocValues doc;
  EXPECT_TRUE(EvalBool("1 < 2 == 1", doc));
  //   "2 > 1 != 0"  parses as  (2 > 1) != 0  ->  1.0 != 0.0  ->  true
  EXPECT_TRUE(EvalBool("2 > 1 != 0", doc));
  // Demonstrates that == has LOWER precedence than <:
  //   "0 == 1 < 2"  parses as  0 == (1 < 2)  ->  0 == 1.0  ->  false
  //   (with same-level left-assoc it would be  (0 == 1) < 2  ->  0.0 < 2  ->  true)
  EXPECT_FALSE(EvalBool("0 == 1 < 2", doc));
}

// Evaluator: logical operators

TEST(FilterExprTest, LogicalAnd) {
  DocValues doc{{"a", "x"}, {"b", 0.5}};
  EXPECT_TRUE(EvalBool("@a == 'x' && @b < 1.0", doc));
  EXPECT_FALSE(EvalBool("@a == 'x' && @b > 1.0", doc));
  EXPECT_FALSE(EvalBool("@a == 'y' && @b < 1.0", doc));
}

TEST(FilterExprTest, LogicalOr) {
  DocValues doc{{"a", "x"}, {"b", 2.0}};
  EXPECT_TRUE(EvalBool("@a == 'x' || @b > 5.0", doc));
  EXPECT_TRUE(EvalBool("@a == 'z' || @b > 1.0", doc));
  EXPECT_FALSE(EvalBool("@a == 'z' || @b > 5.0", doc));
}

TEST(FilterExprTest, LogicalNot) {
  DocValues doc{{"flag", 1.0}};
  EXPECT_FALSE(EvalBool("!@flag", doc));
  EXPECT_TRUE(EvalBool("!!@flag", doc));
  EXPECT_TRUE(EvalBool("!(@flag == 2.0)", doc));
}

TEST(FilterExprTest, LogicalShortCircuit) {
  // AND short-circuits: LHS false -> RHS never evaluated
  // (no crash even if @missing is absent)
  DocValues doc;
  EXPECT_FALSE(EvalBool("0 && @missing == 'x'", doc));
  EXPECT_TRUE(EvalBool("1 || @missing == 'x'", doc));
}

TEST(FilterExprTest, Parentheses) {
  DocValues doc{{"a", 1.0}, {"b", 0.0}, {"c", 1.0}};
  // Without parens: a || (b && c) -- because && binds tighter
  EXPECT_TRUE(EvalBool("@a || @b && @c", doc));
  // With parens: (a || b) && c
  EXPECT_TRUE(EvalBool("(@a || @b) && @c", doc));
  EXPECT_FALSE(EvalBool("(@b || @b) && @c", doc));
}

// Evaluator: arithmetic

TEST(FilterExprTest, Arithmetic) {
  DocValues doc{{"val", 3.0}};
  EXPECT_TRUE(EvalBool("@val + 1 > 3", doc));
  EXPECT_TRUE(EvalBool("@val * 2 == 6", doc));
  EXPECT_TRUE(EvalBool("@val - 1 == 2", doc));
  EXPECT_TRUE(EvalBool("@val / 3 == 1", doc));
  EXPECT_TRUE(EvalBool("@val % 2 == 1", doc));
}

TEST(FilterExprTest, Power) {
  DocValues doc{{"val", 3.0}};
  EXPECT_TRUE(EvalBool("@val ^ 2 < 10", doc));   // 9 < 10
  EXPECT_FALSE(EvalBool("@val ^ 2 > 10", doc));  // 9 > 10 -> false
  // Right-associativity: 2^3^2 = 2^(3^2) = 2^9 = 512
  EXPECT_TRUE(EvalBool("2 ^ 3 ^ 2 == 512", doc));
}

TEST(FilterExprTest, UnaryMinus) {
  DocValues doc{{"val", 3.0}};
  EXPECT_TRUE(EvalBool("-@val == -3", doc));
  EXPECT_TRUE(EvalBool("-@val < 0", doc));
}

TEST(FilterExprTest, DivisionByZero) {
  DocValues doc{{"val", 1.0}};
  // Division by zero -> monostate -> falsy
  EXPECT_FALSE(EvalBool("@val / 0 == 1", doc));
}

TEST(FilterExprTest, NaNBehavior) {
  DocValues doc;
  // sqrt(-1) -> NaN; NaN is falsy
  EXPECT_FALSE(EvalBool("sqrt(-1) > 0", doc));
  EXPECT_FALSE(EvalBool("sqrt(-1) < 0", doc));
  // NaN == NaN -> false (IEEE 754)
  EXPECT_FALSE(EvalBool("sqrt(-1) == sqrt(-1)", doc));
  // NaN != NaN -> true
  EXPECT_TRUE(EvalBool("sqrt(-1) != sqrt(-1)", doc));
}

TEST(FilterExprTest, ArithmeticNullPropagation) {
  DocValues doc{{"val", 5.0}};
  // Missing field in arithmetic -> null propagation
  EXPECT_EQ(Eval("@missing + 1", doc), Value{});
  EXPECT_EQ(Eval("@missing * 2", doc), Value{});
  EXPECT_EQ(Eval("@missing - @val", doc), Value{});
  EXPECT_EQ(Eval("@val / @missing", doc), Value{});
  EXPECT_EQ(Eval("@missing % 3", doc), Value{});
  EXPECT_EQ(Eval("@missing ^ 2", doc), Value{});
}

TEST(FilterExprTest, NegateNonNumeric) {
  DocValues doc{{"tag", "hello"}};
  // Unary minus on string -> null
  EXPECT_EQ(Eval("-@tag", doc), Value{});
  // Unary minus on missing field -> null
  EXPECT_EQ(Eval("-@missing", doc), Value{});
}

// Built-in functions -- string

TEST(FilterExprTest, FuncLower) {
  DocValues doc{{"name", "SPORTS"}};
  EXPECT_TRUE(EvalBool("lower(@name) == 'sports'", doc));
  EXPECT_FALSE(EvalBool("lower(@name) == 'SPORTS'", doc));
}

TEST(FilterExprTest, FuncUpper) {
  DocValues doc{{"name", "sports"}};
  EXPECT_TRUE(EvalBool("upper(@name) == 'SPORTS'", doc));
}

TEST(FilterExprTest, FuncSubstr) {
  DocValues doc{{"tag", "technology"}};
  EXPECT_TRUE(EvalBool("substr(@tag, 0, 4) == 'tech'", doc));
  EXPECT_TRUE(EvalBool("substr(@tag, 4, 4) == 'nolo'", doc));
  // offset >= length -> empty string
  EXPECT_EQ(Eval("substr(@tag, 20, 3)", doc), Value(""s));
  // length beyond end -> clipped to end ("technology"[6..] = "logy")
  EXPECT_TRUE(EvalBool("substr(@tag, 6, 100) == 'logy'", doc));
  // NaN offset or length -> null propagation (no UB)
  EXPECT_EQ(Eval("substr(@tag, sqrt(-1), 4)", doc), Value{});
  EXPECT_EQ(Eval("substr(@tag, 0, sqrt(-1))", doc), Value{});
  // Inf offset or length -> null propagation
  EXPECT_EQ(Eval("substr(@tag, inf, 4)", doc), Value{});
}

TEST(FilterExprTest, FuncStartsWith) {
  DocValues doc{{"name", "sports"}};
  EXPECT_TRUE(EvalBool("startswith(@name, 'spo')", doc));
  EXPECT_FALSE(EvalBool("startswith(@name, 'xyz')", doc));
}

TEST(FilterExprTest, FuncContains) {
  DocValues doc{{"name", "sports"}};
  EXPECT_TRUE(EvalBool("contains(@name, 'ort')", doc));
  EXPECT_FALSE(EvalBool("contains(@name, 'xyz')", doc));
}

TEST(FilterExprTest, FuncExists) {
  DocValues doc{{"present", 0.0}};
  EXPECT_TRUE(EvalBool("exists(@present)", doc));  // field exists (even if 0)
  EXPECT_FALSE(EvalBool("exists(@absent)", doc));  // field not in doc
}

TEST(FilterExprTest, FuncStrLen) {
  DocValues doc{{"name", "hello"}};
  EXPECT_TRUE(EvalBool("strlen(@name) == 5", doc));
  EXPECT_FALSE(EvalBool("strlen(@name) == 3", doc));
}

TEST(FilterExprTest, FuncToNumber) {
  DocValues doc{{"str_val", "42.5"}};
  EXPECT_TRUE(EvalBool("to_number(@str_val) > 40", doc));
  // Round-trip for special values: to_number("inf") -> Inf, to_number("nan") -> NaN
  DocValues doc2{{"s_inf", "inf"}, {"s_nan", "nan"}};
  EXPECT_TRUE(EvalBool("to_number(@s_inf) > 1e300", doc2));
  // NaN is falsy and NaN != NaN
  EXPECT_FALSE(EvalBool("to_number(@s_nan) == to_number(@s_nan)", doc2));
}

TEST(FilterExprTest, FuncToStr) {
  DocValues doc{{"num", 7.0}};
  EXPECT_TRUE(EvalBool("strlen(to_str(@num)) > 0", doc));
  // null input -> propagate null (monostate), not empty string
  DocValues doc2;
  EXPECT_EQ(Eval("to_str(NULL)", doc2), Value{});
  EXPECT_EQ(Eval("to_str(@missing)", doc2), Value{});
  // null propagation: to_str(NULL) is falsy (monostate, not empty string)
  EXPECT_FALSE(EvalBool("exists(to_str(NULL))", doc2));
}

TEST(FilterExprTest, FuncFormat) {
  DocValues doc{{"name", "Alice"}, {"score", 42.0}};
  EXPECT_EQ(Eval("format('100%%')", doc), Value(std::string{"100%"}));
  EXPECT_EQ(Eval("format('hi %s', @name)", doc), Value(std::string{"hi Alice"}));
  // %d truncates to integer
  EXPECT_EQ(Eval("format('%d', @score)", doc), Value(std::string{"42"}));
  // %f: standard printf -- 6 decimal places
  EXPECT_EQ(Eval("format('%f', @score)", doc), Value(std::string{"42.000000"}));
  // Unknown specifier: emitted literally, arg NOT consumed -> subsequent %s still works
  EXPECT_EQ(Eval("format('%z %s', @name)", doc), Value(std::string{"%z Alice"}));
  // %d with out-of-range finite double -> clamped to max safe long long (no UB)
  EXPECT_EQ(Eval("format('%d', 1e100)", doc), Value(std::string{"9223372036854774784"}));
  EXPECT_EQ(Eval("format('%d', -1e100)", doc), Value(std::string{"-9223372036854775808"}));
  // %d with NaN or +/-Inf (non-finite) -> no output, consistent with null propagation
  EXPECT_EQ(Eval("format('%d', sqrt(-1))", doc), Value(std::string{""}));
  EXPECT_EQ(Eval("format('%d', inf)", doc), Value(std::string{""}));
  // %f with NaN / +/-Inf -> emit nothing (same behaviour as %d)
  EXPECT_EQ(Eval("format('%f', sqrt(-1))", doc), Value(std::string{""}));
  EXPECT_EQ(Eval("format('%f', inf)", doc), Value(std::string{""}));
  // %s with NaN / Inf -> string representation via absl::StrCat
  EXPECT_EQ(Eval("format('%s', sqrt(-1))", doc), Value(std::string{"nan"}));
  EXPECT_EQ(Eval("format('%s', inf)", doc), Value(std::string{"inf"}));
  EXPECT_EQ(Eval("format('%s', -inf)", doc), Value(std::string{"-inf"}));
  // Fewer args than specifiers: second %s has no arg -> literal '%s' kept
  EXPECT_EQ(Eval("format('%s %s', @name)", doc), Value(std::string{"Alice %s"}));
  // Null arg for %s -> nothing emitted
  EXPECT_EQ(Eval("format('[%s]', @missing)", doc), Value(std::string{"[]"}));
}

TEST(FilterExprTest, FuncSplit) {
  // Basic no-op: already comma-separated -- string unchanged
  DocValues doc{{"tags", "sports,tech"}};
  EXPECT_EQ(Eval("split(@tags, ',')", doc), Value("sports,tech"s));

  // Separator normalisation: semicolon -> comma
  DocValues doc2{{"tags", "sports;tech"}};
  EXPECT_EQ(Eval("split(@tags, ';')", doc2), Value("sports,tech"s));

  // Strip whitespace (third argument != 0)
  DocValues doc3{{"tags", "sports, tech"}};
  EXPECT_EQ(Eval("split(@tags, ',', 1)", doc3), Value("sports,tech"s));

  // Strip with numeric zero -- do not strip
  EXPECT_EQ(Eval("split(@tags, ',', 0)", doc3), Value("sports, tech"s));

  // contains() on normalised result
  DocValues doc4{{"tags", "sports;tech;cooking"}};
  EXPECT_TRUE(EvalBool("contains(split(@tags, ';'), 'tech')", doc4));
  EXPECT_FALSE(EvalBool("contains(split(@tags, ';'), 'news')", doc4));

  // Multi-character separator
  DocValues doc_multi{{"tags", "a::b::c"}};
  EXPECT_EQ(Eval("split(@tags, '::')", doc_multi), Value("a,b,c"s));

  // Empty separator -> original string unchanged (avoid character-by-character split)
  DocValues doc5{{"tags", "sports,tech"}};
  EXPECT_EQ(Eval("split(@tags, '')", doc5), Value("sports,tech"s));

  // NaN as strip flag -> falsy (NaN is falsy everywhere in this codebase)
  DocValues doc6{{"tags", "sports, tech"}};
  EXPECT_EQ(Eval("split(@tags, ',', sqrt(-1))", doc6), Value("sports, tech"s));

  // Inf as strip flag -> truthy (consistent with IsTruthy semantics)
  EXPECT_EQ(Eval("split(@tags, ',', inf)", doc6), Value("sports,tech"s));
}

TEST(FilterExprTest, FuncSplitNullSeparator) {
  // Non-string separator -> propagate null (same rule as rest of codebase)
  DocValues doc{{"tags", "sports,tech"}};
  EXPECT_EQ(Eval("split(@tags, NULL)", doc), Value{});
  EXPECT_EQ(Eval("split(@tags, 42)", doc), Value{});
}

TEST(FilterExprTest, FuncMatchedTerms) {
  // matched_terms() always returns empty string (no FT.SEARCH context)
  DocValues doc{{"name", "hello"}};
  EXPECT_EQ(Eval("matched_terms()", doc), Value(""s));
  // Empty string -- falsy
  EXPECT_FALSE(EvalBool("matched_terms()", doc));
  // strlen(matched_terms()) == 0
  EXPECT_TRUE(EvalBool("strlen(matched_terms()) == 0", doc));
}

TEST(FilterExprTest, FuncGeoDistance) {
  // Paris (2.3522,48.8566) -> Reims (4.0317,49.2583) ~ 157 km
  DocValues doc{{"p1", "2.3522,48.8566"}, {"p2", "4.0317,49.2583"}};
  // Default unit: km
  EXPECT_TRUE(EvalBool("geodistance(@p1, @p2) > 100", doc));
  EXPECT_FALSE(EvalBool("geodistance(@p1, @p2) > 500", doc));
}

TEST(FilterExprTest, FuncGeoDistanceUnits) {
  // Paris -> Reims ~ 157 km ~ 157000 m ~ 97 mi ~ 514000 ft
  DocValues doc{{"p1", "2.3522,48.8566"}, {"p2", "4.0317,49.2583"}};
  EXPECT_TRUE(EvalBool("geodistance(@p1, @p2, 'm') > 100000", doc));
  EXPECT_TRUE(EvalBool("geodistance(@p1, @p2, 'km') > 100", doc));
  EXPECT_TRUE(EvalBool("geodistance(@p1, @p2, 'mi') > 60", doc));
  EXPECT_TRUE(EvalBool("geodistance(@p1, @p2, 'ft') > 300000", doc));
}

TEST(FilterExprTest, FuncGeoDistanceAntipodal) {
  // Near-antipodal points: North Pole (0,90) and South Pole (0,-90) ~ 20015 km.
  // Floating-point rounding can push Haversine's `a` slightly above 1.0;
  // the result must be a finite number, never NaN.
  DocValues doc{{"p1", "0.0,90.0"}, {"p2", "0.0,-90.0"}};
  auto v = Eval("geodistance(@p1, @p2)", doc);
  ASSERT_TRUE(std::holds_alternative<double>(v));
  double dist = std::get<double>(v);
  EXPECT_TRUE(std::isfinite(dist));
  EXPECT_GT(dist, 19000.0);
  EXPECT_LT(dist, 21000.0);
}

TEST(FilterExprTest, FuncGeoDistanceUnknownUnit) {
  DocValues doc{{"p1", "2.3522,48.8566"}, {"p2", "4.0317,49.2583"}};
  // Unknown unit -> null (not a silent km fallback)
  EXPECT_EQ(Eval("geodistance(@p1, @p2, 'yd')", doc), Value{});
  EXPECT_EQ(Eval("geodistance(@p1, @p2, 'nmi')", doc), Value{});
  // Non-string unit (null or number) -> propagate null
  EXPECT_EQ(Eval("geodistance(@p1, @p2, NULL)", doc), Value{});
  EXPECT_EQ(Eval("geodistance(@p1, @p2, 42)", doc), Value{});
}

// Built-in functions -- math

TEST(FilterExprTest, FuncAbs) {
  DocValues doc{{"val", -3.5}};
  EXPECT_TRUE(EvalBool("abs(@val) == 3.5", doc));
}

TEST(FilterExprTest, FuncFloor) {
  DocValues doc{{"val", 3.7}};
  EXPECT_EQ(Eval("floor(@val)", doc), Value(3.0));
}

TEST(FilterExprTest, FuncCeil) {
  DocValues doc{{"val", 3.2}};
  EXPECT_EQ(Eval("ceil(@val)", doc), Value(4.0));
}

TEST(FilterExprTest, FuncSqrt) {
  DocValues doc{{"val", 9.0}};
  EXPECT_EQ(Eval("sqrt(@val)", doc), Value(3.0));
}

TEST(FilterExprTest, FuncLog) {
  // log(1) == 0; log(e) ~ 1
  DocValues doc{{"val", 1.0}};
  EXPECT_EQ(Eval("log(@val)", doc), Value(0.0));
  DocValues doc2{{"val", std::exp(1.0)}};
  EXPECT_DOUBLE_EQ(std::get<double>(Eval("log(@val)", doc2)), 1.0);
}

TEST(FilterExprTest, FuncLog2) {
  // log2(8) == 3; log2(1) == 0
  DocValues doc{{"val", 8.0}};
  EXPECT_EQ(Eval("log2(@val)", doc), Value(3.0));
  DocValues doc2{{"val", 1.0}};
  EXPECT_EQ(Eval("log2(@val)", doc2), Value(0.0));
}

TEST(FilterExprTest, FuncLog10) {
  // log10(100) == 2; log10(1) == 0
  DocValues doc{{"val", 100.0}};
  EXPECT_EQ(Eval("log10(@val)", doc), Value(2.0));
  DocValues doc2{{"val", 1.0}};
  EXPECT_EQ(Eval("log10(@val)", doc2), Value(0.0));
}

TEST(FilterExprTest, FuncExp) {
  // exp(0) == 1; exp(1) ~ e
  DocValues doc{{"val", 0.0}};
  EXPECT_EQ(Eval("exp(@val)", doc), Value(1.0));
  DocValues doc2{{"val", 1.0}};
  EXPECT_DOUBLE_EQ(std::get<double>(Eval("exp(@val)", doc2)), std::exp(1.0));
}

TEST(FilterExprTest, FuncUnknownReturnsNull) {
  // An unknown function name should parse successfully but return null (monostate) at eval time.
  DocValues doc{{"val", 42.0}};
  EXPECT_TRUE(ParseOk("no_such_func(@val)"));
  EXPECT_EQ(Eval("no_such_func(@val)", doc), Value{});
  EXPECT_FALSE(EvalBool("no_such_func(@val)", doc));
}

// Built-in functions -- date/time

TEST(FilterExprTest, FuncYear) {
  // 2026-01-01 00:00:00 UTC  ->  UNIX timestamp 1767225600
  DocValues doc{{"ts", 1767225600.0}};
  EXPECT_EQ(Eval("year(@ts)", doc), Value(2026.0));
}

TEST(FilterExprTest, FuncMonthOfYear) {
  // 2026-03-15 00:00:00 UTC  ->  1773532800
  DocValues doc{{"ts", 1773532800.0}};
  EXPECT_EQ(Eval("monthofyear(@ts)", doc), Value(3.0));
  EXPECT_EQ(Eval("month(@ts)", doc), Value(3.0));  // alias
}

TEST(FilterExprTest, FuncHour) {
  // 2026-01-01 14:00:00 UTC  ->  1767276000
  DocValues doc{{"ts", 1767276000.0}};
  EXPECT_EQ(Eval("hour(@ts)", doc), Value(14.0));
}

TEST(FilterExprTest, FuncMinute) {
  // 2026-01-01 14:30:00 UTC  ->  1767277800
  DocValues doc{{"ts", 1767277800.0}};
  EXPECT_EQ(Eval("minute(@ts)", doc), Value(30.0));
}

TEST(FilterExprTest, FuncDayOfMonth) {
  // 2026-03-15 00:00:00 UTC  ->  1773532800
  DocValues doc{{"ts", 1773532800.0}};
  EXPECT_EQ(Eval("day(@ts)", doc), Value(15.0));
  EXPECT_EQ(Eval("dayofmonth(@ts)", doc), Value(15.0));  // alias
}

TEST(FilterExprTest, FuncDayOfWeek) {
  // 2026-01-01 is Thursday -> tm_wday == 4 (0=Sunday)
  DocValues doc{{"ts", 1767225600.0}};
  EXPECT_EQ(Eval("dayofweek(@ts)", doc), Value(4.0));
}

TEST(FilterExprTest, FuncDayOfYear) {
  // 2026-03-15 -> Jan(31) + Feb(28) + 15 = 74th day; ts = 1773532800
  DocValues doc{{"ts", 1773532800.0}};
  EXPECT_EQ(Eval("dayofyear(@ts)", doc), Value(74.0));
}

TEST(FilterExprTest, FuncTimefmt) {
  // 2026-01-01 00:00:00 UTC -> "2026-01-01" with %Y-%m-%d
  DocValues doc{{"ts", 1767225600.0}};
  EXPECT_EQ(Eval("timefmt(@ts, '%Y-%m-%d')", doc), Value("2026-01-01"s));
  // default format includes time
  auto v = Eval("timefmt(@ts)", doc);
  ASSERT_TRUE(std::holds_alternative<std::string>(v));
  EXPECT_EQ(std::get<std::string>(v).substr(0, 10), "2026-01-01");
  // Empty format string -> empty result (strftime produces nothing)
  EXPECT_EQ(Eval("timefmt(@ts, '')", doc), Value(""s));
  // NaN / Inf timestamp -> null propagation (no UB in ToTm)
  DocValues doc2;
  EXPECT_EQ(Eval("timefmt(sqrt(-1), '%Y')", doc2), Value{});
  EXPECT_EQ(Eval("timefmt(inf, '%Y')", doc2), Value{});
  // Large finite timestamp -> clamped to max time_t -> some far-future date (no UB)
  EXPECT_TRUE(std::holds_alternative<std::string>(Eval("timefmt(1e100, '%Y')", doc2)));
  // Non-string format (null or number) -> propagate null
  EXPECT_EQ(Eval("timefmt(@ts, NULL)", doc), Value{});
  EXPECT_EQ(Eval("timefmt(@ts, 42)", doc), Value{});
}

TEST(FilterExprTest, FuncParseTime) {
  // "2026-03-15" with format "%Y-%m-%d" -> 1773532800
  DocValues doc;
  EXPECT_EQ(Eval("parsetime('2026-03-15', '%Y-%m-%d')", doc), Value(1773532800.0));
  // Round-trip: timefmt(parsetime(s, fmt), fmt) == s
  DocValues doc2{{"s", "2026-01-01"}};
  EXPECT_TRUE(EvalBool("timefmt(parsetime(@s, '%Y-%m-%d'), '%Y-%m-%d') == @s", doc2));
  // Trailing garbage must be rejected (strptime partial match)
  EXPECT_EQ(Eval("parsetime('2026-03-15garbage', '%Y-%m-%d')", doc), Value{});
  EXPECT_EQ(Eval("parsetime('2026-03-15 12:00', '%Y-%m-%d')", doc), Value{});
  // Non-string arguments -> null propagation
  EXPECT_EQ(Eval("parsetime(42, '%Y')", doc), Value{});
  EXPECT_EQ(Eval("parsetime('2026', 42)", doc), Value{});
  EXPECT_EQ(Eval("parsetime(NULL, '%Y')", doc), Value{});
  EXPECT_EQ(Eval("parsetime('2026', NULL)", doc), Value{});
}

// MakeFilterStep (pipeline integration)

TEST(FilterExprTest, FilterStepParseError) {
  auto result = MakeFilterStep("@a ==");  // incomplete expression
  EXPECT_TRUE(std::holds_alternative<std::string>(result));
  EXPECT_FALSE(std::get<std::string>(result).empty());
}

TEST(FilterExprTest, FilterStepAllPass) {
  std::vector<DocValues> values = {
      DocValues{{"score", 0.3}},
      DocValues{{"score", 0.5}},
      DocValues{{"score", 0.7}},
  };
  auto step_or_err = MakeFilterStep("@score < 1.0");
  ASSERT_TRUE(std::holds_alternative<AggregationStep>(step_or_err));
  std::vector<AggregationStep> steps = {std::move(std::get<AggregationStep>(step_or_err))};
  auto result = Process(values, {"score"}, steps);
  EXPECT_EQ(result.values.size(), 3);
}

TEST(FilterExprTest, FilterStepEmptyResult) {
  std::vector<DocValues> values = {
      DocValues{{"score", 0.3}},
      DocValues{{"score", 0.5}},
  };
  auto step_or_err = MakeFilterStep("@score > 1.0");
  ASSERT_TRUE(std::holds_alternative<AggregationStep>(step_or_err));
  std::vector<AggregationStep> steps = {std::move(std::get<AggregationStep>(step_or_err))};
  auto result = Process(values, {"score"}, steps);
  EXPECT_EQ(result.values.size(), 0);
}

TEST(FilterExprTest, FilterStepRemovesRows) {
  std::vector<DocValues> values = {
      DocValues{{"distance", 0.3}, {"route", "sports"}},
      DocValues{{"distance", 0.9}, {"route", "technology"}},
      DocValues{{"distance", 0.5}, {"route", "cooking"}},
  };
  auto step_or_err = MakeFilterStep("@distance < 0.8");
  ASSERT_TRUE(std::holds_alternative<AggregationStep>(step_or_err));
  std::vector<AggregationStep> steps = {std::move(std::get<AggregationStep>(step_or_err))};
  auto result = Process(values, {"distance", "route"}, steps);
  ASSERT_EQ(result.values.size(), 2);
  for (const auto& row : result.values) {
    EXPECT_LT(std::get<double>(row.at("distance")), 0.8);
  }
}

TEST(FilterExprTest, FilterStepCompound) {
  std::vector<DocValues> values = {
      DocValues{{"distance", 0.3}, {"route", "sports"}},
      DocValues{{"distance", 0.9}, {"route", "sports"}},
      DocValues{{"distance", 0.5}, {"route", "technology"}},
      DocValues{{"distance", 0.2}, {"route", "cooking"}},
  };
  // Keep only routes "sports" or "cooking" with distance < 0.8
  auto step_or_err = MakeFilterStep(
      "(@route == 'sports' && @distance < 0.8) || "
      "(@route == 'cooking' && @distance < 0.8)");
  ASSERT_TRUE(std::holds_alternative<AggregationStep>(step_or_err));
  std::vector<AggregationStep> steps = {std::move(std::get<AggregationStep>(step_or_err))};
  auto result = Process(values, {"distance", "route"}, steps);
  ASSERT_EQ(result.values.size(), 2);
}

TEST(FilterExprTest, FilterStepWithFunctionCall) {
  std::vector<DocValues> values = {
      DocValues{{"route", "Sports"}},
      DocValues{{"route", "SPORTS"}},
      DocValues{{"route", "cooking"}},
  };
  auto step_or_err = MakeFilterStep("lower(@route) == 'sports'");
  ASSERT_TRUE(std::holds_alternative<AggregationStep>(step_or_err));
  std::vector<AggregationStep> steps = {std::move(std::get<AggregationStep>(step_or_err))};
  auto result = Process(values, {"route"}, steps);
  ASSERT_EQ(result.values.size(), 2);
}

TEST(FilterExprTest, FilterStepAfterGroupBy) {
  // Simulate the semantic-routing use case:
  // GroupBy route, avg distance -- then filter groups by average distance.
  std::vector<DocValues> values;
  values.reserve(10);
  for (int i = 0; i < 5; ++i)
    values.push_back(DocValues{{"vector_distance", 0.3 + i * 0.05}, {"route_name", "sports"}});
  for (int i = 0; i < 5; ++i)
    values.push_back(DocValues{{"vector_distance", 0.9 + i * 0.05}, {"route_name", "cooking"}});

  std::vector<std::string> group_fields = {"route_name"};
  std::vector<Reducer> reducers = {
      Reducer{"vector_distance", "distance", FindReducerFunc(ReducerFunc::AVG)}};
  auto group_step = MakeGroupStep(std::move(group_fields), std::move(reducers));

  auto filter_or_err = MakeFilterStep("@distance < 0.8");
  ASSERT_TRUE(std::holds_alternative<AggregationStep>(filter_or_err));

  std::vector<AggregationStep> steps = {std::move(group_step),
                                        std::move(std::get<AggregationStep>(filter_or_err))};
  auto result = Process(values, {"route_name", "distance"}, steps);

  // Only the "sports" group has avg distance < 0.8
  ASSERT_EQ(result.values.size(), 1);
  EXPECT_EQ(result.values[0].at("route_name"), Value("sports"s));
}

// Reducer edge cases: null and NaN handling

TEST(FilterExprTest, ReducerMinSkipsNull) {
  // MIN should skip null (monostate) values and return the minimum real value.
  std::vector<DocValues> values = {
      DocValues{{"val", 5.0}},
      DocValues{},  // missing "val" -> iterator yields monostate
      DocValues{{"val", 3.0}},
  };
  auto step = MakeGroupStep({}, {Reducer{"val", "r", FindReducerFunc(ReducerFunc::MIN)}});
  auto result = Process(std::move(values), {"r"}, {step});
  ASSERT_EQ(result.values.size(), 1);
  EXPECT_EQ(result.values[0].at("r"), Value(3.0));
}

TEST(FilterExprTest, ReducerMaxSkipsNull) {
  std::vector<DocValues> values = {
      DocValues{},  // missing -> monostate
      DocValues{{"val", 5.0}},
      DocValues{{"val", 8.0}},
  };
  auto step = MakeGroupStep({}, {Reducer{"val", "r", FindReducerFunc(ReducerFunc::MAX)}});
  auto result = Process(std::move(values), {"r"}, {step});
  ASSERT_EQ(result.values.size(), 1);
  EXPECT_EQ(result.values[0].at("r"), Value(8.0));
}

TEST(FilterExprTest, ReducerMinAllNull) {
  // All values null -> MIN returns null (monostate).
  std::vector<DocValues> values = {DocValues{}, DocValues{}};
  auto step = MakeGroupStep({}, {Reducer{"val", "r", FindReducerFunc(ReducerFunc::MIN)}});
  auto result = Process(std::move(values), {"r"}, {step});
  ASSERT_EQ(result.values.size(), 1);
  EXPECT_EQ(result.values[0].at("r"), Value{});
}

TEST(FilterExprTest, ReducerMinMaxSkipNaN) {
  // NaN placed first -- must not "stick" as the min or max.
  double nan = std::numeric_limits<double>::quiet_NaN();
  std::vector<DocValues> values = {
      DocValues{{"val", nan}},
      DocValues{{"val", 1.0}},
      DocValues{{"val", 3.0}},
  };
  auto step = MakeGroupStep({}, {Reducer{"val", "mn", FindReducerFunc(ReducerFunc::MIN)},
                                 Reducer{"val", "mx", FindReducerFunc(ReducerFunc::MAX)}});
  auto result = Process(std::move(values), {"mn", "mx"}, {step});
  ASSERT_EQ(result.values.size(), 1);
  EXPECT_EQ(result.values[0].at("mn"), Value(1.0));
  EXPECT_EQ(result.values[0].at("mx"), Value(3.0));
}

TEST(FilterExprTest, ReducerSumAvgSkipNaN) {
  double nan = std::numeric_limits<double>::quiet_NaN();
  std::vector<DocValues> values = {
      DocValues{{"val", 1.0}},
      DocValues{{"val", nan}},
      DocValues{{"val", 3.0}},
  };
  auto step = MakeGroupStep({}, {Reducer{"val", "s", FindReducerFunc(ReducerFunc::SUM)},
                                 Reducer{"val", "a", FindReducerFunc(ReducerFunc::AVG)}});
  auto result = Process(std::move(values), {"s", "a"}, {step});
  ASSERT_EQ(result.values.size(), 1);
  EXPECT_EQ(result.values[0].at("s"), Value(4.0));  // 1 + 3, NaN skipped
  EXPECT_EQ(result.values[0].at("a"), Value(2.0));  // 4 / 2
}

TEST(FilterExprTest, ReducerSumAvgSkipInf) {
  // +/-Inf are non-finite and must be skipped, same as NaN.
  double inf = std::numeric_limits<double>::infinity();
  std::vector<DocValues> values = {
      DocValues{{"val", 1.0}},
      DocValues{{"val", inf}},
      DocValues{{"val", -inf}},
      DocValues{{"val", 3.0}},
  };
  auto step = MakeGroupStep({}, {Reducer{"val", "s", FindReducerFunc(ReducerFunc::SUM)},
                                 Reducer{"val", "a", FindReducerFunc(ReducerFunc::AVG)}});
  auto result = Process(std::move(values), {"s", "a"}, {step});
  ASSERT_EQ(result.values.size(), 1);
  EXPECT_EQ(result.values[0].at("s"), Value(4.0));  // 1 + 3, Inf skipped
  EXPECT_EQ(result.values[0].at("a"), Value(2.0));  // 4 / 2
}

TEST(FilterExprTest, ReducerCountDistinctNaN) {
  // Two NaN values should count as one distinct value (normalized to monostate).
  double nan = std::numeric_limits<double>::quiet_NaN();
  std::vector<DocValues> values = {
      DocValues{{"val", nan}},
      DocValues{{"val", nan}},
      DocValues{{"val", 1.0}},
  };
  auto step =
      MakeGroupStep({}, {Reducer{"val", "cd", FindReducerFunc(ReducerFunc::COUNT_DISTINCT)}});
  auto result = Process(std::move(values), {"cd"}, {step});
  ASSERT_EQ(result.values.size(), 1);
  // 2 distinct: normalized NaN (as monostate) + 1.0
  EXPECT_EQ(result.values[0].at("cd"), Value(2.0));
}

// DoSort: monostate and mixed-type handling

TEST(FilterExprTest, SortMonostateSortsLikeAbsent) {
  // A field present with monostate value should sort to the end,
  // same as a field that is entirely absent from the DocValues.
  std::vector<DocValues> values = {
      DocValues{{"val", 3.0}}, DocValues{{"val", Value{}}},  // present but monostate (null)
      DocValues{{"val", 1.0}}, DocValues{},                  // field absent entirely
  };
  SortParams params;
  params.fields.push_back({"val", SortOrder::ASC});
  auto step = MakeSortStep(std::move(params));
  auto result = Process(std::move(values), {"val"}, {step});
  ASSERT_EQ(result.values.size(), 4);
  // Sortable values come first in ascending order.
  EXPECT_EQ(result.values[0].at("val"), Value(1.0));
  EXPECT_EQ(result.values[1].at("val"), Value(3.0));
  // Last two are null/absent -- both sort to the end.
}

TEST(FilterExprTest, SortMixedTypesOrdered) {
  // Doubles (variant index 1) sort before strings (index 2) in ASC.
  std::vector<DocValues> values = {
      DocValues{{"val", "beta"}},
      DocValues{{"val", 2.0}},
      DocValues{{"val", "alpha"}},
      DocValues{{"val", 1.0}},
  };
  SortParams params;
  params.fields.push_back({"val", SortOrder::ASC});
  auto step = MakeSortStep(std::move(params));
  auto result = Process(std::move(values), {"val"}, {step});
  ASSERT_EQ(result.values.size(), 4);
  // Doubles come first (sorted among themselves), then strings (sorted among themselves).
  EXPECT_EQ(result.values[0].at("val"), Value(1.0));
  EXPECT_EQ(result.values[1].at("val"), Value(2.0));
  EXPECT_EQ(result.values[2].at("val"), Value("alpha"s));
  EXPECT_EQ(result.values[3].at("val"), Value("beta"s));
}

TEST(FilterExprTest, SortMixedTypesDESC) {
  // In DESC, strings (higher variant index) come first, then doubles.
  std::vector<DocValues> values = {
      DocValues{{"val", "beta"}},
      DocValues{{"val", 2.0}},
      DocValues{{"val", "alpha"}},
      DocValues{{"val", 1.0}},
  };
  SortParams params;
  params.fields.push_back({"val", SortOrder::DESC});
  auto step = MakeSortStep(std::move(params));
  auto result = Process(std::move(values), {"val"}, {step});
  ASSERT_EQ(result.values.size(), 4);
  EXPECT_EQ(result.values[0].at("val"), Value("beta"s));
  EXPECT_EQ(result.values[1].at("val"), Value("alpha"s));
  EXPECT_EQ(result.values[2].at("val"), Value(2.0));
  EXPECT_EQ(result.values[3].at("val"), Value(1.0));
}

TEST(FilterExprTest, GroupByNaNGroupsTogether) {
  // NaN values in GROUPBY fields must group together (not create per-document groups).
  double nan = std::numeric_limits<double>::quiet_NaN();
  std::vector<DocValues> values = {
      DocValues{{"cat", nan}, {"v", 1.0}},
      DocValues{{"cat", nan}, {"v", 2.0}},
      DocValues{{"cat", 1.0}, {"v", 3.0}},
  };
  auto step = MakeGroupStep({"cat"}, {Reducer{"v", "cnt", FindReducerFunc(ReducerFunc::COUNT)}});
  auto result = Process(std::move(values), {"cat", "cnt"}, {step});
  // Two groups: NaN (normalized to monostate, 2 docs) and 1.0 (1 doc).
  ASSERT_EQ(result.values.size(), 2);
}

}  // namespace dfly::aggregate
