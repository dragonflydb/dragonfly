// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "base/gtest.h"
#include "base/logging.h"
#include "core/search/query_driver.h"

namespace dfly {
namespace search {

using namespace std;

class SearchParserTest : public ::testing::Test {
 protected:
  SearchParserTest() {
    query_driver_.scanner()->set_debug(1);
  }

  void SetInput(const std::string& str) {
    query_driver_.SetInput(str);
  }

  Parser::symbol_type Lex() {
    return query_driver_.Lex();
  }

  int Parse(const std::string& str) {
    query_driver_.ResetScanner();
    query_driver_.SetInput(str);

    return Parser(&query_driver_)();
  }

  void ParseExpr(const std::string& str) {
    raw_expr_ = str;
    Parse(str);
    expr_ = query_driver_.Get();
  }

  bool Check(string_view input) const {
    return expr_->Check(input);
  }

  string DebugExpr() const {
    return raw_expr_ + " parsed as " + expr_->Debug();
  }

  string raw_expr_;
  AstExpr expr_;
  QueryDriver query_driver_;
};

// tokens are not assignable, so we can not reuse them. This macros reduce the boilerplate.
#define NEXT_EQ(tok_enum, type, val)                    \
  {                                                     \
    auto tok = Lex();                                   \
    ASSERT_EQ(tok.type_get(), Parser::token::tok_enum); \
    EXPECT_EQ(val, tok.value.as<type>());               \
  }

#define NEXT_TOK(tok_enum)                              \
  {                                                     \
    auto tok = Lex();                                   \
    ASSERT_EQ(tok.type_get(), Parser::token::tok_enum); \
  }
#define NEXT_ERROR()                          \
  {                                           \
    bool caught = false;                      \
    try {                                     \
      auto tok = Lex();                       \
    } catch (const Parser::syntax_error& e) { \
      caught = true;                          \
    }                                         \
    ASSERT_TRUE(caught);                      \
  }

#define CHECK_ALL(...)                                                    \
  {                                                                       \
    for (auto input : {__VA_ARGS__})                                      \
      EXPECT_TRUE(Check(input)) << input << " failed on " << DebugExpr(); \
  }

#define CHECK_NONE(...)                                                    \
  {                                                                        \
    for (auto input : {__VA_ARGS__})                                       \
      EXPECT_FALSE(Check(input)) << input << " failed on " << DebugExpr(); \
  }

TEST_F(SearchParserTest, Scanner) {
  SetInput("ab cd");
  // 3.5.1 does not have name() method.
  // EXPECT_STREQ("term", tok.name());

  NEXT_EQ(TOK_TERM, string, "ab");
  NEXT_EQ(TOK_TERM, string, "cd");
  NEXT_TOK(TOK_YYEOF);

  SetInput("(5a 6) ");

  NEXT_TOK(TOK_LPAREN);
  NEXT_EQ(TOK_TERM, string, "5a");
  NEXT_EQ(TOK_INT64, int64_t, 6);
  NEXT_TOK(TOK_RPAREN);

  SetInput(R"( "hello\"world" )");
  NEXT_EQ(TOK_TERM, string, R"(hello"world)");

  SetInput(" $param @field:hello");
  NEXT_EQ(TOK_PARAM, string, "$param");
  NEXT_EQ(TOK_FIELD, string, "@field");
  NEXT_TOK(TOK_COLON);
  NEXT_EQ(TOK_TERM, string, "hello");

  SetInput("почтальон Печкин");
  NEXT_EQ(TOK_TERM, string, "почтальон");
  NEXT_EQ(TOK_TERM, string, "Печкин");

  SetInput("18446744073709551616");
  NEXT_ERROR();
}

TEST_F(SearchParserTest, Parse) {
  EXPECT_EQ(0, Parse(" foo bar (baz) "));
  EXPECT_EQ(0, Parse(" -(foo) @foo:bar @ss:[1 2]"));
  EXPECT_EQ(1, Parse(" -(foo "));
  EXPECT_EQ(1, Parse(" foo:bar "));
  EXPECT_EQ(1, Parse(" @foo:@bar "));
  EXPECT_EQ(1, Parse(" @foo: "));
}

TEST_F(SearchParserTest, MatchTerm) {
  ParseExpr("foo");
  CHECK_ALL("foo", "foo bar", "more foo bar");
  CHECK_NONE("wrong", "nomatch");
}

TEST_F(SearchParserTest, MatchNotTerm) {
  ParseExpr("-foo");
  CHECK_ALL("faa", "definitielyright");
  CHECK_NONE("foo", "foo bar", "more foo bar");
}

TEST_F(SearchParserTest, MatchLogicalNode) {
  ParseExpr("foo bar");

  CHECK_ALL("foo bar", "bar foo", "more bar and foo");
  CHECK_NONE("wrong", "foo", "bar", "foob", "far");

  ParseExpr("foo | bar");

  CHECK_ALL("foo bar", "foo", "bar", "foo and more", "or only bar");
  CHECK_NONE("wrong", "only far");

  ParseExpr("foo bar baz");

  CHECK_ALL("baz bar foo", "bar and foo and baz");
  CHECK_NONE("wrong", "foo baz", "bar baz", "and foo");
}

TEST_F(SearchParserTest, MatchParenthesis) {
  ParseExpr("( foo | oof ) ( bar | rab )");

  CHECK_ALL("foo bar", "oof rab", "foo rab", "oof bar", "foo oof bar rab");
  CHECK_NONE("wrong", "bar rab", "foo oof");
}

TEST_F(SearchParserTest, CheckNotPriority) {
  for (auto expr : {"-bar foo baz", "foo -bar baz", "foo baz -bar"}) {
    ParseExpr(expr);

    CHECK_ALL("foo baz", "foo rab baz", "baz rab foo");
    CHECK_NONE("wrong", "bar", "foo bar baz", "foo baz bar");
  }

  for (auto expr : {"-bar | foo", "foo | -bar"}) {
    ParseExpr(expr);

    CHECK_ALL("foo", "right", "foo bar");
    CHECK_NONE("bar", "bar baz");
  }

  ParseExpr("foo | -(bar baz)");
  CHECK_ALL("foo", "not b/r and b/z", "foo bar baz", "single bar", "only baz");
  CHECK_NONE("bar baz", "some more bar and baz");
}

}  // namespace search

}  // namespace dfly
