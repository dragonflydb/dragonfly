// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "base/gtest.h"
#include "base/logging.h"
#include "core/search/base.h"
#include "core/search/query_driver.h"
#include "core/search/search.h"

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
    search_algo_.emplace(str);
  }

  bool Check(DocumentAccessor* doc) const {
    return search_algo_->Check(doc);
  }

  string DebugExpr() const {
    return raw_expr_;
  }

  string raw_expr_;
  optional<SearchAlgorithm> search_algo_;
  QueryDriver query_driver_;
};

class MockedDocument : public DocumentAccessor {
 public:
  using Map = std::unordered_map<std::string, std::string>;

  MockedDocument() = default;
  MockedDocument(std::string test_field) : hset_{{"field", test_field}} {
  }

  bool Check(DocumentAccessor::FieldConsumer f, string_view active_field) const override {
    if (!active_field.empty()) {
      auto it = hset_.find(string{active_field});
      return f(it != hset_.end() ? it->second : "");
    } else {
      for (const auto& [k, v] : hset_) {
        if (f(v))
          return true;
      }
      return false;
    }
  }

  void Set(Map hset) {
    hset_ = hset;
  }

 private:
  Map hset_{};
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

#define CHECK_ALL(...)                                                  \
  {                                                                     \
    for (auto str : {__VA_ARGS__}) {                                    \
      MockedDocument hset{str};                                         \
      EXPECT_TRUE(Check(&hset)) << str << " failed on " << DebugExpr(); \
    }                                                                   \
  }

#define CHECK_NONE(...)                                                  \
  {                                                                      \
    for (auto str : {__VA_ARGS__}) {                                     \
      MockedDocument hset{str};                                          \
      EXPECT_FALSE(Check(&hset)) << str << " failed on " << DebugExpr(); \
    }                                                                    \
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

  // Check basic cases
  CHECK_ALL("foo", "foo bar", "more foo bar");
  CHECK_NONE("wrong", "nomatch");

  // Check part of sentence + case.
  CHECK_ALL("Foo is cool.", "Where is foo?", "One. FOO!. More", "Foo is foo.");

  // Check part of word is not matched
  CHECK_NONE("foocool", "veryfoos", "ufoo", "morefoomore", "thefoo");
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
}

TEST_F(SearchParserTest, CheckParenthesisPriority) {
  ParseExpr("foo | -(bar baz)");

  CHECK_ALL("foo", "not b/r and b/z", "foo bar baz", "single bar", "only baz");
  CHECK_NONE("bar baz", "some more bar and baz");

  ParseExpr("( foo (bar | baz) (rab | zab) ) | true");

  CHECK_ALL("true", "foo bar rab", "foo baz zab", "foo bar zab");
  CHECK_NONE("wrong", "foo bar baz", "foo rab zab", "foo bar what", "foo rab foo");
}

TEST_F(SearchParserTest, MatchField) {
  ParseExpr("@f1:foo @f2:bar @f3:baz");

  MockedDocument doc{};

  doc.Set({{"f1", "foo"}, {"f2", "bar"}, {"f3", "baz"}});
  EXPECT_TRUE(Check(&doc));

  doc.Set({{"f1", "foo"}, {"f2", "bar"}, {"f3", "last is wrong"}});
  EXPECT_FALSE(Check(&doc));

  doc.Set({{"f1", "its"}, {"f2", "totally"}, {"f3", "wrong"}});
  EXPECT_FALSE(Check(&doc));

  doc.Set({{"f1", "im foo but its only me and"}, {"f2", "bar"}});
  EXPECT_FALSE(Check(&doc));

  doc.Set({});
  EXPECT_FALSE(Check(&doc));
}

TEST_F(SearchParserTest, MatchRange) {
  ParseExpr("@f1:[1 10] @f2:[50 100]");

  MockedDocument doc{};

  doc.Set({{"f1", "5"}, {"f2", "50"}});
  EXPECT_TRUE(Check(&doc));

  doc.Set({{"f1", "1"}, {"f2", "100"}});
  EXPECT_TRUE(Check(&doc));

  doc.Set({{"f1", "10"}, {"f2", "50"}});
  EXPECT_TRUE(Check(&doc));

  doc.Set({{"f1", "11"}, {"f2", "49"}});
  EXPECT_FALSE(Check(&doc));

  doc.Set({{"f1", "0"}, {"f2", "101"}});
  EXPECT_FALSE(Check(&doc));
}

TEST_F(SearchParserTest, CheckExprInField) {
  ParseExpr("@f1:(a|b) @f2:(c d) @f3:-e");

  MockedDocument doc{};

  doc.Set({{"f1", "a"}, {"f2", "c and d"}, {"f3", "right"}});
  EXPECT_TRUE(Check(&doc));

  doc.Set({{"f1", "b"}, {"f2", "d and c"}, {"f3", "ok"}});
  EXPECT_TRUE(Check(&doc));

  doc.Set({{"f1", "none"}, {"f2", "only d"}, {"f3", "ok"}});
  EXPECT_FALSE(Check(&doc));

  doc.Set({{"f1", "b"}, {"f2", "d and c"}, {"f3", "it has an e"}});
  EXPECT_FALSE(Check(&doc)) << DebugExpr();

  ParseExpr({"@f1:(a (b | c) -(d | e)) @f2:-(a|b)"});

  doc.Set({{"f1", "a b w"}, {"f2", "c"}});
  EXPECT_TRUE(Check(&doc));

  doc.Set({{"f1", "a b d"}, {"f2", "c"}});
  EXPECT_FALSE(Check(&doc));

  doc.Set({{"f1", "a b w"}, {"f2", "a"}});
  EXPECT_FALSE(Check(&doc));

  doc.Set({{"f1", "a w"}, {"f2", "c"}});
  EXPECT_FALSE(Check(&doc));
}

}  // namespace search

}  // namespace dfly
