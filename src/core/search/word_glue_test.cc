// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/word_glue.h"

#include <string>
#include <vector>

#include "base/gtest.h"
#include "base/logging.h"

namespace dfly::search {

using namespace std;
using TokenId = Parser::token;

namespace {

struct Spec {
  int kind;
  size_t begin, end;
  string text = {};
};

Parser::symbol_type MakeSym(const Spec& s) {
  Parser::location_type loc;
  switch (s.kind) {
    case TokenId::TOK_TERM:
      return Parser::make_TERM(s.text, loc);
    case TokenId::TOK_DOUBLE:
      return Parser::make_DOUBLE(s.text, loc);
    case TokenId::TOK_GLUE:
      return Parser::make_GLUE(loc);
    case TokenId::TOK_COMMA:
      return Parser::make_COMMA(loc);
    case TokenId::TOK_LBRACKET:
      return Parser::make_LBRACKET(loc);
    case TokenId::TOK_RBRACKET:
      return Parser::make_RBRACKET(loc);
    case TokenId::TOK_AS:
      return Parser::make_AS(loc);
    case TokenId::TOK_STAR:
      return Parser::make_STAR(loc);
  }
  return Parser::make_YYEOF(loc);
}

// Feeds `specs` through WordGlue and returns the kinds it yields, up to and including EOF.
vector<int> Filter(const vector<Spec>& specs, string_view input = {}) {
  size_t i = 0;
  WordGlue glue{[&] {
                  Spec s = i < specs.size() ? specs[i++] : Spec{TokenId::TOK_YYEOF, 0, 0};
                  return RawToken{MakeSym(s), s.begin, s.end};
                },
                input};
  vector<int> res;
  do {
    res.push_back(glue.Next().type_get());
  } while (res.back() != TokenId::TOK_YYEOF);
  return res;
}

}  // namespace

TEST(WordGlueTest, TokenStream) {
  constexpr int kTerm = TokenId::TOK_TERM, kGlue = TokenId::TOK_GLUE, kEof = TokenId::TOK_YYEOF;

  // x.y
  EXPECT_EQ(Filter({{kTerm, 0, 1, "x"}, {kGlue, 1, 2}, {kTerm, 2, 3, "y"}}),
            (vector<int>{kTerm, kGlue, kTerm, kEof}));
  // x.!.y: a run of separator tokens is one GLUE
  EXPECT_EQ(Filter({{kTerm, 0, 1, "x"},
                    {kGlue, 1, 2},
                    {TokenId::TOK_COMMA, 2, 3},
                    {kGlue, 3, 4},
                    {kTerm, 4, 5, "y"}}),
            (vector<int>{kTerm, kGlue, kTerm, kEof}));
  // x. y and x .y: whitespace next to the run drops it
  EXPECT_EQ(Filter({{kTerm, 0, 1, "x"}, {kGlue, 1, 2}, {kTerm, 3, 4, "y"}}),
            (vector<int>{kTerm, kTerm, kEof}));
  EXPECT_EQ(Filter({{kTerm, 0, 1, "x"}, {kGlue, 2, 3}, {kTerm, 3, 4, "y"}}),
            (vector<int>{kTerm, kTerm, kEof}));
  // [1,2]: a comma inside brackets is not a separator
  EXPECT_EQ(Filter({{TokenId::TOK_LBRACKET, 0, 1},
                    {TokenId::TOK_DOUBLE, 1, 2, "1"},
                    {TokenId::TOK_COMMA, 2, 3},
                    {TokenId::TOK_DOUBLE, 3, 4, "2"},
                    {TokenId::TOK_RBRACKET, 4, 5}}),
            (vector<int>{TokenId::TOK_LBRACKET, TokenId::TOK_DOUBLE, TokenId::TOK_COMMA,
                         TokenId::TOK_DOUBLE, TokenId::TOK_RBRACKET, kEof}));
  // x.as: a keyword inside a word becomes a term of its spelling
  EXPECT_EQ(Filter({{kTerm, 0, 1, "x"}, {kGlue, 1, 2}, {TokenId::TOK_AS, 2, 4}}, "x.as"),
            (vector<int>{kTerm, kGlue, kTerm, kEof}));
  // x.*
  EXPECT_THROW(Filter({{kTerm, 0, 1, "x"}, {kGlue, 1, 2}, {TokenId::TOK_STAR, 2, 3}}),
               Parser::syntax_error);
}

// A long run of separators before a word, adjacent or spaced out, is dropped.
TEST(WordGlueTest, LongSeparatorRun) {
  constexpr size_t kRun = 200'000;
  for (size_t gap : {1, 2}) {  // one run of adjacent separators, or separated by whitespace
    size_t i = 0;
    Parser::location_type loc;
    WordGlue glue{[&] {
                    size_t pos = i * gap;
                    ++i;
                    if (i <= kRun)
                      return RawToken{Parser::make_GLUE(loc), pos, pos + 1};
                    if (i == kRun + 1)
                      return RawToken{Parser::make_TERM("foo", loc), pos, pos + 3};
                    return RawToken{Parser::make_YYEOF(loc), pos, pos};
                  },
                  {}};
    EXPECT_EQ(glue.Next().type_get(), TokenId::TOK_TERM);
    EXPECT_EQ(glue.Next().type_get(), TokenId::TOK_YYEOF);
  }
}

}  // namespace dfly::search
