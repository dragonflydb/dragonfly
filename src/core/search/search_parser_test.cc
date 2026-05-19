// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "base/gtest.h"
#include "base/logging.h"
#include "core/search/base.h"
#include "core/search/query_driver.h"
#include "core/search/search.h"

namespace dfly::search {

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

  void SetParams(const QueryParams* params) {
    query_driver_.SetParams(params);
  }

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

TEST_F(SearchParserTest, Scanner) {
  SetInput("ab cd");
  // 3.5.1 does not have name() method.
  // EXPECT_STREQ("term", tok.name());

  NEXT_EQ(TOK_TERM, string, "ab");
  NEXT_EQ(TOK_TERM, string, "cd");
  NEXT_TOK(TOK_YYEOF);

  SetInput("*");
  NEXT_TOK(TOK_STAR);

  SetInput("(5a 6) ");
  NEXT_TOK(TOK_LPAREN);
  NEXT_EQ(TOK_TERM, string, "5a");
  NEXT_EQ(TOK_UINT32, string, "6");
  NEXT_TOK(TOK_RPAREN);

  SetInput(R"( "hello\"world" )");
  NEXT_EQ(TOK_TERM, string, R"(hello"world)");

  SetInput("@field:hello");
  NEXT_EQ(TOK_FIELD, string, "@field");
  NEXT_TOK(TOK_COLON);
  NEXT_EQ(TOK_TERM, string, "hello");

  SetInput("@field:{ tag }");
  NEXT_EQ(TOK_FIELD, string, "@field");
  NEXT_TOK(TOK_COLON);
  NEXT_TOK(TOK_LCURLBR);
  NEXT_EQ(TOK_TERM, string, "tag");
  NEXT_TOK(TOK_RCURLBR);

  // After the term lexer learned to handle backslash-escapes (\X anywhere in a
  // term), `{blue\,1\\\$\+}` and similar inputs are matched by the TERM rule
  // (same length as TAG_VAL on these inputs, term rule comes first). The
  // grammar accepts both TERM and TAG_VAL as `tag_list_element`, so the only
  // observable difference is the token type — the unescaped string is identical.
  SetInput("@color:{blue\\,1\\\\\\$\\+}");
  NEXT_EQ(TOK_FIELD, string, "@color");
  NEXT_TOK(TOK_COLON);
  NEXT_TOK(TOK_LCURLBR);
  NEXT_EQ(TOK_TERM, string, R"(blue,1\$+)");
  NEXT_TOK(TOK_RCURLBR);

  SetInput("@color:{blue\\.1\\\"\\%\\=}");
  NEXT_EQ(TOK_FIELD, string, "@color");
  NEXT_TOK(TOK_COLON);
  NEXT_TOK(TOK_LCURLBR);
  NEXT_EQ(TOK_TERM, string, "blue.1\"%=");
  NEXT_TOK(TOK_RCURLBR);

  SetInput("@color:{blue\\<1\\'\\^\\~}");
  NEXT_EQ(TOK_FIELD, string, "@color");
  NEXT_TOK(TOK_COLON);
  NEXT_TOK(TOK_LCURLBR);
  NEXT_EQ(TOK_TERM, string, "blue<1'^~");
  NEXT_TOK(TOK_RCURLBR);

  SetInput("@color:{blue\\>1\\:\\&\\/}");
  NEXT_EQ(TOK_FIELD, string, "@color");
  NEXT_TOK(TOK_COLON);
  NEXT_TOK(TOK_LCURLBR);
  NEXT_EQ(TOK_TERM, string, "blue>1:&/");
  NEXT_TOK(TOK_RCURLBR);

  SetInput("@color:{blue\\{1\\;\\*\\ }");
  NEXT_EQ(TOK_FIELD, string, "@color");
  NEXT_TOK(TOK_COLON);
  NEXT_TOK(TOK_LCURLBR);
  NEXT_EQ(TOK_TERM, string, "blue{1;* ");
  NEXT_TOK(TOK_RCURLBR);

  SetInput("@color:{blue\\}1\\!\\(}");
  NEXT_EQ(TOK_FIELD, string, "@color");
  NEXT_TOK(TOK_COLON);
  NEXT_TOK(TOK_LCURLBR);
  NEXT_EQ(TOK_TERM, string, "blue}1!(");
  NEXT_TOK(TOK_RCURLBR);

  SetInput("@color:{blue\\[1\\@\\)}");
  NEXT_EQ(TOK_FIELD, string, "@color");
  NEXT_TOK(TOK_COLON);
  NEXT_TOK(TOK_LCURLBR);
  NEXT_EQ(TOK_TERM, string, "blue[1@)");
  NEXT_TOK(TOK_RCURLBR);

  SetInput("@color:{blue\\]1\\#\\-}");
  NEXT_EQ(TOK_FIELD, string, "@color");
  NEXT_TOK(TOK_COLON);
  NEXT_TOK(TOK_LCURLBR);
  NEXT_EQ(TOK_TERM, string, "blue]1#-");
  NEXT_TOK(TOK_RCURLBR);

  // Colon in tag value (unescaped)
  SetInput("@t:{Tag:value}");
  NEXT_EQ(TOK_FIELD, string, "@t");
  NEXT_TOK(TOK_COLON);
  NEXT_TOK(TOK_LCURLBR);
  NEXT_EQ(TOK_TAG_VAL, string, "Tag:value");
  NEXT_TOK(TOK_RCURLBR);

  // Prefix simple
  SetInput("pre*");
  NEXT_EQ(TOK_PREFIX, string, "pre");

  // TODO: uncomment when we support escaped terms
  // Prefix escaped (redis doesn't support quoted prefix matches)
  // SetInput("pre\\**");
  // NEXT_EQ(TOK_PREFIX, string, "pre*");

  // Prefix in tag
  SetInput("@color:{prefix*}");
  NEXT_EQ(TOK_FIELD, string, "@color");
  NEXT_TOK(TOK_COLON);
  NEXT_TOK(TOK_LCURLBR);
  NEXT_EQ(TOK_PREFIX, string, "prefix");
  NEXT_TOK(TOK_RCURLBR);

  // Prefix escaped star
  SetInput("@color:{\"prefix*\"}");
  NEXT_EQ(TOK_FIELD, string, "@color");
  NEXT_TOK(TOK_COLON);
  NEXT_TOK(TOK_LCURLBR);
  NEXT_EQ(TOK_TERM, string, "prefix*");
  NEXT_TOK(TOK_RCURLBR);

  // Prefix spaced with star
  SetInput("pre *");
  NEXT_EQ(TOK_TERM, string, "pre");
  NEXT_TOK(TOK_STAR);

  SetInput("почтальон Печкин");
  NEXT_EQ(TOK_TERM, string, "почтальон");
  NEXT_EQ(TOK_TERM, string, "Печкин");

  SetInput("33.3");
  NEXT_EQ(TOK_DOUBLE, string, "33.3");
}

TEST_F(SearchParserTest, EscapedTagPrefixes) {
  SetInput("@name:{escape\\-err*}");
  NEXT_EQ(TOK_FIELD, string, "@name");
  NEXT_TOK(TOK_COLON);
  NEXT_TOK(TOK_LCURLBR);
  NEXT_EQ(TOK_PREFIX, string, "escape-err");
  NEXT_TOK(TOK_RCURLBR);

  SetInput("@name:{escape\\+pre*}");
  NEXT_EQ(TOK_FIELD, string, "@name");
  NEXT_TOK(TOK_COLON);
  NEXT_TOK(TOK_LCURLBR);
  NEXT_EQ(TOK_PREFIX, string, "escape+pre");
  NEXT_TOK(TOK_RCURLBR);

  SetInput("@name:{escape\\.pre*}");
  NEXT_EQ(TOK_FIELD, string, "@name");
  NEXT_TOK(TOK_COLON);
  NEXT_TOK(TOK_LCURLBR);
  NEXT_EQ(TOK_PREFIX, string, "escape.pre");
  NEXT_TOK(TOK_RCURLBR);

  SetInput("@name:{complex\\-escape\\+with\\.many\\*chars*}");
  NEXT_EQ(TOK_FIELD, string, "@name");
  NEXT_TOK(TOK_COLON);
  NEXT_TOK(TOK_LCURLBR);
  NEXT_EQ(TOK_PREFIX, string, "complex-escape+with.many*chars");
  NEXT_TOK(TOK_RCURLBR);
}

TEST_F(SearchParserTest, TildeScanner) {
  SetInput("~hello");
  NEXT_TOK(TOK_TILDE);
  NEXT_EQ(TOK_TERM, string, "hello");
  NEXT_TOK(TOK_YYEOF);

  SetInput("hello ~world");
  NEXT_EQ(TOK_TERM, string, "hello");
  NEXT_TOK(TOK_TILDE);
  NEXT_EQ(TOK_TERM, string, "world");
  NEXT_TOK(TOK_YYEOF);
}

TEST_F(SearchParserTest, EscapedTilde) {
  // \~hello must produce a literal-text TERM "~hello", not a TILDE+TERM pair.
  SetInput("\\~hello");
  NEXT_EQ(TOK_TERM, string, "~hello");
  NEXT_TOK(TOK_YYEOF);

  // foo\~bar — escape in middle of a term yields one TERM "foo~bar".
  SetInput("foo\\~bar");
  NEXT_EQ(TOK_TERM, string, "foo~bar");
  NEXT_TOK(TOK_YYEOF);

  // Escape other special chars too (\- and \|).
  SetInput("foo\\-bar");
  NEXT_EQ(TOK_TERM, string, "foo-bar");
  NEXT_TOK(TOK_YYEOF);

  SetInput("foo\\|bar");
  NEXT_EQ(TOK_TERM, string, "foo|bar");
  NEXT_TOK(TOK_YYEOF);

  // Tag-specific chars are now also escapable in term context.
  SetInput("foo\\,bar");
  NEXT_EQ(TOK_TERM, string, "foo,bar");
  NEXT_TOK(TOK_YYEOF);

  SetInput("foo\\$bar");
  NEXT_EQ(TOK_TERM, string, "foo$bar");
  NEXT_TOK(TOK_YYEOF);

  // Literal backslash via \\ — UnescapeTerm strips the leading \ producing a
  // single \ character. Also verifies UnescapeTerm doesn't trip its DCHECK.
  SetInput("foo\\\\bar");
  NEXT_EQ(TOK_TERM, string, "foo\\bar");
  NEXT_TOK(TOK_YYEOF);

  SetInput("\\\\");
  NEXT_EQ(TOK_TERM, string, "\\");
  NEXT_TOK(TOK_YYEOF);

  // Escape inside prefix/suffix/infix.
  SetInput("foo\\~*");
  NEXT_EQ(TOK_PREFIX, string, "foo~");
  NEXT_TOK(TOK_YYEOF);

  SetInput("*foo\\~");
  NEXT_EQ(TOK_SUFFIX, string, "foo~");
  NEXT_TOK(TOK_YYEOF);

  SetInput("*foo\\~bar*");
  NEXT_EQ(TOK_INFIX, string, "foo~bar");
  NEXT_TOK(TOK_YYEOF);
}

TEST_F(SearchParserTest, TildeParse) {
  // Simple optional term
  EXPECT_EQ(0, Parse("~hello"));
  // AND with optional
  EXPECT_EQ(0, Parse("hello ~world"));
  // Double optional
  EXPECT_EQ(0, Parse("~hello ~world"));
  // Optional with grouping
  EXPECT_EQ(0, Parse("~(hello world)"));
  // Optional prefix
  EXPECT_EQ(0, Parse("~hel*"));
  // Nested with NOT
  EXPECT_EQ(0, Parse("~-hello"));
  // Field-qualified optional — with and without parentheses
  EXPECT_EQ(0, Parse("@field:(~hello)"));
  EXPECT_EQ(0, Parse("@field:~hello"));
  EXPECT_EQ(0, Parse("@title:~hel*"));
  EXPECT_EQ(0, Parse("@title:~(hello world)"));
}

TEST_F(SearchParserTest, TildeInvalidGrammar) {
  // ~ cannot precede top-level KNN or vector-range constructs (they live at
  // final_query level, not search_unary_expr). Must be a clean syntax error.
  EXPECT_EQ(1, Parse("~*=>[KNN 3 @v vec]"));
  EXPECT_EQ(1, Parse("~@v:[VECTOR_RANGE 1 vec]"));

  // ~ followed by closing paren or empty group — syntax errors.
  EXPECT_EQ(1, Parse("~)"));
  EXPECT_EQ(1, Parse("~()"));

  // Bare ~ at end of input — syntax error.
  EXPECT_EQ(1, Parse("hello ~"));

  // ~ inside a tag list is NOT supported (per issue #7223).
  EXPECT_EQ(1, Parse("@tag:{~value}"));
}

TEST_F(SearchParserTest, Parse) {
  EXPECT_EQ(0, Parse(" foo bar (baz) "));
  EXPECT_EQ(0, Parse(" -(foo) @foo:bar @ss:[1 2]"));
  EXPECT_EQ(0, Parse("@foo:{ tag1 | tag2 }"));

  EXPECT_EQ(0, Parse("@foo:{1|2}"));
  EXPECT_EQ(0, Parse("@foo:{1|2.0|4|3.0}"));
  EXPECT_EQ(0, Parse("@foo:{1|hello|3.0|world|4}"));

  EXPECT_EQ(0, Parse("@name:{escape\\-err*}"));

  // Parenthesized star - used by LangChain for KNN queries (issue #6342)
  EXPECT_EQ(0, Parse("(*)"));
  EXPECT_EQ(0, Parse("((*))"));
  EXPECT_EQ(0, Parse("(((*)))"));

  // Colon in tag value
  EXPECT_EQ(0, Parse("@t:{Tag:value}"));
  EXPECT_EQ(0, Parse("@t:{Tag:*}"));
  EXPECT_EQ(0, Parse("@category:{Product:Electronics}"));

  EXPECT_EQ(1, Parse(" -(foo "));
  EXPECT_EQ(1, Parse(" foo:bar "));
  EXPECT_EQ(1, Parse(" @foo:@bar "));
  EXPECT_EQ(1, Parse(" @foo: "));

  EXPECT_EQ(0, Parse("*suffix"));
  EXPECT_EQ(0, Parse("*infix*"));

  EXPECT_EQ(1, Parse("pre***"));

  // Geo units
  EXPECT_EQ(0, Parse("@t:{km}"));
  EXPECT_EQ(0, Parse("@t:{Km|M}"));
  EXPECT_EQ(0, Parse("@t:{ft|mi}"));
  EXPECT_EQ(0, Parse("@location:[0.0 0.0 1 m]"));
  EXPECT_EQ(0, Parse("@location:[0.0 0.0 1 Km]"));
  EXPECT_EQ(1, Parse("@location:[0.0 0.0 1 yd]"));
}

TEST_F(SearchParserTest, ParseParams) {
  QueryParams params;
  params["k"] = "10";
  params["name"] = "alex";
  SetParams(&params);

  SetInput("$name $k");
  NEXT_EQ(TOK_TERM, string, "alex");
  NEXT_EQ(TOK_UINT32, string, "10");
}

TEST_F(SearchParserTest, Quotes) {
  SetInput(" \"fir  st\"  'sec@o@nd' \":third:\" 'four\\\"th' ");
  NEXT_EQ(TOK_TERM, string, "fir  st");
  NEXT_EQ(TOK_TERM, string, "sec@o@nd");
  NEXT_EQ(TOK_TERM, string, ":third:");
  NEXT_EQ(TOK_TERM, string, "four\"th");
}

TEST_F(SearchParserTest, Numeric) {
  SetInput("11 123123123123 '22'");
  NEXT_EQ(TOK_UINT32, string, "11");
  NEXT_EQ(TOK_DOUBLE, string, "123123123123");
  NEXT_EQ(TOK_TERM, string, "22");
}

TEST_F(SearchParserTest, VectorRange) {
  // Full vector range query tokenization
  SetInput("@vector:[VECTOR_RANGE $radius $vec]=>{$YIELD_DISTANCE_AS: dist}");
  NEXT_EQ(TOK_FIELD, string, "@vector");
  NEXT_TOK(TOK_COLON);
  NEXT_TOK(TOK_LBRACKET);
  NEXT_TOK(TOK_VECTOR_RANGE);
}

TEST_F(SearchParserTest, VectorRangeParse) {
  QueryParams params;
  params["radius"] = "1";
  // 4 bytes = one float dimension
  params["vec"] = std::string(4, '\0');
  SetParams(&params);

  // Basic syntax parses without error
  EXPECT_EQ(0, Parse("@f:[VECTOR_RANGE $radius $vec]=>{$YIELD_DISTANCE_AS: dist}"));
}

TEST_F(SearchParserTest, KNN) {
  SetInput("*=>[KNN 1 @vector field_vec]");
  NEXT_TOK(TOK_STAR);
  NEXT_TOK(TOK_ARROW);
  NEXT_TOK(TOK_LBRACKET);
}

TEST_F(SearchParserTest, KNNfull) {
  SetInput("*=>[Knn 1 @vector field_vec EF_Runtime 15 as vec_sort]");
  NEXT_TOK(TOK_STAR);
  NEXT_TOK(TOK_ARROW);
  NEXT_TOK(TOK_LBRACKET);

  NEXT_TOK(TOK_KNN);
  NEXT_EQ(TOK_UINT32, string, "1");
  NEXT_TOK(TOK_FIELD);
  NEXT_TOK(TOK_TERM);

  NEXT_TOK(TOK_EF_RUNTIME);
  NEXT_EQ(TOK_UINT32, string, "15");

  NEXT_TOK(TOK_AS);
  NEXT_EQ(TOK_TERM, string, "vec_sort");

  NEXT_TOK(TOK_RBRACKET);
}

}  // namespace dfly::search
