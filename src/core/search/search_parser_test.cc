// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <absl/strings/str_cat.h>

#include <array>

#include "base/gtest.h"
#include "base/logging.h"
#include "core/search/base.h"
#include "core/search/query_driver.h"
#include "core/search/search.h"

namespace dfly::search {

using namespace std;

// Compact textual form of a parsed query tree, used to pin exact ASTs in tests.
struct AstDumper {
  string operator()(monostate) const {
    return "<empty>";
  }
  string operator()(const AstStarNode&) const {
    return "*";
  }
  string operator()(const AstStarFieldNode&) const {
    return "@*";
  }
  string operator()(const AstTermNode& n) const {
    return absl::StrCat("T(", n.affix, ")");
  }
  string operator()(const AstPrefixNode& n) const {
    return absl::StrCat("P(", n.affix, ")");
  }
  string operator()(const AstSuffixNode& n) const {
    return absl::StrCat("S(", n.affix, ")");
  }
  string operator()(const AstInfixNode& n) const {
    return absl::StrCat("I(", n.affix, ")");
  }
  string operator()(const AstWildcardNode& n) const {
    return absl::StrCat("W(", n.affix, ")");
  }
  string operator()(const AstPhraseNode& n) const {
    return absl::StrCat("PH(", n.raw, "~", n.slop, ")");
  }
  string operator()(const AstRangeNode& n) const {
    return absl::StrCat("R[", n.lo, ",", n.hi, "]");
  }
  string operator()(const AstGeoNode& n) const {
    return absl::StrCat("GEO(", n.lon, ",", n.lat, ",", n.radius, n.unit, ")");
  }
  string operator()(const AstNegateNode& n) const {
    return absl::StrCat("NOT(", Dump(*n.node), ")");
  }
  string operator()(const AstOptionalNode& n) const {
    return absl::StrCat("OPT(", Dump(*n.node), ")");
  }
  string operator()(const AstAttributeNode& n) const {
    return absl::StrCat("ATTR(", Dump(*n.node), ",w=", n.weight, ")");
  }
  string operator()(const AstLogicalNode& n) const {
    string res = n.op == AstLogicalNode::AND ? "AND{" : "OR{";
    for (size_t i = 0; i < n.nodes.size(); ++i)
      absl::StrAppend(&res, i ? " " : "", Dump(n.nodes[i]));
    return res + "}";
  }
  string operator()(const AstFieldNode& n) const {
    return absl::StrCat(n.field, ":", Dump(*n.node));
  }
  string operator()(const AstTagsNode& n) const {
    string res = "TAGS{";
    for (size_t i = 0; i < n.tags.size(); ++i)
      absl::StrAppend(&res, i ? "|" : "", visit(*this, n.tags[i]));
    return res + "}";
  }
  string operator()(const AstKnnNode& n) const {
    return absl::StrCat("KNN(", n.filter ? Dump(*n.filter) : "", ";", n.limit, ";", n.field, ";",
                        n.score_alias, ")");
  }
  string operator()(const AstVectorRangeNode& n) const {
    return absl::StrCat("VRANGE(", n.field, ";", n.radius, ")");
  }

  string Dump(const AstNode& n) const {
    return visit(*this, static_cast<const NodeVariants&>(n));
  }
};

string DumpAst(const AstNode& n) {
  return AstDumper{}.Dump(n);
}

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

  // The dumped AST of `str`, or "error" if it does not parse.
  string ParseDump(const string& str) {
    try {
      if (Parse(str) != 0)
        return "error";
    } catch (const std::exception&) {
      return "error";
    }
    return DumpAst(query_driver_.Take());
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

#define NEXT_PHRASE(raw_val, slop_val)                    \
  {                                                       \
    auto tok = Lex();                                     \
    ASSERT_EQ(tok.type_get(), Parser::token::TOK_PHRASE); \
    const auto& pt = tok.value.as<PhraseTok>();           \
    EXPECT_EQ(pt.raw, raw_val);                           \
    EXPECT_EQ(pt.slop, static_cast<uint32_t>(slop_val));  \
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
  NEXT_PHRASE(R"(hello\"world)", 0);

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
  // Lexer always emits PHRASE for quoted strings; the tag grammar treats it as a literal term.
  NEXT_PHRASE("prefix*", 0);
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
  // ~ cannot precede a top-level KNN construct (it lives at final_query level, not
  // search_unary_expr). Must be a clean syntax error.
  EXPECT_EQ(1, Parse("~*=>[KNN 3 @v vec]"));

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
  // Quoted strings tokenize to PHRASE (distinct from TERM) so the grammar can route them
  // into AstPhraseNode for exact phrase queries. See PHRASE_QUERIES_PLAN.md.
  SetInput(" \"fir  st\"  'sec@o@nd' \":third:\" 'four\\\"th' ");
  NEXT_PHRASE("fir  st", 0);
  NEXT_PHRASE("sec@o@nd", 0);
  NEXT_PHRASE(":third:", 0);
  NEXT_PHRASE("four\\\"th", 0);
}

TEST_F(SearchParserTest, Numeric) {
  SetInput("11 123123123123 '22'");
  NEXT_EQ(TOK_UINT32, string, "11");
  NEXT_EQ(TOK_DOUBLE, string, "123123123123");
  // '22' is a quoted single-token phrase — still a phrase lit, executor will treat it as 1-token.
  NEXT_PHRASE("22", 0);
}

TEST_F(SearchParserTest, VectorRange) {
  // Full vector range query tokenization
  SetInput("@vector:[VECTOR_RANGE $radius $vec]=>{$YIELD_DISTANCE_AS: dist}");
  NEXT_EQ(TOK_FIELD, string, "@vector");
  NEXT_TOK(TOK_COLON);
  NEXT_TOK(TOK_LBRACKET);
  NEXT_TOK(TOK_VECTOR_RANGE);
}

TEST_F(SearchParserTest, WeightAttributes) {
  QueryParams params;
  params["w"] = "5.0";
  params["radius"] = "1";
  params["vec"] = std::string(4, '\0');
  SetParams(&params);

  SetInput("@name:(mal)=>{$weight:5.0}");
  NEXT_EQ(TOK_FIELD, string, "@name");
  NEXT_TOK(TOK_COLON);
  NEXT_TOK(TOK_LPAREN);
  NEXT_EQ(TOK_TERM, string, "mal");
  NEXT_TOK(TOK_RPAREN);
  NEXT_TOK(TOK_ATTR_ARROW);
  NEXT_TOK(TOK_WEIGHT);
  NEXT_TOK(TOK_COLON);
  NEXT_EQ(TOK_DOUBLE, string, "5.0");
  NEXT_TOK(TOK_RCURLBR);

  EXPECT_EQ(0, Parse("@name:(mal)=>{$weight:5.0}"));
  auto ast = query_driver_.Take();
  ASSERT_TRUE(std::holds_alternative<AstAttributeNode>(ast.Variant()));
  const auto& attr = std::get<AstAttributeNode>(ast.Variant());
  EXPECT_EQ(attr.weight, 5.0);
  EXPECT_TRUE(std::holds_alternative<AstFieldNode>(attr.node->Variant()));

  EXPECT_EQ(0, Parse("@name:(mal) => { $weight: 5.0 }"));
  EXPECT_EQ(0, Parse("(@country:(mal)=>{$weight:20.0} | @city:(mal)=>{$weight:10.0})"));
  EXPECT_EQ(0, Parse("@name:(mal)=>{$weight:$w}"));
  EXPECT_EQ(0, Parse("@active:{true}=>{$weight:2}"));
  EXPECT_EQ(0, Parse("@f:[VECTOR_RANGE $radius $vec]=>{$weight:2}"));
  EXPECT_EQ(0, Parse("(@f:[VECTOR_RANGE $radius $vec])=>{$weight:2}"));
  EXPECT_NE(0, Parse("*=>{$weight:2}"));
}

TEST_F(SearchParserTest, PerTermWeightInFieldGroup) {
  QueryParams params;
  params["w"] = "5.0";
  SetParams(&params);

  // Per-term weight inside a field group (as emitted by clients that weight individual words).
  EXPECT_EQ(0, Parse("@description:(machine=>{$weight:2.0} | learning)"));
  EXPECT_EQ(0, Parse("@description:(machine=>{$weight:2.0})"));
  EXPECT_EQ(0, Parse("@description:(machine=>{$weight:2.0} networks)"));
  EXPECT_EQ(0, Parse("@f:(a=>{$weight:2} | b=>{$weight:3} | c)"));
  EXPECT_EQ(0, Parse("@f:(a=>{$weight:$w})"));
  EXPECT_NE(0, Parse("@f:(*=>{$weight:2})"));

  // The weighted term stays scoped to the field: Field{ Or[ Attribute{Term}, Term ] }.
  EXPECT_EQ(0, Parse("@description:(machine=>{$weight:2.0} | learning)"));
  auto ast = query_driver_.Take();
  ASSERT_TRUE(std::holds_alternative<AstFieldNode>(ast.Variant()));
  const auto& field = std::get<AstFieldNode>(ast.Variant());
  EXPECT_EQ(field.field, "description");
  ASSERT_TRUE(std::holds_alternative<AstLogicalNode>(field.node->Variant()));
  const auto& logical = std::get<AstLogicalNode>(field.node->Variant());
  EXPECT_EQ(logical.op, AstLogicalNode::OR);
  ASSERT_EQ(logical.nodes.size(), 2u);
  ASSERT_TRUE(std::holds_alternative<AstAttributeNode>(logical.nodes[0].Variant()));
  const auto& weighted = std::get<AstAttributeNode>(logical.nodes[0].Variant());
  EXPECT_EQ(weighted.weight, 2.0);
  EXPECT_TRUE(std::holds_alternative<AstTermNode>(weighted.node->Variant()));
}

TEST_F(SearchParserTest, VectorRangeParse) {
  QueryParams params;
  params["radius"] = "1";
  // 4 bytes = one float dimension
  params["vec"] = std::string(4, '\0');
  SetParams(&params);

  // Basic syntax parses without error
  EXPECT_EQ(0, Parse("@f:[VECTOR_RANGE $radius $vec]=>{$YIELD_DISTANCE_AS: dist}"));
  EXPECT_EQ(0, Parse("@f:[VECTOR_RANGE $radius $vec]=>{$EPSILON: 0.1}"));
  EXPECT_EQ(0, Parse("@f:[VECTOR_RANGE $radius $vec]=>{$EPSILON: 0.1; $YIELD_DISTANCE_AS: dist}"));
  EXPECT_EQ(0, Parse("@f:[VECTOR_RANGE $radius $vec]=>{$YIELD_DISTANCE_AS: dist; $EPSILON: 0.1}"));
  EXPECT_NE(0, Parse("@f:[VECTOR_RANGE $radius $vec]=>{$EPSILON: 0}"));
  EXPECT_NE(0, Parse("@f:[VECTOR_RANGE $radius $vec]=>{$EPSILON: 2000000}"));
  EXPECT_NE(0, Parse("@f:[VECTOR_RANGE $radius $vec]=>{$EPSILON: inf}"));
  EXPECT_NE(0, Parse("*=>[KNN 1 @f $vec]=>{$EPSILON: 0.1}"));
}

TEST_F(SearchParserTest, VectorRangeCombinations) {
  QueryParams params;
  params["radius"] = "1";
  // 4 bytes = one float dimension
  params["vec"] = std::string(4, '\0');
  SetParams(&params);

  // VECTOR_RANGE is a regular predicate: parenthesized, AND-ed in any order, without parens,
  // OR-ed, negated and nested must all parse.
  EXPECT_EQ(0, Parse("(@f:[VECTOR_RANGE $radius $vec]=>{$YIELD_DISTANCE_AS: dist})"));
  EXPECT_EQ(0, Parse("(@f:[VECTOR_RANGE $radius $vec])"));
  EXPECT_EQ(0, Parse("(@f:[VECTOR_RANGE $radius $vec]=>{$YIELD_DISTANCE_AS: dist} @name:(idxA))"));
  EXPECT_EQ(0, Parse("(@name:(idxA) @f:[VECTOR_RANGE $radius $vec]=>{$YIELD_DISTANCE_AS: dist})"));
  EXPECT_EQ(0, Parse("@f:[VECTOR_RANGE $radius $vec]=>{$YIELD_DISTANCE_AS: dist} @name:(idxA)"));
  EXPECT_EQ(0, Parse("@f:[VECTOR_RANGE $radius $vec]=>{$YIELD_DISTANCE_AS: dist} | @name:(idxA)"));
  EXPECT_EQ(0, Parse("-@f:[VECTOR_RANGE $radius $vec]"));
  EXPECT_EQ(0, Parse("~@f:[VECTOR_RANGE $radius $vec]"));
  EXPECT_EQ(0, Parse("((@f:[VECTOR_RANGE $radius $vec]=>{$YIELD_DISTANCE_AS: dist}))"));

  // A lone (parenthesized) range still reduces to the range node itself.
  EXPECT_EQ(0, Parse("(@f:[VECTOR_RANGE $radius $vec])"));
  auto ast = query_driver_.Take();
  EXPECT_TRUE(std::holds_alternative<AstVectorRangeNode>(ast.Variant()));
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

TEST_F(SearchParserTest, KnnQueryAttributes) {
  QueryParams params;
  params["k"] = "3";
  params["ef"] = "25";
  // 4 bytes = one float dimension
  params["vec"] = std::string(4, '\0');
  SetParams(&params);

  EXPECT_EQ(0, Parse("*=>[KNN $k @vector $vec EF_RUNTIME 7 AS inline_score]"
                     "=>{$EF_RUNTIME: $ef; $YIELD_DISTANCE_AS: attr_score}"));

  auto ast = query_driver_.Take();
  ASSERT_TRUE(std::holds_alternative<AstKnnNode>(ast.Variant()));
  const auto& knn = std::get<AstKnnNode>(ast.Variant());
  EXPECT_EQ(knn.limit, 3u);
  EXPECT_EQ(knn.score_alias, "attr_score");
  ASSERT_TRUE(knn.ef_runtime);
  EXPECT_EQ(*knn.ef_runtime, 25u);
}

TEST_F(SearchParserTest, PhraseSlopLex) {
  // `"..."~N` (no whitespace before ~) → PHRASE token with slop=N.
  SetInput("\"foo bar\"~3");
  NEXT_PHRASE("foo bar", 3);

  // Whitespace between `"..."` and `~` blocks slop attachment: ~ is the unary optional operator.
  SetInput("\"foo bar\" ~3");
  NEXT_PHRASE("foo bar", 0);
  NEXT_TOK(TOK_TILDE);
  NEXT_EQ(TOK_UINT32, string, "3");

  // Slop digits must be unsigned; multi-digit works.
  SetInput("\"a b\"~123");
  NEXT_PHRASE("a b", 123);
}

TEST_F(SearchParserTest, PhraseSlopParse) {
  ASSERT_EQ(0, Parse("\"machine learning\"~2"));
  AstExpr root = query_driver_.Take();
  ASSERT_TRUE(std::holds_alternative<AstPhraseNode>(root));
  const auto& p = std::get<AstPhraseNode>(root);
  EXPECT_EQ(p.raw, "machine learning");
  EXPECT_EQ(p.slop, 2u);

  // Field-scoped slop.
  ASSERT_EQ(0, Parse("@title:\"machine learning\"~5"));
  AstExpr field = query_driver_.Take();
  ASSERT_TRUE(std::holds_alternative<AstFieldNode>(field));
  const auto& fnode = std::get<AstFieldNode>(field);
  ASSERT_TRUE(std::holds_alternative<AstPhraseNode>(*fnode.node));
  EXPECT_EQ(std::get<AstPhraseNode>(*fnode.node).slop, 5u);
}

// Slop inside a tag value (@tag:{"foo"~N}) is meaningless and must be rejected, not silently
// dropped. Bare quoted tag values are still allowed and become literal AstTermNode.
TEST_F(SearchParserTest, PhraseSlopRejectedInTagContext) {
  // Bare quoted tag parses successfully.
  EXPECT_EQ(0, Parse("@t:{\"foo bar\"}"));

  // Slop in tag context — parser action throws Parser::syntax_error which bison converts to a
  // non-zero parse result code.
  EXPECT_NE(0, Parse("@t:{\"foo\"~2}"));
}

// Quoted tag values must process one layer of \X escapes, mirroring the unquoted tag path,
// so values containing backslashes or quotes round-trip between HSET and FT.SEARCH.
TEST_F(SearchParserTest, QuotedTagEscapes) {
  auto tag_affix = [this](const string& query) -> string {
    EXPECT_EQ(0, Parse(query));
    AstExpr e = query_driver_.Take();
    EXPECT_TRUE(std::holds_alternative<AstFieldNode>(e));
    const AstNode& tags = *std::get<AstFieldNode>(e).node;
    EXPECT_TRUE(std::holds_alternative<AstTagsNode>(tags));
    const auto& tn = std::get<AstTagsNode>(tags);
    EXPECT_EQ(tn.tags.size(), 1u);
    EXPECT_TRUE(std::holds_alternative<AstTermNode>(tn.tags[0]));
    return std::get<AstTermNode>(tn.tags[0]).affix;
  };

  // Recognized escapes \\ and \" resolve to their single-character values.
  EXPECT_EQ(tag_affix(R"(@t:{"tnt\\backslash"})"), R"(tnt\backslash)");
  EXPECT_EQ(tag_affix(R"(@t:{"tnt\"quote"})"), "tnt\"quote");

  // Characterization: one layer of \X is stripped for any X, mirroring the unquoted make_Tag
  // path, so an unrecognized escape drops its backslash rather than passing through verbatim.
  // This is an intentional policy change from v1.39's verbatim behavior; pinning it keeps a
  // future reader from "restoring" the old semantics.
  EXPECT_EQ(tag_affix(R"(@t:{"ACME\jdoe"})"), "ACMEjdoe");

  // A trailing lone backslash (e.g. a Windows path) must not abort — it previously tripped the
  // DCHECK in UnescapeTerm — and drops the dangling backslash; the doubled form is a literal `\`.
  EXPECT_EQ(tag_affix(R"(@t:{"C:\"})"), "C:");
  EXPECT_EQ(tag_affix(R"(@t:{"C:\\"})"), R"(C:\)");
}

TEST_F(SearchParserTest, PhraseParse) {
  // Top-level phrase.
  ASSERT_EQ(0, Parse("\"machine learning\""));
  AstExpr root = query_driver_.Take();
  ASSERT_TRUE(std::holds_alternative<AstPhraseNode>(root));
  EXPECT_EQ(std::get<AstPhraseNode>(root).raw, "machine learning");

  // Phrase inside @field:"..."
  ASSERT_EQ(0, Parse("@title:\"fully convolutional network\""));
  AstExpr field = query_driver_.Take();
  ASSERT_TRUE(std::holds_alternative<AstFieldNode>(field));
  const auto& fnode = std::get<AstFieldNode>(field);
  EXPECT_EQ(fnode.field, "title");
  ASSERT_TRUE(std::holds_alternative<AstPhraseNode>(*fnode.node));
  EXPECT_EQ(std::get<AstPhraseNode>(*fnode.node).raw, "fully convolutional network");

  // Single-quoted single token still becomes a phrase (executor handles 1-token case).
  ASSERT_EQ(0, Parse("'foo'"));
  AstExpr single = query_driver_.Take();
  ASSERT_TRUE(std::holds_alternative<AstPhraseNode>(single));

  // Phrase combined with AND of free terms — RAG-style "...AND \"...\"" queries.
  ASSERT_EQ(0, Parse("machine \"deep learning\""));
  AstExpr and_root = query_driver_.Take();
  ASSERT_TRUE(std::holds_alternative<AstLogicalNode>(and_root));
  const auto& log = std::get<AstLogicalNode>(and_root);
  EXPECT_EQ(log.op, AstLogicalNode::AND);
  bool found_phrase = false;
  for (const auto& child : log.nodes) {
    if (std::holds_alternative<AstPhraseNode>(child)) {
      found_phrase = true;
      EXPECT_EQ(std::get<AstPhraseNode>(child).raw, "deep learning");
    }
  }
  EXPECT_TRUE(found_phrase);
}

TEST_F(SearchParserTest, WildcardLex) {
  // Lowercase `w` marks a glob; one layer of `\` escapes is stripped at lex time.
  SetInput("w'hel*'");
  NEXT_EQ(TOK_WILDCARD, string, "hel*");
  NEXT_TOK(TOK_YYEOF);

  SetInput(R"(w'hel\*')");
  NEXT_EQ(TOK_WILDCARD, string, "hel*");
  NEXT_TOK(TOK_YYEOF);

  SetInput(R"(w'hel\\*')");
  NEXT_EQ(TOK_WILDCARD, string, R"(hel\*)");
  NEXT_TOK(TOK_YYEOF);

  SetInput("w\"h?o\"");
  NEXT_EQ(TOK_WILDCARD, string, "h?o");
  NEXT_TOK(TOK_YYEOF);

  // A trailing lone backslash must not abort (previously tripped a DCHECK in UnescapeTerm); the
  // dangling backslash is dropped, so `w"a\"` and `w'a\'` both yield `a`.
  SetInput(R"(w"a\")");
  NEXT_EQ(TOK_WILDCARD, string, "a");
  NEXT_TOK(TOK_YYEOF);

  SetInput(R"(w'a\')");
  NEXT_EQ(TOK_WILDCARD, string, "a");
  NEXT_TOK(TOK_YYEOF);

  // Uppercase `W` is not a wildcard: it rewinds to a term followed by a phrase.
  SetInput("W'HEL*'");
  NEXT_EQ(TOK_TERM, string, "W");
  NEXT_PHRASE("HEL*", 0);
}

TEST_F(SearchParserTest, WildcardParse) {
  ASSERT_EQ(0, Parse("w'hel*'"));
  AstExpr root = query_driver_.Take();
  ASSERT_TRUE(std::holds_alternative<AstWildcardNode>(root));
  EXPECT_EQ(std::get<AstWildcardNode>(root).affix, "hel*");

  // After a field colon, both bare and parenthesized.
  ASSERT_EQ(0, Parse("@title:w'h?llo'"));
  AstExpr field = query_driver_.Take();
  ASSERT_TRUE(std::holds_alternative<AstFieldNode>(field));
  ASSERT_TRUE(std::holds_alternative<AstWildcardNode>(*std::get<AstFieldNode>(field).node));
  ASSERT_EQ(0, Parse("@title:(w'h?llo')"));

  // As a tag value.
  ASSERT_EQ(0, Parse("@tag:{w'hel*'}"));
  AstExpr tag_field = query_driver_.Take();
  ASSERT_TRUE(std::holds_alternative<AstFieldNode>(tag_field));
  const AstNode& tags = *std::get<AstFieldNode>(tag_field).node;
  ASSERT_TRUE(std::holds_alternative<AstTagsNode>(tags));
  const auto& tn = std::get<AstTagsNode>(tags);
  ASSERT_EQ(tn.tags.size(), 1u);
  EXPECT_TRUE(std::holds_alternative<AstWildcardNode>(tn.tags[0]));
}

// A parenthesized field condition must accept the same atoms as the bare `@field:...` form.
TEST_F(SearchParserTest, FieldParenthesizedAtoms) {
  auto field_child = [this](const std::string& q) -> AstNode {
    EXPECT_EQ(0, Parse(q)) << q;
    AstExpr e = query_driver_.Take();
    if (auto* f = std::get_if<AstFieldNode>(&e))
      return std::move(*f->node);
    ADD_FAILURE() << "not a field node: " << q;
    return e;
  };

  for (const auto& q : {R"(@prefix:("hello"))"s, R"((@prefix:("hello")))"s}) {
    AstNode n = field_child(q);
    ASSERT_TRUE(std::holds_alternative<AstPhraseNode>(n)) << q;
    EXPECT_EQ(std::get<AstPhraseNode>(n).raw, "hello");
    EXPECT_EQ(std::get<AstPhraseNode>(n).slop, 0u);
  }

  {
    AstNode n = field_child(R"(@title:("machine learning"~2))");
    ASSERT_TRUE(std::holds_alternative<AstPhraseNode>(n));
    EXPECT_EQ(std::get<AstPhraseNode>(n).raw, "machine learning");
    EXPECT_EQ(std::get<AstPhraseNode>(n).slop, 2u);
  }

  {
    AstNode n = field_child("@prefix:(hel*)");
    ASSERT_TRUE(std::holds_alternative<AstPrefixNode>(n));
    EXPECT_EQ(std::get<AstPrefixNode>(n).affix, "hel");
  }
  {
    AstNode n = field_child("@prefix:(*llo)");
    ASSERT_TRUE(std::holds_alternative<AstSuffixNode>(n));
    EXPECT_EQ(std::get<AstSuffixNode>(n).affix, "llo");
  }
  {
    AstNode n = field_child("@prefix:(*ell*)");
    ASSERT_TRUE(std::holds_alternative<AstInfixNode>(n));
    EXPECT_EQ(std::get<AstInfixNode>(n).affix, "ell");
  }

  {
    AstNode n = field_child(R"(@prefix:(-"world"))");
    ASSERT_TRUE(std::holds_alternative<AstNegateNode>(n));
    EXPECT_TRUE(std::holds_alternative<AstPhraseNode>(*std::get<AstNegateNode>(n).node));
  }
  {
    AstNode n = field_child(R"(@prefix:(~"hello"))");
    ASSERT_TRUE(std::holds_alternative<AstOptionalNode>(n));
    EXPECT_TRUE(std::holds_alternative<AstPhraseNode>(*std::get<AstOptionalNode>(n).node));
  }

  {
    AstNode n = field_child(R"(@title:("machine" | "deep"))");
    ASSERT_TRUE(std::holds_alternative<AstLogicalNode>(n));
    EXPECT_EQ(std::get<AstLogicalNode>(n).op, AstLogicalNode::OR);
    EXPECT_EQ(std::get<AstLogicalNode>(n).nodes.size(), 2u);
  }
  {
    AstNode n = field_child(R"(@title:("machine" "learning"))");
    ASSERT_TRUE(std::holds_alternative<AstLogicalNode>(n));
    EXPECT_EQ(std::get<AstLogicalNode>(n).op, AstLogicalNode::AND);
    EXPECT_EQ(std::get<AstLogicalNode>(n).nodes.size(), 2u);
  }

  EXPECT_EQ(0, Parse(R"((@prefix:("u123\.documents") @key:{doc1}))"));

  // `*` is the field-level "match all" only as the bare form, not as an atom in the grouping.
  EXPECT_NE(0, Parse("@prefix:(*)"));
}

// A decimal literal tokenizes to DOUBLE and must be usable as a search term, both free-standing
// and after a field colon, not only inside numeric ranges.
TEST_F(SearchParserTest, DoubleAsTerm) {
  ASSERT_EQ(0, Parse("3.14"));
  AstExpr root = query_driver_.Take();
  ASSERT_TRUE(std::holds_alternative<AstTermNode>(root));
  EXPECT_EQ(std::get<AstTermNode>(root).affix, "3.14");

  ASSERT_EQ(0, Parse("@title:3.14"));
  AstExpr field = query_driver_.Take();
  ASSERT_TRUE(std::holds_alternative<AstFieldNode>(field));
  ASSERT_TRUE(std::holds_alternative<AstTermNode>(*std::get<AstFieldNode>(field).node));
  EXPECT_EQ(std::get<AstTermNode>(*std::get<AstFieldNode>(field).node).affix, "3.14");

  // Decimal as a term inside a parenthesized field condition.
  ASSERT_EQ(0, Parse("@title:(3.14)"));
  AstExpr paren = query_driver_.Take();
  ASSERT_TRUE(std::holds_alternative<AstFieldNode>(paren));
  EXPECT_TRUE(std::holds_alternative<AstTermNode>(*std::get<AstFieldNode>(paren).node));
}

// A failed parse on a reused driver must not leave the previous query's AST behind: ResetScanner()
// clears it, so a syntax error (which never calls Set()) yields an empty result, not stale state.
TEST_F(SearchParserTest, ResetClearsStaleAst) {
  ASSERT_EQ(0, Parse("@field:hello"));
  ASSERT_TRUE(std::holds_alternative<AstFieldNode>(query_driver_.Take()));

  EXPECT_NE(0, Parse("("));
  EXPECT_TRUE(std::holds_alternative<std::monostate>(query_driver_.Take()));
}

// Word atoms joined by separator characters without whitespace form one glued word.
TEST_F(SearchParserTest, GluedWordAst) {
  QueryParams params;
  params["v"] = "abcd";
  params["n"] = "10";
  SetParams(&params);

  for (char sep : string{".,!#&=<>^?+/'"}) {
    string query = absl::StrCat("x", string(1, sep), "y");
    EXPECT_EQ(ParseDump(query), "AND{T(x) T(y)}") << query;
    EXPECT_EQ(ParseDump("@f:" + query), "f:AND{T(x) T(y)}") << query;
    string run = absl::StrCat("@f:x", string(1, sep), ",y");  // a comma later in the run
    EXPECT_EQ(ParseDump(run), "f:AND{T(x) T(y)}") << run;
  }

  const pair<string, string> kCases[] = {
      {"@email:example.com", "email:AND{T(example) T(com)}"},
      {"@email:*example.com*", "email:AND{S(example) P(com)}"},
      {"-@email:example.com", "NOT(email:AND{T(example) T(com)})"},
      {"@email:jane.2024.example.com", "email:AND{T(jane) T(2024) T(example) T(com)}"},
      {"@email:jane.2x.com", "email:AND{T(jane) T(2x) T(com)}"},
      {"@email:v1.2.3*", "email:AND{T(v1) T(2) P(3)}"},
      {"@email:a.b.5c*", "email:AND{T(a) T(b) P(5c)}"},
      {"@email:*0.0.1*", "email:AND{S(0) T(0) P(1)}"},
      {"@email:.5*", "email:P(5)"},
      {"@email:.example", "email:T(example)"},
      {"@email:example.", "email:T(example)"},
      {"@email:example..com", "email:AND{T(example) T(com)}"},
      {"foo.5 x.y", "AND{T(foo) T(.5) AND{T(x) T(y)}}"},
      {"@email:(example.com | john.doe)", "email:OR{AND{T(example) T(com)} AND{T(john) T(doe)}}"},
      {"@email:(example.com=>{$weight:2})", "email:ATTR(AND{T(example) T(com)},w=2)"},
      {"@email:example.com.=>[KNN 10 @vec $v AS d]", "KNN(email:AND{T(example) T(com)};10;vec;d)"},
      {"*=>.[KNN 10 @vec $v AS d]", "KNN(*;10;vec;d)"},
      {"@email:a.b.5$v", "AND{email:AND{T(a) T(b) T(5)} T(abcd)}"},
      {R"(a.b.5x\$y)", "AND{T(a) T(b) T(5x$y)}"},
      {"@f:x.$v", "f:AND{T(x) T(abcd)}"},
      {"@f:$v.x", "f:AND{T(abcd) T(x)}"},
      {"@email:john.as", "email:AND{T(john) T(as)}"},
      {"@email:as.example.com", "email:AND{T(as) T(example) T(com)}"},
      {"@t:foo.KNN.vector_range", "t:AND{T(foo) T(KNN) T(vector_range)}"},
      {"ef_runtime/as", "AND{T(ef_runtime) T(as)}"},
      {"*=>[KNN 10 @vec $v AS.d]", "KNN(*;10;vec;d)"},
      {"@email:as.2024.example.com", "email:AND{T(as) T(2024) T(example) T(com)}"},
      {"knn+5", "AND{T(knn) T(+5)}"},
      {"x.y.5as", "AND{T(x) T(y) T(5as)}"},
      {"as,'..5.6", "AND{T(as) T(5) T(6)}"},
      {"@email:.5*!.6", "email:AND{P(5) T(6)}"},
      {"@email:v1.5*.jane.5", "email:AND{T(v1) P(5) T(jane) T(5)}"},
      {"@f:x.5*,.5", "f:AND{T(x) P(5) T(5)}"},
      {"@f:x.5*.as", "f:AND{T(x) P(5) T(as)}"},
      {"@f:.5*.5", "AND{f:P(5) T(.5)}"},
      {".5*#as", "AND{P(5) T(as)}"},
      {"@f:x,+5.1e3", "AND{f:AND{T(x) T(5.1)} T(e3)}"},
      {"@f:x,+5inf", "f:AND{T(x) T(5inf)}"},
      {"@f:x.-5", "f:AND{T(x) T(-5)}"},
      {"@f:x!.1.5", "f:AND{T(x) T(1) T(5)}"},
      {"@f:(x!.5)", "f:AND{T(x) T(5)}"},
      {"@f:x!+5.5w'q'", "AND{f:AND{T(x) T(5.5)} W(q)}"},
      {"@f:x!.5 w'q'", "AND{f:AND{T(x) T(5)} W(q)}"},
      {"@f:x.+inf$n", "AND{f:AND{T(x) T(inf)} T(10)}"},
      {"@n:[1 5] @email:example.com", "AND{n:R[1,5] email:AND{T(example) T(com)}}"},
      {"@t:{a} @f:x.y", "AND{t:TAGS{T(a)} f:AND{T(x) T(y)}}"},
  };
  for (const auto& [query, ast] : kCases)
    EXPECT_EQ(ParseDump(query), ast) << query;
}

// Queries that parse without glued words must keep their exact tree.
TEST_F(SearchParserTest, GluedWordUnchangedAst) {
  const pair<string, string> kCases[] = {
      {"v1.2.3", "AND{T(v1) T(.2) T(.3)}"},
      {"10.0.0.1", "AND{T(10.0) T(.0) T(.1)}"},
      {"foo.5", "AND{T(foo) T(.5)}"},
      {"@email:jane.2024", "AND{email:T(jane) T(.2024)}"},
      {R"(example\.com)", "T(example.com)"},
      {R"(@email:"example.com")", "email:PH(example.com~0)"},
      {"''''", "AND{PH(~0) PH(~0)}"},
      {"''+5", "AND{PH(~0) T(+5)}"},
      {"'.<'.3.14+5", "AND{PH(.<~0) T(.3) T(.14) T(+5)}"},
      {"foo=>{$weight:.5}", "ATTR(T(foo),w=0.5)"},
      {"@n:[1,5]", "n:R[1,5]"},
      {"@n:[1,.5]", "n:R[1,0.5]"},
      {"@n:[(1 5]", "n:R[1,5]"},
      {"a--b", "AND{T(a) NOT(NOT(T(b)))}"},
  };
  for (const auto& [query, ast] : kCases)
    EXPECT_EQ(ParseDump(query), ast) << query;
}

// Phrases and quotes are not word atoms: a separator next to them is skipped.
TEST_F(SearchParserTest, GluedWordPhrasesAndQuotes) {
  const pair<string, string> kCases[] = {
      {"@email:example.'com'~2", "AND{email:T(example) PH(com~2)}"},
      {R"(@email:"foo".bar)", "AND{email:PH(foo~0) T(bar)}"},
      {"@email:w'ex*'.com", "AND{email:W(ex*) T(com)}"},
      // A second apostrophe pairs into a phrase before either one can glue.
      {"@email:O'Brien | @name:'x'", "AND{email:T(O) PH(Brien | @name:~0) T(x)}"},
      {"@last:O'Brien @first:D'Arcy", "AND{last:T(O) PH(Brien @first:D~0) T(Arcy)}"},
  };
  for (const auto& [query, ast] : kCases)
    EXPECT_EQ(ParseDump(query), ast) << query;
}

TEST_F(SearchParserTest, GluedWordErrors) {
  for (string query : {".", "'", "@email:*.com", "@email:.*", "@email:10.0*", "@n:[1,,5]",
                       "@email_tag:{example.com}", "@email:as", "@email:.as", "@email:as.",
                       "@email:foo*as", "x.5as", "john.$ef_runtime", "x.y.5$ef_runtime", "x.5 *",
                       "+5.5*", "@f:x+5.5*", "@f:x.5**", "@n:[.5* 1]"}) {
    EXPECT_EQ(ParseDump(query), "error") << query;
  }
  // A split `.5`/`+inf` would lex differently next to these word parts, e.g. `5*y` is P(5) T(y).
  for (string query : {"@f:x!.5*y", "@f:x!+5*y*", ".5*example.com*", "@f:x.y.+inf*z", "x!.5w'q'",
                       "-@f:x.+inf0", "@f:x.+inf5.5"}) {
    EXPECT_EQ(ParseDump(query), "error") << query;
  }
}

// Separators in [..], {..} or not between two atoms are skipped like spaces; numbers keep `.`/`+`.
TEST_F(SearchParserTest, GluedWordSkippedSeparators) {
  QueryParams params;
  params["v"] = "abcd";
  SetParams(&params);

  const pair<string, string> kCases[] = {
      {"@n:[1!5]", "n:R[1,5]"},
      {"@n:[(1.5/2.5]", "n:R[1.5,2.5]"},
      {"@n:[.5!1]", "n:R[0.5,1]"},
      {"@n:[1!.5]", "n:R[1,0.5]"},
      {"@n:[1 !+.5]", "n:R[1,0.5]"},
      {"@g:[1.2!3.4!5!km]", "g:GEO(1.2,3.4,5KM)"},
      {"@f:[VECTOR_RANGE 1!$v]", "VRANGE(f;1)"},
      {"foo=>{$weight:!.5}", "ATTR(T(foo),w=0.5)"},
      {"@t:{!.5}", "t:TAGS{T(.5)}"},
      {"@t:{!+.5}", "t:TAGS{T(+.5)}"},
      {"@t:{!+Inf}", "t:TAGS{T(+Inf)}"},
      {"@n:[1 .5!]", "n:R[1,0.5]"},
      {"@email_tag:{.com}", "email_tag:TAGS{T(com)}"},
      {"foo=>{$weight:!2}", "ATTR(T(foo),w=2)"},
      {"@n:[1 5]!.5", "AND{n:R[1,5] T(.5)}"},
      {"@t:{a} !.5", "AND{t:TAGS{T(a)} T(.5)}"},
      {"@f:.5,,", "f:T(.5)"},
      {"@f:,!.5", "f:T(.5)"},
      {"@f:!.5*.5", "AND{f:P(5) T(.5)}"},
      {"@f:x .5*", "AND{f:T(x) P(5)}"},
      {"@f:x. !y", "AND{f:T(x) T(y)}"},
      {"@f:y. z", "AND{f:T(y) T(z)}"},
      {"* .", "*"},
      {"! *", "*"},
  };
  for (const auto& [query, ast] : kCases)
    EXPECT_EQ(ParseDump(query), ast) << query;
}

// A driver reused without ResetScanner() must not carry glue state over to the next input.
TEST_F(SearchParserTest, GluedWordReusedDriver) {
  // Each failed parse leaves brackets, buffered, pending or previous-token state behind.
  const array<array<string, 3>, 5> kCases = {{{"@n:[1", "@f:x.y", "f:AND{T(x) T(y)}"},
                                              {"@n:[1!2!3!4]", "@f:x.y", "f:AND{T(x) T(y)}"},
                                              {"@f:x.*", "@f:x.y", "f:AND{T(x) T(y)}"},
                                              {"* x", "   .y", "T(y)"},
                                              {"* x", ".y", "T(y)"}}};
  for (const auto& [bad, good, ast] : kCases) {
    query_driver_.ResetScanner();
    query_driver_.SetInput(bad);
    EXPECT_NE(Parser(&query_driver_)(), 0) << bad;
    query_driver_.SetInput(good);
    ASSERT_EQ(Parser(&query_driver_)(), 0) << bad;
    EXPECT_EQ(DumpAst(query_driver_.Take()), ast) << bad;
  }
}

}  // namespace dfly::search
