%skeleton "lalr1.cc" // -*- C++ -*-
%require "3.5"  // fedora 32 has this one.

%defines  // %header starts from 3.8.1

%define api.namespace {dfly::search}

%define api.token.raw
%define api.token.constructor
%define api.value.type variant
%define api.parser.class {Parser}
%define parse.assert
%define api.value.automove true

// Added to header file before parser declaration.
%code requires {
  #include "core/search/ast_expr.h"

  namespace dfly {
  namespace search {
    class QueryDriver;
    // Token value for quoted phrases: raw text plus optional ~N slop. `slop`=0 = exact adjacency.
    struct PhraseTok {
      std::string raw;
      uint32_t slop = 0;
    };
    struct KnnAttributes {
      std::string score_alias;
      std::optional<uint32_t> ef_runtime;
      std::optional<double> weight;
    };
    struct VectorRangeAttributes {
      std::string score_alias;
      std::optional<double> epsilon;
      std::optional<double> weight;
    };
    struct TextAttributes {
      std::optional<double> weight;
    };
    inline std::ostream& operator<<(std::ostream& os, const PhraseTok&) {
      return os;  // bison debug trace requires this; content not material to traces.
    }
    inline std::ostream& operator<<(std::ostream& os, const KnnAttributes&) {
      return os;  // bison debug trace requires this; content not material to traces.
    }
    inline std::ostream& operator<<(std::ostream& os, const VectorRangeAttributes&) {
      return os;  // bison debug trace requires this; content not material to traces.
    }
    inline std::ostream& operator<<(std::ostream& os, const TextAttributes&) {
      return os;  // bison debug trace requires this; content not material to traces.
    }
  }
  }
}

// Added to cc file
%code {
#include <cmath>

#include <absl/strings/ascii.h>
#include "core/search/query_driver.h"
#include "core/search/search.h"
#include "core/search/vector_utils.h"

#define yylex driver->scanner()->Lex

using namespace std;

uint32_t toUint32(string_view src);
double toDouble(string_view src);
// Defined in lexer.lex: strips backslashes from \X sequences.
string UnescapeTerm(string_view src);

}

%parse-param { QueryDriver *driver  }

%locations

%define parse.trace
%define parse.error verbose  // detailed
%define parse.lac full
%define api.token.prefix {TOK_}

%token
  LPAREN      "("
  RPAREN      ")"
  STAR        "*"
  ARROW       "=>"
  ATTR_ARROW  "=>{"
  COLON       ":"
  LBRACKET    "["
  RBRACKET    "]"
  LCURLBR     "{"
  RCURLBR     "}"
  OR_OP       "|"
  COMMA       ","
  SEMICOLON   ";"
  TILDE       "~"
  KNN         "KNN"
  AS          "AS"
  EF_RUNTIME  "EF_RUNTIME"
  EPSILON     "$EPSILON"
  VECTOR_RANGE      "VECTOR_RANGE"
  YIELD_DISTANCE_AS "$YIELD_DISTANCE_AS"
  WEIGHT      "$WEIGHT"
;

%token AND_OP

// Needed 0 at the end to satisfy bison 3.5.1
%token YYEOF 0
%token <std::string> TERM "term" TAG_VAL "tag_val" PARAM "param" FIELD "field" PREFIX "prefix" SUFFIX "suffix" INFIX "infix" WILDCARD "wildcard"
%token <PhraseTok> PHRASE "phrase"

%precedence TERM TAG_VAL
%left OR_OP
%left AND_OP
%right NOT_OP TILDE
%precedence LPAREN RPAREN

// Dangling-else for the optional VECTOR_RANGE YIELD: at `]` with `=>` ahead, ARROW outranks the
// no-yield rule (tagged %prec NO_YIELD), so bison shifts into the YIELD clause instead of reducing
// and treating `=>` as a top-level KNN arrow.
%precedence NO_YIELD
%precedence ARROW

%token <std::string> DOUBLE "double"
%token <std::string> UINT32 "uint32"
%nterm <AstExpr> final_query filter star_expr search_expr search_unary_expr search_primary
%nterm <AstExpr> attributed_search_primary search_or_expr search_and_expr bracket_filter_expr
%nterm <AstExpr> field_cond field_cond_expr field_unary_expr field_or_expr field_and_expr tag_list
%nterm <AstExpr> term_atom
%nterm <AstTagsNode::TagValueProxy> tag_list_element

%nterm <AstKnnNode> knn_query
%nterm <std::string> opt_knn_alias
%nterm <KnnAttributes> opt_knn_attrs knn_attrs knn_attr
%nterm <std::string> geounit
%nterm <std::optional<uint32_t>> opt_ef_runtime
%nterm <AstVectorRangeNode> vector_range_query
%nterm <VectorRangeAttributes> vector_range_attrs vector_range_attr vector_range_attrs_clause
%nterm <TextAttributes> text_attrs text_attr text_attrs_clause
%nterm <double> vec_range_radius

%printer { yyo << $$; } <*>;

%%

final_query:
  filter
      { driver->Set(std::move($1)); }
  | filter ARROW knn_query
      { driver->Set(AstKnnNode(std::move($1), std::move($3))); }

knn_query:
  LBRACKET KNN UINT32 FIELD TERM opt_ef_runtime opt_knn_alias RBRACKET opt_knn_attrs
    {
      // Accept any string as vector - validation happens later during search execution
      uint32_t knn_count = toUint32($3);
      auto field = std::move($4);
      auto alias = std::move($7);
      auto attrs = std::move($9);
      if (!attrs.score_alias.empty())
        alias = std::move(attrs.score_alias);
      std::optional<uint32_t> ef = $6;
      if (attrs.ef_runtime)
        ef = attrs.ef_runtime;

      // Raw query bytes are validated at search time against the field's declared dtype width.
      $$ = AstKnnNode(knn_count, std::move(field), std::string{$5}, std::move(alias), ef);
    }

opt_knn_alias:
  AS TERM { $$ = std::move($2); }
  | { $$ = std::string{}; }

opt_ef_runtime:
  /* empty */ { $$ = std::nullopt; }
  | EF_RUNTIME UINT32 { $$ = toUint32($2); }

opt_knn_attrs:
  /* empty */ { $$ = KnnAttributes{}; }
  | ATTR_ARROW knn_attrs opt_knn_attr_trailer RCURLBR { $$ = std::move($2); }

opt_knn_attr_trailer:
  /* empty */
  | SEMICOLON

knn_attrs:
  knn_attr { $$ = std::move($1); }
  | knn_attrs SEMICOLON knn_attr
    {
      $$ = std::move($1);
      auto attr = std::move($3);
      if (attr.ef_runtime)
        $$.ef_runtime = attr.ef_runtime;
      if (!attr.score_alias.empty())
        $$.score_alias = std::move(attr.score_alias);
      if (attr.weight) {
        if ($$.weight)
          YYABORT;
        $$.weight = attr.weight;
      }
    }

knn_attr:
  EF_RUNTIME COLON UINT32
    {
      $$ = KnnAttributes{};
      $$.ef_runtime = toUint32($3);
    }
  | YIELD_DISTANCE_AS COLON TERM
    {
      $$ = KnnAttributes{};
      $$.score_alias = std::move($3);
    }
  | WEIGHT COLON vec_range_radius
    {
      double weight = $3;
      if (!SchemaField::TextParams::IsValidWeight(weight))
        YYABORT;
      $$ = KnnAttributes{};
      $$.weight = weight;
    }

vector_range_query:
  FIELD COLON LBRACKET VECTOR_RANGE vec_range_radius TERM RBRACKET vector_range_attrs_clause
    {
      double radius = $5;
      auto field = std::move($1);
      auto attrs = std::move($8);
      $$ = AstVectorRangeNode(std::move(field), radius, std::string{$6},
                              std::move(attrs.score_alias), attrs.epsilon);
    }
  | FIELD COLON LBRACKET VECTOR_RANGE vec_range_radius TERM RBRACKET %prec NO_YIELD
    {
      double radius = $5;
      auto field = std::move($1);
      $$ = AstVectorRangeNode(std::move(field), radius, std::string{$6}, std::string{}, std::nullopt);
    }

vector_range_attrs_clause:
  ATTR_ARROW vector_range_attrs opt_vector_range_attr_trailer RCURLBR { $$ = std::move($2); }

opt_vector_range_attr_trailer:
  /* empty */
  | SEMICOLON

vector_range_attrs:
  vector_range_attr { $$ = std::move($1); }
  | vector_range_attrs SEMICOLON vector_range_attr
    {
      $$ = std::move($1);
      auto attr = std::move($3);
      if (attr.epsilon) {
        if ($$.epsilon)
          YYABORT;
        $$.epsilon = attr.epsilon;
      }
      if (!attr.score_alias.empty()) {
        if (!$$.score_alias.empty())
          YYABORT;
        $$.score_alias = std::move(attr.score_alias);
      }
      if (attr.weight) {
        if ($$.weight)
          YYABORT;
        $$.weight = attr.weight;
      }
    }

vector_range_attr:
  YIELD_DISTANCE_AS COLON TERM
    {
      $$ = VectorRangeAttributes{};
      $$.score_alias = std::move($3);
    }
  | EPSILON COLON vec_range_radius
    {
      double epsilon = $3;
      if (!SchemaField::VectorParams::IsValidRuntimeHnswEpsilon(epsilon))
        YYABORT;
      $$ = VectorRangeAttributes{};
      $$.epsilon = epsilon;
    }
  | WEIGHT COLON vec_range_radius
    {
      double weight = $3;
      if (!SchemaField::TextParams::IsValidWeight(weight))
        YYABORT;
      $$ = VectorRangeAttributes{};
      $$.weight = weight;
    }

vec_range_radius:
  DOUBLE  { $$ = toDouble($1); }
  | UINT32 { $$ = static_cast<double>(toUint32($1)); }
  | TERM   { double v = 0; if (!absl::SimpleAtod($1, &v)) YYABORT; $$ = v; }

text_attrs_clause:
  ATTR_ARROW text_attrs opt_text_attr_trailer RCURLBR { $$ = std::move($2); }

opt_text_attr_trailer:
  /* empty */
  | SEMICOLON

text_attrs:
  text_attr { $$ = std::move($1); }
  | text_attrs SEMICOLON text_attr
    {
      $$ = std::move($1);
      auto attr = std::move($3);
      if (attr.weight) {
        if ($$.weight)
          YYABORT;
        $$.weight = attr.weight;
      }
    }

text_attr:
  WEIGHT COLON vec_range_radius
    {
      double weight = $3;
      if (!SchemaField::TextParams::IsValidWeight(weight))
        YYABORT;
      $$ = TextAttributes{};
      $$.weight = weight;
    }

filter:
  search_expr               { $$ = std::move($1); }
  | star_expr               { $$ = std::move($1); }

star_expr:
  STAR                      { $$ = AstStarNode(); }
  | LPAREN star_expr RPAREN { $$ = std::move($2); }

search_expr:
  search_unary_expr         { $$ = std::move($1); }
  | search_and_expr         { $$ = std::move($1); }
  | search_or_expr          { $$ = std::move($1); }

search_and_expr:
  search_unary_expr search_unary_expr %prec AND_OP { $$ = AstLogicalNode(std::move($1), std::move($2), AstLogicalNode::AND); }
  | search_and_expr search_unary_expr %prec AND_OP { $$ = AstLogicalNode(std::move($1), std::move($2), AstLogicalNode::AND); }

search_or_expr:
  search_expr OR_OP search_and_expr                { $$ = AstLogicalNode(std::move($1), std::move($3), AstLogicalNode::OR); }
  | search_expr OR_OP search_unary_expr            { $$ = AstLogicalNode(std::move($1), std::move($3), AstLogicalNode::OR); }

// Leaf atoms shared by the top-level, field-condition, and field-grouping positions.
term_atom:
  TERM        { $$ = AstTermNode(std::move($1));   }
  | PHRASE    { auto p = std::move($1); $$ = AstPhraseNode(std::move(p.raw), p.slop); }
  | PREFIX    { $$ = AstPrefixNode(std::move($1)); }
  | SUFFIX    { $$ = AstSuffixNode(std::move($1)); }
  | INFIX     { $$ = AstInfixNode(std::move($1));  }
  | WILDCARD  { $$ = AstWildcardNode(std::move($1)); }
  | UINT32    { $$ = AstTermNode(std::move($1));   }
  | DOUBLE    { $$ = AstTermNode(std::move($1));   }

search_unary_expr:
  attributed_search_primary           { $$ = std::move($1);                  }
  | NOT_OP search_unary_expr          { $$ = AstNegateNode(std::move($2));   }
  | TILDE search_unary_expr           { $$ = AstOptionalNode(std::move($2)); }
  | vector_range_query                { $$ = std::move($1);                  }

attributed_search_primary:
  search_primary                      { $$ = std::move($1);                  }
  | search_primary text_attrs_clause
    { $$ = AstAttributeNode(std::move($1), $2.weight.value_or(1.0)); }

search_primary:
  LPAREN search_expr RPAREN           { $$ = std::move($2);                  }
  | term_atom                         { $$ = std::move($1);                  }
  | FIELD COLON field_cond            { $$ = AstFieldNode(std::move($1), std::move($3)); }

field_cond:
  term_atom                                             { $$ = std::move($1);                }
  | STAR                                                { $$ = AstStarFieldNode();           }
  | NOT_OP field_cond                                   { $$ = AstNegateNode(std::move($2)); }
  | TILDE field_cond                                    { $$ = AstOptionalNode(std::move($2)); }
  | LPAREN field_cond_expr RPAREN                       { $$ = std::move($2); }
  | LBRACKET bracket_filter_expr RBRACKET               { $$ = std::move($2); }
  | LCURLBR tag_list RCURLBR                            { $$ = std::move($2); }

bracket_filter_expr:
  /* Numeric filter has form [(] UINT32|DOUBLE [COMMA] [(] UINT32|DOUBLE */
  DOUBLE DOUBLE                                { $$ = AstRangeNode(toDouble($1), false, toDouble($2), false); }
  | LPAREN DOUBLE DOUBLE                       { $$ = AstRangeNode(toDouble($2), true, toDouble($3), false); }
  | DOUBLE LPAREN DOUBLE                       { $$ = AstRangeNode(toDouble($1), false, toDouble($3), true); }
  | LPAREN DOUBLE LPAREN DOUBLE                { $$ = AstRangeNode(toDouble($2), true, toDouble($4), true); }
  | DOUBLE UINT32                              { $$ = AstRangeNode(toDouble($1), false, toUint32($2), false); }
  | LPAREN DOUBLE UINT32                       { $$ = AstRangeNode(toDouble($2), true, toUint32($3), false); }
  | DOUBLE LPAREN UINT32                       { $$ = AstRangeNode(toDouble($1), false, toUint32($3), true); }
  | LPAREN DOUBLE LPAREN UINT32                { $$ = AstRangeNode(toDouble($2), true, toUint32($4), true); }
  | UINT32 DOUBLE                              { $$ = AstRangeNode(toUint32($1), false, toDouble($2), false); }
  | LPAREN UINT32 DOUBLE                       { $$ = AstRangeNode(toUint32($2), true, toDouble($3), false); }
  | UINT32 LPAREN DOUBLE                       { $$ = AstRangeNode(toUint32($1), false, toDouble($3), true); }
  | LPAREN UINT32 LPAREN DOUBLE                { $$ = AstRangeNode(toUint32($2), true, toDouble($4), true); }
  | UINT32 UINT32                              { $$ = AstRangeNode(toUint32($1), false, toUint32($2), false); }
  | LPAREN UINT32 UINT32                       { $$ = AstRangeNode(toUint32($2), true, toUint32($3), false); }
  | UINT32 LPAREN UINT32                       { $$ = AstRangeNode(toUint32($1), false, toUint32($3), true); }
  | LPAREN UINT32 LPAREN UINT32                { $$ = AstRangeNode(toUint32($2), true, toUint32($4), true); }
  | DOUBLE COMMA DOUBLE                        { $$ = AstRangeNode(toDouble($1), false, toDouble($3), false); }
  | DOUBLE COMMA UINT32                        { $$ = AstRangeNode(toDouble($1), false, toUint32($3), false); }
  | UINT32 COMMA DOUBLE                        { $$ = AstRangeNode(toUint32($1), false, toDouble($3), false); }
  | UINT32 COMMA UINT32                        { $$ = AstRangeNode(toUint32($1), false, toUint32($3), false); }
  | LPAREN DOUBLE COMMA DOUBLE                 { $$ = AstRangeNode(toDouble($2), true, toDouble($4), false); }
  | DOUBLE COMMA LPAREN DOUBLE                 { $$ = AstRangeNode(toDouble($1), false, toDouble($4), true); }
  | LPAREN DOUBLE COMMA LPAREN DOUBLE          { $$ = AstRangeNode(toDouble($2), true, toDouble($5), true); }
  | LPAREN DOUBLE COMMA UINT32                 { $$ = AstRangeNode(toDouble($2), true, toUint32($4), false); }
  | DOUBLE COMMA LPAREN UINT32                 { $$ = AstRangeNode(toDouble($1), false, toUint32($4), true); }
  | LPAREN DOUBLE COMMA LPAREN UINT32          { $$ = AstRangeNode(toDouble($2), true, toUint32($5), true); }
  | LPAREN UINT32 COMMA DOUBLE                 { $$ = AstRangeNode(toUint32($2), true, toDouble($4), false); }
  | UINT32 COMMA LPAREN DOUBLE                 { $$ = AstRangeNode(toUint32($1), false, toDouble($4), true); }
  | LPAREN UINT32 COMMA LPAREN DOUBLE          { $$ = AstRangeNode(toUint32($2), true, toDouble($5), true); }
  | LPAREN UINT32 COMMA UINT32                 { $$ = AstRangeNode(toUint32($2), true, toUint32($4), false); }
  | UINT32 COMMA LPAREN UINT32                 { $$ = AstRangeNode(toUint32($1), false, toUint32($4), true); }
  | LPAREN UINT32 COMMA LPAREN UINT32          { $$ = AstRangeNode(toUint32($2), true, toUint32($5), true); }
  /* GEO filter */
  | DOUBLE DOUBLE UINT32 geounit               { $$ = AstGeoNode(toDouble($1), toDouble($2), toUint32($3), std::move($4)); }
  | DOUBLE DOUBLE DOUBLE geounit               { $$ = AstGeoNode(toDouble($1), toDouble($2), toDouble($3), std::move($4)); }

geounit:
  TERM
  {
    std::string unit = $1;
    absl::AsciiStrToUpper(&unit);
    if ((unit == "M") || (unit == "KM") || (unit == "MI") || (unit == "FT")) {
        $$ = unit;
    } else {
        YYABORT;
    }
  }

field_cond_expr:
  field_unary_expr { $$ = std::move($1); }
  | field_and_expr { $$ = std::move($1); }
  | field_or_expr  { $$ = std::move($1); }

field_and_expr:
  field_unary_expr field_unary_expr %prec AND_OP  { $$ = AstLogicalNode(std::move($1), std::move($2), AstLogicalNode::AND); }
  | field_and_expr field_unary_expr %prec AND_OP  { $$ = AstLogicalNode(std::move($1), std::move($2), AstLogicalNode::AND); }

field_or_expr:
  field_cond_expr OR_OP field_unary_expr          { $$ = AstLogicalNode(std::move($1), std::move($3), AstLogicalNode::OR); }
  | field_cond_expr OR_OP field_and_expr          { $$ = AstLogicalNode(std::move($1), std::move($3), AstLogicalNode::OR); }

field_unary_expr:
  LPAREN field_cond_expr RPAREN { $$ = std::move($2);                  }
  | NOT_OP field_unary_expr     { $$ = AstNegateNode(std::move($2));   }
  | TILDE field_unary_expr      { $$ = AstOptionalNode(std::move($2)); }
  | term_atom                   { $$ = std::move($1);                  }
  | term_atom text_attrs_clause { $$ = AstAttributeNode(std::move($1), $2.weight.value_or(1.0)); }

tag_list:
  tag_list_element                       { $$ = AstTagsNode(std::move($1));                }
  | tag_list OR_OP tag_list_element      { $$ = AstTagsNode(std::move($1), std::move($3)); }

tag_list_element:
  TERM        { $$ = AstTermNode(std::move($1));   }
  | PHRASE {
      /* Inside {..}, quoted strings are literal tag values with one layer of `\X` escapes,
         matching the unquoted tag path (make_Tag). ~N slop is only meaningful for phrases,
         so reject it here rather than silently dropping it. */
      auto p = std::move($1);
      if (p.slop != 0)
        throw Parser::syntax_error(@$, "slop is not allowed in tag values");
      $$ = AstTermNode(UnescapeTerm(p.raw));
    }
  | PREFIX    { $$ = AstPrefixNode(std::move($1)); }
  | SUFFIX    { $$ = AstSuffixNode(std::move($1)); }
  | INFIX     { $$ = AstInfixNode(std::move($1));  }
  | WILDCARD  { $$ = AstWildcardNode(std::move($1)); }
  | UINT32    { $$ = AstTermNode(std::move($1));   }
  | DOUBLE    { $$ = AstTermNode(std::move($1));   }
  | TAG_VAL   { $$ = AstTermNode(std::move($1));   }


%%

void
dfly::search::Parser::error(const location_type& l, const string& m)
{
  driver->Error(l, m);
}

std::uint32_t toUint32(string_view str) {
  uint32_t val = 0;
  std::ignore = absl::SimpleAtoi(str, &val); // no need to check the result because str is parsed by regex
  return val;
}

double toDouble(string_view str) {
  double val = 0;
  std::ignore = absl::SimpleAtod(str, &val); // no need to check the result because str is parsed by regex
  return val;
}
