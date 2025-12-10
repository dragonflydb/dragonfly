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
  }
  }
}

// Added to cc file
%code {
#include "core/search/query_driver.h"
#include "core/search/vector_utils.h"

#define yylex driver->scanner()->Lex

using namespace std;

uint32_t toUint32(string_view src);
double toDouble(string_view src);

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
  COLON       ":"
  LBRACKET    "["
  RBRACKET    "]"
  LCURLBR     "{"
  RCURLBR     "}"
  OR_OP       "|"
  COMMA       ","
  KNN         "KNN"
  AS          "AS"
  EF_RUNTIME  "EF_RUNTIME"
  GEOUNIT_M   "GEOUNIT_M"
  GEOUNIT_KM  "GEOUNIT_KM"
  GEOUNIT_MI  "GEOUNIT_MI"
  GEOUNIT_FT  "GEOUNIT_FT"
;

%token AND_OP

// Needed 0 at the end to satisfy bison 3.5.1
%token YYEOF 0
%token <std::string> TERM "term" TAG_VAL "tag_val" PARAM "param" FIELD "field" PREFIX "prefix" SUFFIX "suffix" INFIX "infix"

%precedence TERM TAG_VAL
%left OR_OP
%left AND_OP
%right NOT_OP
%precedence LPAREN RPAREN

%token <std::string> DOUBLE "double"
%token <std::string> UINT32 "uint32"
%nterm <AstExpr> final_query filter search_expr search_unary_expr search_or_expr search_and_expr bracket_filter_expr
%nterm <AstExpr> field_cond field_cond_expr field_unary_expr field_or_expr field_and_expr tag_list
%nterm <AstTagsNode::TagValueProxy> tag_list_element

%nterm <AstKnnNode> knn_query
%nterm <std::string> opt_knn_alias
%nterm <std::string> geounit
%nterm <std::optional<size_t>> opt_ef_runtime

%printer { yyo << $$; } <*>;

%%

final_query:
  filter
      { driver->Set(std::move($1)); }
  | filter ARROW knn_query
      { driver->Set(AstKnnNode(std::move($1), std::move($3))); }

knn_query:
  LBRACKET KNN UINT32 FIELD TERM opt_ef_runtime opt_knn_alias RBRACKET
    {
      // Accept any string as vector - validation happens later during search execution
      uint32_t knn_count = toUint32($3);
      auto field = std::move($4);
      auto alias = std::move($7);
      auto ef = $6;

      auto vec_result = BytesToFtVectorSafe($5);
      if (!vec_result) {
        // Create empty vector for invalid data - will return empty results during search
        auto empty_vec = std::make_unique<float[]>(0);
        $$ = AstKnnNode(knn_count, std::move(field), std::make_pair(std::move(empty_vec), size_t{0}), std::move(alias), ef);
      } else {
        $$ = AstKnnNode(knn_count, std::move(field), std::move(*vec_result), std::move(alias), ef);
      }
    }

opt_knn_alias:
  AS TERM { $$ = std::move($2); }
  | { $$ = std::string{}; }

opt_ef_runtime:
  /* empty */ { $$ = std::nullopt; }
  | EF_RUNTIME UINT32 { $$ = toUint32($2); }

filter:
  search_expr               { $$ = std::move($1); }
  | STAR                    { $$ = AstStarNode(); }

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

search_unary_expr:
  LPAREN search_expr RPAREN           { $$ = std::move($2);                }
  | NOT_OP search_unary_expr          { $$ = AstNegateNode(std::move($2)); }
  | TERM                              { $$ = AstTermNode(std::move($1));   }
  | PREFIX                            { $$ = AstPrefixNode(std::move($1)); }
  | SUFFIX                            { $$ = AstSuffixNode(std::move($1)); }
  | INFIX                             { $$ = AstInfixNode(std::move($1));  }
  | UINT32                            { $$ = AstTermNode(std::move($1));   }
  | FIELD COLON field_cond            { $$ = AstFieldNode(std::move($1), std::move($3)); }

field_cond:
  TERM                                                  { $$ = AstTermNode(std::move($1));   }
  | UINT32                                              { $$ = AstTermNode(std::move($1));   }
  | STAR                                                { $$ = AstStarFieldNode();           }
  | NOT_OP field_cond                                   { $$ = AstNegateNode(std::move($2)); }
  | LPAREN field_cond_expr RPAREN                       { $$ = std::move($2); }
  | LBRACKET bracket_filter_expr RBRACKET               { $$ = std::move($2); }
  | LCURLBR tag_list RCURLBR                            { $$ = std::move($2); }
  | PREFIX                                              { $$ = AstPrefixNode(std::move($1)); }
  | SUFFIX                                              { $$ = AstSuffixNode(std::move($1)); }
  | INFIX                                               { $$ = AstInfixNode(std::move($1));  }

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
  GEOUNIT_M     { $$ = "M"; }
  | GEOUNIT_KM  { $$ = "KM"; }
  | GEOUNIT_MI  { $$ = "MI"; }
  | GEOUNIT_FT  { $$ = "FT"; }

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
  LPAREN field_cond_expr RPAREN { $$ = std::move($2);                }
  | NOT_OP field_unary_expr     { $$ = AstNegateNode(std::move($2)); }
  | TERM                        { $$ = AstTermNode(std::move($1));   }
  | UINT32                      { $$ = AstTermNode(std::move($1));   }

tag_list:
  tag_list_element                       { $$ = AstTagsNode(std::move($1));                }
  | tag_list OR_OP tag_list_element      { $$ = AstTagsNode(std::move($1), std::move($3)); }

tag_list_element:
  TERM        { $$ = AstTermNode(std::move($1));   }
  | PREFIX    { $$ = AstPrefixNode(std::move($1)); }
  | SUFFIX    { $$ = AstSuffixNode(std::move($1)); }
  | INFIX     { $$ = AstInfixNode(std::move($1));  }
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
