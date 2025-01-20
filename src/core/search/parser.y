%skeleton "lalr1.cc" // -*- C++ -*-
%require "3.5"  // fedora 32 has this one.

%defines  // %header starts from 3.8.1

%define api.namespace {dfly::search}

%define api.token.raw
%define api.token.constructor
%define api.value.type variant
%define api.parser.class {Parser}
%define parse.assert

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
  KNN         "KNN"
  AS          "AS"
  EF_RUNTIME  "EF_RUNTIME"
;

%token AND_OP

// Needed 0 at the end to satisfy bison 3.5.1
%token YYEOF 0
%token <std::string> TERM "term" TAG_VAL "tag_val" PARAM "param" FIELD "field" PREFIX "prefix"

%precedence TERM TAG_VAL
%left OR_OP
%left AND_OP
%right NOT_OP
%precedence LPAREN RPAREN

%token <std::string> DOUBLE "double"
%token <std::string> UINT32 "uint32"
%nterm <double> generic_number
%nterm <bool> opt_lparen
%nterm <AstExpr> final_query filter search_expr search_unary_expr search_or_expr search_and_expr numeric_filter_expr
%nterm <AstExpr> field_cond field_cond_expr field_unary_expr field_or_expr field_and_expr tag_list
%nterm <AstTagsNode::TagValueProxy> tag_list_element

%nterm <AstKnnNode> knn_query
%nterm <std::string> opt_knn_alias
%nterm <std::optional<size_t>> opt_ef_runtime

%printer { yyo << $$; } <*>;

%%

final_query:
  filter
      { driver->Set(std::move($1)); }
  | filter ARROW knn_query
      { driver->Set(AstKnnNode(std::move($1), std::move($3))); }

knn_query:
  LBRACKET KNN UINT32 FIELD TERM opt_knn_alias opt_ef_runtime RBRACKET
    { $$ = AstKnnNode(toUint32($3), $4, BytesToFtVector($5), $6, $7); }

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
  LPAREN search_expr RPAREN           { $$ = std::move($2); }
  | NOT_OP search_unary_expr          { $$ = AstNegateNode(std::move($2)); }
  | TERM                              { $$ = AstTermNode(std::move($1)); }
  | PREFIX                            { $$ = AstPrefixNode(std::move($1)); }
  | UINT32                            { $$ = AstTermNode(std::move($1)); }
  | FIELD COLON field_cond            { $$ = AstFieldNode(std::move($1), std::move($3)); }

field_cond:
  TERM                                                  { $$ = AstTermNode(std::move($1)); }
  | UINT32                                              { $$ = AstTermNode(std::move($1)); }
  | NOT_OP field_cond                                   { $$ = AstNegateNode(std::move($2)); }
  | LPAREN field_cond_expr RPAREN                       { $$ = std::move($2); }
  | LBRACKET numeric_filter_expr RBRACKET               { $$ = std::move($2); }
  | LCURLBR tag_list RCURLBR                            { $$ = std::move($2); }

numeric_filter_expr:
opt_lparen generic_number opt_lparen generic_number { $$ = AstRangeNode($2, $1, $4, $3); }

generic_number:
  DOUBLE { $$ = toDouble($1); }
  | UINT32 { $$ = toUint32($1); }

opt_lparen:
  /* empty */ { $$ = false; }
  | LPAREN { $$ = true; }

field_cond_expr:
  field_unary_expr                       { $$ = std::move($1); }
  | field_and_expr                       { $$ = std::move($1); }
  | field_or_expr                        { $$ = std::move($1); }

field_and_expr:
  field_unary_expr field_unary_expr %prec AND_OP  { $$ = AstLogicalNode(std::move($1), std::move($2), AstLogicalNode::AND); }
  | field_and_expr field_unary_expr %prec AND_OP  { $$ = AstLogicalNode(std::move($1), std::move($2), AstLogicalNode::AND); }

field_or_expr:
  field_cond_expr OR_OP field_unary_expr          { $$ = AstLogicalNode(std::move($1), std::move($3), AstLogicalNode::OR); }
  | field_cond_expr OR_OP field_and_expr          { $$ = AstLogicalNode(std::move($1), std::move($3), AstLogicalNode::OR); }

field_unary_expr:
  LPAREN field_cond_expr RPAREN                  { $$ = std::move($2); }
  | NOT_OP field_unary_expr                      { $$ = AstNegateNode(std::move($2)); };
  | TERM                                         { $$ = AstTermNode(std::move($1)); }
  | UINT32                                       { $$ = AstTermNode(std::move($1)); }

tag_list:
  tag_list_element                       { $$ = AstTagsNode(std::move($1)); }
  | tag_list OR_OP tag_list_element      { $$ = AstTagsNode(std::move($1), std::move($3)); }

tag_list_element:
  TERM        { $$ = AstTermNode(std::move($1)); }
  | PREFIX    { $$ = AstPrefixNode(std::move($1)); }
  | UINT32    { $$ = AstTermNode(std::move($1)); }
  | DOUBLE    { $$ = AstTermNode(std::move($1)); }
  | TAG_VAL   { $$ = AstTermNode(std::move($1)); }


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
