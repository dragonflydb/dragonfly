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

// Have to disable because GCC doesn't understand `symbol_type`'s union
// implementation
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"

#define yylex driver->scanner()->Lex

using namespace std;
}

%parse-param { QueryDriver *driver  }

%locations

%define parse.trace
%define parse.error verbose  // detailed
%define parse.lac full
%define api.token.prefix {TOK_}

%token
  LPAREN   "("
  RPAREN   ")"
  STAR     "*"
  ARROW    "=>"
  COLON    ":"
  LBRACKET "["
  RBRACKET "]"
  LCURLBR  "{"
  RCURLBR  "}"
  OR_OP    "|"
  KNN      "KNN"
  AS       "AS"
;

%token AND_OP

// Needed 0 at the end to satisfy bison 3.5.1
%token YYEOF 0
%token <std::string> TERM "term" PARAM "param" FIELD "field"

%precedence TERM
%left OR_OP
%left AND_OP
%right NOT_OP
%precedence LPAREN RPAREN

%token <double> DOUBLE "double"
%token <uint32_t> UINT32 "uint32"
%nterm <double> generic_number
%nterm <bool> opt_lparen
%nterm <AstExpr> final_query filter search_expr search_unary_expr search_or_expr search_and_expr numeric_filter_expr
%nterm <AstExpr> field_cond field_cond_expr field_unary_expr field_or_expr field_and_expr tag_list

%nterm <AstKnnNode> knn_query
%nterm <std::string> opt_knn_alias

%printer { yyo << $$; } <*>;

%%

final_query:
  filter
      { driver->Set(move($1)); }
  | filter ARROW knn_query
      { driver->Set(AstKnnNode(move($1), move($3))); }

knn_query:
  LBRACKET KNN UINT32 FIELD TERM opt_knn_alias RBRACKET
    { $$ = AstKnnNode($3, $4, BytesToFtVector($5), $6); }

opt_knn_alias:
  AS TERM { $$ = move($2); }
  | { $$ = std::string{}; }

filter:
  search_expr               { $$ = move($1); }
  | STAR                    { $$ = AstStarNode(); }

search_expr:
  search_unary_expr         { $$ = move($1); }
  | search_and_expr         { $$ = move($1); }
  | search_or_expr          { $$ = move($1); }

search_and_expr:
  search_unary_expr search_unary_expr %prec AND_OP { $$ = AstLogicalNode(move($1), move($2), AstLogicalNode::AND); }
  | search_and_expr search_unary_expr %prec AND_OP { $$ = AstLogicalNode(move($1), move($2), AstLogicalNode::AND); }

search_or_expr:
  search_expr OR_OP search_and_expr                { $$ = AstLogicalNode(move($1), move($3), AstLogicalNode::OR); }
  | search_expr OR_OP search_unary_expr            { $$ = AstLogicalNode(move($1), move($3), AstLogicalNode::OR); }

search_unary_expr:
  LPAREN search_expr RPAREN           { $$ = move($2); }
  | NOT_OP search_unary_expr          { $$ = AstNegateNode(move($2)); }
  | TERM                              { $$ = AstTermNode(move($1)); }
  | UINT32                            { $$ = AstTermNode(to_string($1)); }
  | FIELD COLON field_cond            { $$ = AstFieldNode(move($1), move($3)); }

field_cond:
  TERM                                                  { $$ = AstTermNode(move($1)); }
  | UINT32                                              { $$ = AstTermNode(to_string($1)); }
  | NOT_OP field_cond                                   { $$ = AstNegateNode(move($2)); }
  | LPAREN field_cond_expr RPAREN                       { $$ = move($2); }
  | LBRACKET numeric_filter_expr RBRACKET               { $$ = move($2); }
  | LCURLBR tag_list RCURLBR                            { $$ = move($2); }

numeric_filter_expr:
opt_lparen generic_number opt_lparen generic_number { $$ = AstRangeNode($2, $1, $4, $3); }

generic_number:
  DOUBLE { $$ = $1; }
  | UINT32 { $$ = $1; }

opt_lparen:
  /* empty */ { $$ = false; }
  | LPAREN { $$ = true; }

field_cond_expr:
  field_unary_expr                       { $$ = move($1); }
  | field_and_expr                       { $$ = move($1); }
  | field_or_expr                        { $$ = move($1); }

field_and_expr:
  field_unary_expr field_unary_expr %prec AND_OP  { $$ = AstLogicalNode(move($1), move($2), AstLogicalNode::AND); }
  | field_and_expr field_unary_expr %prec AND_OP  { $$ = AstLogicalNode(move($1), move($2), AstLogicalNode::AND); }

field_or_expr:
  field_cond_expr OR_OP field_unary_expr          { $$ = AstLogicalNode(move($1), move($3), AstLogicalNode::OR); }
  | field_cond_expr OR_OP field_and_expr          { $$ = AstLogicalNode(move($1), move($3), AstLogicalNode::OR); }

field_unary_expr:
  LPAREN field_cond_expr RPAREN                  { $$ = move($2); }
  | NOT_OP field_unary_expr                      { $$ = AstNegateNode(move($2)); };
  | TERM                                         { $$ = AstTermNode(move($1)); }
  | UINT32                                       { $$ = AstTermNode(to_string($1)); }

tag_list:
  TERM                       { $$ = AstTagsNode(move($1)); }
  | UINT32                   { $$ = AstTagsNode(to_string($1)); }
  | tag_list OR_OP TERM      { $$ = AstTagsNode(move($1), move($3)); }
  | tag_list OR_OP DOUBLE    { $$ = AstTagsNode(move($1), to_string($3)); }


%%

void
dfly::search::Parser::error(const location_type& l, const string& m)
{
  cerr << l << ": " << m << '\n';
}
