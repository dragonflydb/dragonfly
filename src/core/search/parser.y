%skeleton "lalr1.cc" // -*- C++ -*-
%require "3.5.1"    // That's what's present on ubuntu 20.04.

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
  KNN    "KNN"
;

%token AND_OP

// Needed 0 at the end to satisfy bison 3.5.1
%token YYEOF 0
%token <std::string> TERM "term" PARAM "param" FIELD "field"

%precedence TERM
%left OR_OP AND_OP
%right NOT_OP
%precedence LPAREN RPAREN

%token <int64_t> INT64 "int64"
%nterm <AstExpr> final_query filter search_expr field_cond field_cond_expr tag_list

%printer { yyo << $$; } <*>;

%%

final_query:
  filter
      { driver->Set(move($1)); }
  | filter ARROW LBRACKET KNN INT64 FIELD TERM RBRACKET
      { driver->Set(AstKnnNode(move($1), $5, $6, driver->GetParams().knn_vec)); }

filter:
  search_expr { $$ = move($1); }
  | STAR { $$ = AstStarNode(); }

search_expr:
 LPAREN search_expr RPAREN              { $$ = move($2); }
 | search_expr search_expr %prec AND_OP { $$ = AstLogicalNode(move($1), move($2), AstLogicalNode::AND); }
 | search_expr OR_OP search_expr        { $$ = AstLogicalNode(move($1), move($3), AstLogicalNode::OR); }
 | NOT_OP search_expr                   { $$ = AstNegateNode(move($2)); }
 | TERM                                 { $$ = AstTermNode(move($1)); }
 | FIELD COLON field_cond               { $$ = AstFieldNode(move($1), move($3)); }

field_cond:
  TERM                                  { $$ = AstTermNode(move($1)); }
  | NOT_OP field_cond                   { $$ = AstNegateNode(move($2)); }
  | LPAREN field_cond_expr RPAREN       { $$ = move($2); }
  | LBRACKET INT64 INT64 RBRACKET       { $$ = AstRangeNode(move($2), move($3)); }
  | LCURLBR tag_list RCURLBR            { $$ = move($2); }

field_cond_expr:
  LPAREN field_cond_expr RPAREN                   { $$ = move($2); }
  | field_cond_expr field_cond_expr %prec AND_OP  { $$ = AstLogicalNode(move($1), move($2), AstLogicalNode::AND); }
  | field_cond_expr OR_OP field_cond_expr         { $$ = AstLogicalNode(move($1), move($3), AstLogicalNode::OR); }
  | NOT_OP field_cond_expr                        { $$ = AstNegateNode(move($2)); };
  | TERM                                          { $$ = AstTermNode(move($1)); }

tag_list:
  TERM                       { $$ = AstTagsNode(move($1)); }
  | tag_list OR_OP TERM      { $$ = AstTagsNode(move($1), move($3)); }

%%

void
dfly::search::Parser::error(const location_type& l, const string& m)
{
  cerr << l << ": " << m << '\n';
}
