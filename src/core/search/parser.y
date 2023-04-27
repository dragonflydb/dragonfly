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
  LPAREN  "("
  RPAREN  ")"
  STAR    "*"
  ARROW   "=>"
  COLON   ":"
  LBRACKET "["
  RBRACKET "]"
;

%precedence NOT_OP

// Needed 0 at the end to satisfy bison 3.5.1
%token YYEOF 0
%token <std::string> TERM "term" PARAM "param" FIELD "field"

%token <int64_t> INT64 "int64"
%nterm <AstExpr> search_expr field_filter field_cond range_value term_list opt_neg_term

%printer { yyo << $$; } <*>;

%%

query:
  search_expr
  | query search_expr
  ;


search_expr:
 LPAREN search_expr RPAREN { $$ = $2; }
 | NOT_OP search_expr { $$ = AstExpr{}; };
 | TERM { }
 | field_filter;

field_filter:
   FIELD COLON field_cond { $$ = AstExpr{}; }

field_cond: term_list | range_value
 range_value: LBRACKET INT64 INT64 RBRACKET { $$ = AstExpr{}; }

term_list:
  opt_neg_term |
  LPAREN term_list opt_neg_term RPAREN { };

opt_neg_term:
  TERM { } | NOT_OP TERM { $$ = AstExpr{}; };

%%

void
dfly::search::Parser::error(const location_type& l, const string& m)
{
  cerr << l << ": " << m << '\n';
}
