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
  NOT_OP  "~"
;

%token YYEOF
%token <std::string> TERM "term" PARAM "param" FIELD "field"

%token <int64_t> INT64 "int64"
%nterm <AstExpr> search_expr

%printer { yyo << $$; } <*>;

%%

query:
  search_expr
  | query search_expr
  ;

search_expr: TERM {
  cout << $1 << endl;
}

%%

void
dfly::search::Parser::error(const location_type& l, const string& m)
{
  cerr << l << ": " << m << '\n';
}
