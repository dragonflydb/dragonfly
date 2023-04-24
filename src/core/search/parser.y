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
  namespace dfly {
  namespace search {
    class QueryDriver;
  }
  }
}

// Added to cc file
%code {
#include "core/search/query_driver.h"

#define yylex driver.scanner()->ParserLex
}

// Only for parser
%param { QueryDriver& driver }

%locations

%define parse.trace
%define parse.error verbose  // detailed
%define parse.lac full
%define api.token.prefix {TOK_}

%token
  LPAREN  "("
  RPAREN  ")"
;

%token YYEOF
%token <std::string> TERM "term"
%token <int> NUMBER "number"
%nterm <int> bool_expr

%printer { yyo << $$; } <*>;

%%
%start input;

input:
  %empty
  | bool_expr
  {
    std::cout << $1 << std::endl;
  }
  ;

bool_expr: TERM {
  std::cout << $1 << std::endl;
} | TERM bool_expr {
   std::cout << $1 << std::endl;
}

%%

void
dfly::search::Parser::error(const location_type& l, const std::string& m)
{
  std::cerr << l << ": " << m << '\n';
}
