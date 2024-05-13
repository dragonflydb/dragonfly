%skeleton "lalr1.cc" // -*- C++ -*-
%require "3.5"  // fedora 32 has this one.

%defines  // %header starts from 3.8.1

%define api.namespace {dfly::json}
%define api.token.raw
%define api.token.constructor
%define api.value.type variant
%define api.parser.class {Parser}
%define parse.assert

// Added to header file before parser declaration.
%code requires {
  #include "src/core/json/path.h"
  namespace dfly {
  namespace json {
    class Driver;
  }
  }
}

// Added to cc file
%code {

#include "src/core/json/lexer_impl.h"
#include "src/core/json/driver.h"

// Have to disable because GCC doesn't understand `symbol_type`'s union
// implementation
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"

#define yylex driver->lexer()->Lex

using namespace std;
}

%parse-param { Driver *driver  }

%locations

%define parse.trace
%define parse.error verbose  // detailed
%define parse.lac full
%define api.token.prefix {TOK_}

%token
  LBRACKET "["
  RBRACKET "]"
  COLON    ":"
  LPARENT  "("
  RPARENT  ")"
  ROOT "$"
  DOT  "."
  WILDCARD "*"
  DESCENT ".."

// Needed 0 at the end to satisfy bison 3.5.1
%token YYEOF 0
%token <std::string> UNQ_STR "unquoted string"
%token <int>  INT "integer"

%nterm <std::string> identifier
%nterm <PathSegment> bracket_index


%%
// Based on the following specification:
// https://danielaparker.github.io/JsonCons.Net/articles/JsonPath/Specification.html

jsonpath: ROOT { /* skip adding root */ } opt_relative_location
         | function_expr opt_relative_location

opt_relative_location:
        | relative_location

relative_location: DOT relative_path
        | DESCENT { driver->AddSegment(PathSegment{SegmentType::DESCENT}); } relative_path
        | LBRACKET bracket_index RBRACKET { driver->AddSegment($2); } opt_relative_location

relative_path: identifier { driver->AddIdentifier($1); } opt_relative_location
        | WILDCARD { driver->AddWildcard(); } opt_relative_location


identifier: UNQ_STR
         // | single_quoted_string | double_quoted_string

bracket_index: WILDCARD { $$ = PathSegment{SegmentType::INDEX, IndexExpr::All()}; }
              | INT { $$ = PathSegment(SegmentType::INDEX, IndexExpr($1, $1)); }
              | INT COLON INT { $$ = PathSegment(SegmentType::INDEX, IndexExpr::HalfOpen($1, $3)); }
              | INT COLON { $$ = PathSegment(SegmentType::INDEX, IndexExpr($1, INT_MAX)); }
              | COLON INT { $$ = PathSegment(SegmentType::INDEX, IndexExpr::HalfOpen(0, $2)); }

function_expr: UNQ_STR { driver->AddFunction($1); } LPARENT ROOT relative_location RPARENT
%%


void dfly::json::Parser::error(const location_type& l, const string& m)
{
  driver->Error(l, m);
}
