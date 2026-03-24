// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

%skeleton "lalr1.cc" // -*- C++ -*-
%require "3.5"

%defines  // emit filter_parser.hh

%define api.namespace {dfly::aggregate}
%define api.token.raw
%define api.token.constructor
%define api.value.type variant
%define api.parser.class {FilterParser}
%define parse.assert
%define api.value.automove true

// Added to the generated header before the parser class declaration.
%code requires {
  #include "server/search/filter_expr.h"

  namespace dfly {
  namespace aggregate {
    class FilterDriver;
  }
  }
}

// Added to the generated .cc file.
%code {
  #include <absl/strings/numbers.h>

  #include <cmath>
  #include <limits>

  #include "base/logging.h"
  #include "server/search/filter_driver.h"

  // GCC 13+ emits spurious -Wmaybe-uninitialized warnings in bison-generated code.
  #if !defined(__clang__) && defined(__GNUC__) && __GNUC__ >= 13
  #pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
  #endif

  #define yylex driver->scanner()->Lex

  using namespace dfly::aggregate;

  static double ParseNumber(const std::string& s) {
    if (s == "inf")
      return std::numeric_limits<double>::infinity();
    double v = 0.0;
    bool ok = absl::SimpleAtod(s, &v);
    DCHECK(ok) << "invalid number token: " << s;
    return v;
  }

  template <typename T>
  static FilterExpr MakeNode(T val) {
    return std::make_unique<FilterExprNode>(std::move(val));
  }
}

%parse-param { FilterDriver* driver }

%locations

%define parse.trace
%define parse.error verbose
%define parse.lac full
%define api.token.prefix {TOK_}

%token
  LPAREN  "("
  RPAREN  ")"
  COMMA   ","
  OP_EQ   "=="
  OP_NEQ  "!="
  OP_LT   "<"
  OP_LTE  "<="
  OP_GT   ">"
  OP_GTE  ">="
  OP_AND  "&&"
  OP_OR   "||"
  OP_NOT  "!"
  OP_PLUS  "+"
  OP_MINUS "-"
  OP_MUL   "*"
  OP_DIV   "/"
  OP_MOD   "%"
  OP_POW   "^"
  KW_NULL  "NULL"
;

// Needed 0 at the end to satisfy bison 3.5
%token YYEOF 0

%token <std::string> NUMBER  "number"
%token <std::string> STRING  "string"
%token <std::string> FIELD   "@field"
%token <std::string> IDENT   "identifier"

%nterm <FilterExpr>              expr
%nterm <std::vector<FilterExpr>> arg_list opt_arg_list

// Operator precedence -- lowest to highest.
%left  OP_OR
%left  OP_AND
%right OP_NOT
%left  OP_EQ OP_NEQ
%left  OP_LT OP_LTE OP_GT OP_GTE
%left  OP_PLUS OP_MINUS
%left  OP_MUL OP_DIV OP_MOD
%right OP_POW
%right UMINUS

%%

final_expr:
  expr  { driver->Set(std::move($1)); }
;

expr:
    expr OP_OR  expr  { $$ = MakeNode(LogicExpr{LogicOp::OR,  std::move($1), std::move($3)}); }
  | expr OP_AND expr  { $$ = MakeNode(LogicExpr{LogicOp::AND, std::move($1), std::move($3)}); }
  | OP_NOT expr       { $$ = MakeNode(NotExpr{std::move($2)}); }
  | expr OP_EQ  expr  { $$ = MakeNode(CmpExpr{CmpOp::EQ,  std::move($1), std::move($3)}); }
  | expr OP_NEQ expr  { $$ = MakeNode(CmpExpr{CmpOp::NEQ, std::move($1), std::move($3)}); }
  | expr OP_LT  expr  { $$ = MakeNode(CmpExpr{CmpOp::LT,  std::move($1), std::move($3)}); }
  | expr OP_LTE expr  { $$ = MakeNode(CmpExpr{CmpOp::LTE, std::move($1), std::move($3)}); }
  | expr OP_GT  expr  { $$ = MakeNode(CmpExpr{CmpOp::GT,  std::move($1), std::move($3)}); }
  | expr OP_GTE expr  { $$ = MakeNode(CmpExpr{CmpOp::GTE, std::move($1), std::move($3)}); }
  | expr OP_PLUS  expr { $$ = MakeNode(ArithExpr{ArithOp::ADD, std::move($1), std::move($3)}); }
  | expr OP_MINUS expr { $$ = MakeNode(ArithExpr{ArithOp::SUB, std::move($1), std::move($3)}); }
  | expr OP_MUL   expr { $$ = MakeNode(ArithExpr{ArithOp::MUL, std::move($1), std::move($3)}); }
  | expr OP_DIV   expr { $$ = MakeNode(ArithExpr{ArithOp::DIV, std::move($1), std::move($3)}); }
  | expr OP_MOD   expr { $$ = MakeNode(ArithExpr{ArithOp::MOD, std::move($1), std::move($3)}); }
  | expr OP_POW   expr { $$ = MakeNode(ArithExpr{ArithOp::POW, std::move($1), std::move($3)}); }
  | OP_MINUS expr %prec UMINUS { $$ = MakeNode(NegateExpr{std::move($2)}); }
  | NUMBER  { $$ = MakeNode(NumLiteral{ParseNumber($1)}); }
  | STRING  { $$ = MakeNode(StrLiteral{std::move($1)}); }
  | FIELD   { $$ = MakeNode(FieldRef{std::move($1)}); }
  | KW_NULL { $$ = MakeNode(NullLiteral{}); }
  | IDENT LPAREN opt_arg_list RPAREN
      {
        FuncCallExpr fc;
        fc.name = std::move($1);
        fc.args = std::move($3);
        $$ = MakeNode(std::move(fc));
      }
  | LPAREN expr RPAREN { $$ = std::move($2); }
;

opt_arg_list:
  /* empty */  { /* $$ default-constructed as empty vector */ }
  | arg_list   { $$ = std::move($1); }
;

arg_list:
  expr
    {
      $$.push_back(std::move($1));
    }
  | arg_list COMMA expr
    {
      $$ = $1;
      $$.push_back(std::move($3));
    }
;

%%

void dfly::aggregate::FilterParser::error(const location_type& l, const std::string& m) {
  driver->Error(l, m);
}
