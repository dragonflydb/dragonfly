// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

%top{
  // Our lexer needs to know about FilterParser::symbol_type
  #include "server/search/filter_parser.hh"
}

%o bison-cc-namespace="dfly.aggregate" bison-cc-parser="FilterParser"
%o namespace="dfly.aggregate"
%o lexer="FilterLexer" class="FilterScanner" lex="Lex"
%o nodefault batch

%{
  #define DFLY_FILTER_LEXER_CC 1
  #include "server/search/filter_scanner.h"
  #undef DFLY_FILTER_LEXER_CC
%}

dq       \"
sq       \'
esc_seq  \\[^\n]

%%

[[:space:]]+   // skip whitespace

"("   return FilterParser::make_LPAREN(loc());
")"   return FilterParser::make_RPAREN(loc());
","   return FilterParser::make_COMMA(loc());
"=="  return FilterParser::make_OP_EQ(loc());
"!="  return FilterParser::make_OP_NEQ(loc());
"<="  return FilterParser::make_OP_LTE(loc());
"<"   return FilterParser::make_OP_LT(loc());
">="  return FilterParser::make_OP_GTE(loc());
">"   return FilterParser::make_OP_GT(loc());
"&&"  return FilterParser::make_OP_AND(loc());
"||"  return FilterParser::make_OP_OR(loc());
"!"   return FilterParser::make_OP_NOT(loc());
"+"   return FilterParser::make_OP_PLUS(loc());
"-"   return FilterParser::make_OP_MINUS(loc());
"*"   return FilterParser::make_OP_MUL(loc());
"/"   return FilterParser::make_OP_DIV(loc());
"%"   return FilterParser::make_OP_MOD(loc());
"^"   return FilterParser::make_OP_POW(loc());

// Numbers: integer, decimal, scientific notation
[0-9]+(\.[0-9]+)?([eE][+-]?[0-9]+)?   return FilterParser::make_NUMBER(str(), loc());
\.[0-9]+([eE][+-]?[0-9]+)?            return FilterParser::make_NUMBER(str(), loc());

// Special numeric keyword -- case-insensitive (Inf, INF, inf all accepted)
[iI][nN][fF]   return FilterParser::make_NUMBER("inf", loc());

// String literals (single or double quoted, with escape sequences)
{dq}({esc_seq}|[^"\\])*{dq}   return MakeStringLit(matched_view(1, 1), loc());
{sq}({esc_seq}|[^'\\])*{sq}   return MakeStringLit(matched_view(1, 1), loc());

// Field references: @name -- strip the leading '@'
"@"[a-zA-Z_][a-zA-Z0-9_.\-]*   return FilterParser::make_FIELD(str().substr(1), loc());

// NULL keyword -- case-insensitive (null, Null, NULL all accepted)
[nN][uU][lL][lL]   return FilterParser::make_KW_NULL(loc());

// Identifiers (function names, etc.) -- lowercased
[a-zA-Z_][a-zA-Z0-9_]*   return FilterParser::make_IDENT(ToLower(str()), loc());

<<EOF>>   return FilterParser::make_YYEOF(loc());
.         throw FilterParser::syntax_error(loc(), "unexpected character: " + str());
%%
