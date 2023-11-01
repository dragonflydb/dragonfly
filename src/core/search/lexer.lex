%top{
  // Our lexer need to know about Parser::symbol_type
  #include "core/search/parser.hh"
}

%{
  #include <absl/strings/escaping.h>
  #include <absl/strings/numbers.h>

  #include "base/logging.h"

  #define DFLY_LEXER_CC 1
     #include "core/search/scanner.h"
  #undef DFLY_LEXER_CC
%}

%o bison-cc-namespace="dfly.search" bison-cc-parser="Parser"
%o namespace="dfly.search"
%o class="Scanner" lex="Lex"
%o nodefault batch
/* %o debug */

/* Declarations before lexer implementation.  */
%{
  // A number symbol corresponding to the value in S.
  using dfly::search::Parser;
  using namespace std;

  Parser::symbol_type make_DOUBLE(string_view, const Parser::location_type& loc);
  Parser::symbol_type make_UINT32(string_view, const Parser::location_type& loc);
  Parser::symbol_type make_StringLit(string_view src, const Parser::location_type& loc);
%}

blank [ \t\r]
dq    \"
sq    \'
esc_chars ['"\?\\abfnrtv]
esc_seq \\{esc_chars}
str_char ([^"]|{esc_seq})
term_char [_]|\w


%{
  // Code run each time a pattern is matched.
%}

%%

%{
  // Code run each time lex() is called.
%}

[[:space:]]+   // skip white space

"("            return Parser::make_LPAREN (loc());
")"            return Parser::make_RPAREN (loc());
"*"            return Parser::make_STAR (loc());
"-"            return Parser::make_NOT_OP (loc());
":"            return Parser::make_COLON (loc());
"=>"           return Parser::make_ARROW (loc());
"["            return Parser::make_LBRACKET (loc());
"]"            return Parser::make_RBRACKET (loc());
"{"            return Parser::make_LCURLBR (loc());
"}"            return Parser::make_RCURLBR (loc());
"|"            return Parser::make_OR_OP (loc());
"KNN"          return Parser::make_KNN (loc());
"AS"           return Parser::make_AS (loc());

[0-9]+                   return make_UINT32(matched_view(), loc());
[+-]?(([0-9]*[.])?[0-9]+|inf)  return make_DOUBLE(matched_view(), loc());

{dq}{str_char}*{dq}  return make_StringLit(matched_view(1, 1), loc());
{sq}{str_char}*{sq}  return make_StringLit(matched_view(1, 1), loc());

"$"{term_char}+ return ParseParam(str(), loc());
"@"{term_char}+ return Parser::make_FIELD(str(), loc());

{term_char}+   return Parser::make_TERM(str(), loc());

<<EOF>>    return Parser::make_YYEOF(loc());
%%

Parser::symbol_type make_UINT32 (string_view str, const Parser::location_type& loc) {
  uint32_t val = 0;
  if (!absl::SimpleAtoi(str, &val))
    throw Parser::syntax_error (loc, "not an unsigned integer or out of range: " + string(str));

  return Parser::make_UINT32(val, loc);
}

Parser::symbol_type make_DOUBLE (string_view str, const Parser::location_type& loc) {
  double val = 0;
  if (!absl::SimpleAtod(str, &val))
    throw Parser::syntax_error (loc, "not a double or out of range: " + string(str));

  return Parser::make_DOUBLE(val, loc);
}

Parser::symbol_type make_StringLit(string_view src, const Parser::location_type& loc) {
  string res;
  if (!absl::CUnescape(src, &res))
    throw Parser::syntax_error (loc, "bad escaped string: " + string(src));

  return Parser::make_TERM(res, loc);
}
