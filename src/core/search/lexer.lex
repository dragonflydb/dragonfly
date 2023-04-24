
/* Seems that flex does not have unicode support.
   TODO: to consider https://en.wikipedia.org/wiki/RE/flex in the future.
*/
%{
  #include <absl/strings/escaping.h>
  #include <absl/strings/numbers.h>
  #include "base/logging.h"
  #include "core/search/query_driver.h"

  // Define main lexer function. QueryDriver is the shared state between scanner and parser
  #undef YY_DECL
  #define YY_DECL auto dfly::search::Scanner::ParserLex(QueryDriver& driver) -> Parser::symbol_type
%}

%option noyywrap nounput noinput batch debug
%option yyclass="dfly::Scanner"
%option c++

/* Declarations before lexer implementation.  */
%{
  // A number symbol corresponding to the value in S.
  using dfly::search::Parser;
  using namespace std;

  Parser::symbol_type make_INT64 (string_view, const Parser::location_type& loc);
  Parser::symbol_type make_StringLit(string_view src, const Parser::location_type& loc);
%}

blank [ \t\r]
dq    \"
esc_chars ['"\?\\abfnrtv]
esc_seq \\{esc_chars}
str_char ([^"]|{esc_seq})

%{
  // Code run each time a pattern is matched.
  # define YY_USER_ACTION  loc.columns (yyleng);
%}

%%

%{
  // A handy shortcut to the location held by the driver.
  auto& loc = driver.location;
  // Code run each time yylex is called.
  loc.step ();
%}

{blank}+   loc.step ();

\n         loc.lines (yyleng); loc.step ();

"("        return Parser::make_LPAREN (loc);
")"        return Parser::make_RPAREN (loc);

-?[0-9]+    return make_INT64(Matched(), loc);

{dq}{str_char}*{dq}  return make_StringLit(string_view{YYText(), size_t(YYLeng())}, loc);

[[:alnum:]]+   return Parser::make_TERM(Matched(), loc);

<<EOF>>    return Parser::make_YYEOF(loc);
%%

Parser::symbol_type
make_INT64 (string_view str, const Parser::location_type& loc)
{
  int64_t val = 0;
  if (!absl::SimpleAtoi(str, &val))
    throw Parser::syntax_error (loc, "not an integer or out of range: " + string(str));

  return Parser::make_INT64(val, loc);
}

Parser::symbol_type make_StringLit(string_view src, const Parser::location_type& loc) {
  DCHECK_GE(src.size(), 2u);

  // Remove quotes
  src.remove_prefix(1);
  src.remove_suffix(1);
  string res;
  if (!absl::CUnescape(src, &res)) {
    throw Parser::syntax_error (loc, "bad escaped string: " + string(src));
  }
  return Parser::make_TERM(res, loc);
}
