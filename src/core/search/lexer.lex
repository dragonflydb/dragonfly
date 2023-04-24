%{
  #include <climits>
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

  Parser::symbol_type make_NUMBER (const std::string &s, const Parser::location_type& loc);
%}

int [0-9]+
blank [ \t\r]

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

{int}       return make_NUMBER (yytext, loc);
[^ \t\r]+   return Parser::make_TERM (yytext, loc);

<<EOF>>    return Parser::make_YYEOF(loc);
%%

Parser::symbol_type
make_NUMBER (const std::string &s, const Parser::location_type& loc)
{
  errno = 0;
  long n = strtol (s.c_str(), NULL, 10);
  if (! (INT_MIN <= n && n <= INT_MAX && errno != ERANGE))
    throw Parser::syntax_error (loc, "integer is out of range: " + s);
  return Parser::make_NUMBER ((int) n, loc);
}
