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

  Parser::symbol_type make_StringLit(string_view src, const Parser::location_type& loc);
  Parser::symbol_type make_TagVal(string_view src, bool is_prefix, const Parser::location_type& loc);
%}

dq    \"
sq    \'
esc_chars ['"\?\\abfnrtv]
esc_seq \\{esc_chars}
term_char \w
tag_val_char {term_char}|\\[,.<>{}\[\]\\\"\':;!@#$%^&*()\-+=~\/ ]
asterisk_char \*


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
","            return Parser::make_COMMA (loc());
"KNN"          return Parser::make_KNN (loc());
"AS"           return Parser::make_AS (loc());
"EF_RUNTIME"   return Parser::make_EF_RUNTIME (loc());

[0-9]{1,9}                     return Parser::make_UINT32(str(), loc());
[+-]?(([0-9]*[.])?[0-9]+|inf)  return Parser::make_DOUBLE(str(), loc());

{dq}([^"]|{esc_seq})*{dq}      return make_StringLit(matched_view(1, 1), loc());
{sq}([^']|{esc_seq})*{sq}      return make_StringLit(matched_view(1, 1), loc());

"$"{term_char}+                return ParseParam(str(), loc());
"@"{term_char}+                return Parser::make_FIELD(str(), loc());
{term_char}+{asterisk_char}    return Parser::make_PREFIX(str(), loc());

{term_char}+                   return Parser::make_TERM(str(), loc());
{tag_val_char}+{asterisk_char} return make_TagVal(str(), true, loc());
{tag_val_char}+                return make_TagVal(str(), false, loc());

<<EOF>>    return Parser::make_YYEOF(loc());
%%

Parser::symbol_type make_StringLit(string_view src, const Parser::location_type& loc) {
  string res;
  if (!absl::CUnescape(src, &res))
    throw Parser::syntax_error (loc, "bad escaped string: " + string(src));

  return Parser::make_TERM(res, loc);
}

Parser::symbol_type make_TagVal(string_view src, bool is_prefix, const Parser::location_type& loc) {
  string res;
  res.reserve(src.size());

  bool escaped = false;
  size_t len = is_prefix ? src.size() - 1 : src.size(); // Exclude the '*' at the end for prefix

  for (size_t i = 0; i < len; ++i) {
    if (escaped) {
      escaped = false;
    } else if (src[i] == '\\') {
      escaped = true;
      continue;
    }
    res.push_back(src[i]);
  }

  // Add '*' back for prefix
  if (is_prefix) {
    res.push_back('*');
    return Parser::make_PREFIX(res, loc);
  }

  return Parser::make_TAG_VAL(res, loc);
}
