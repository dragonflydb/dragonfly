%top{
  // Our lexer need to know about Parser::symbol_type
  #include "core/search/parser.hh"
  #include "core/search/tag_types.h" // Include TagType enum
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
%o nodefault batch case-insensitive
/* %o debug */

/* Declarations before lexer implementation.  */
%{
  // A number symbol corresponding to the value in S.
  using dfly::search::Parser;
  using namespace std;
  using dfly::search::TagType;

  Parser::symbol_type make_StringLit(string_view src, const Parser::location_type& loc);
  Parser::symbol_type make_Tag(string_view src, TagType type, const Parser::location_type& loc);
%}

dq         \"
sq         \'
esc_chars  ['"\?\\abfnrtv]
esc_seq    \\{esc_chars}
term_ch    \w
tag_val_base_ch [^,.<>{}\[\]\\\"\?':;!@#$%^&*()\-+=~\/| ]|\\.
tag_val_ch {tag_val_base_ch}+(:+{tag_val_base_ch}*)*
astrsk_ch  \*


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
"M"            return Parser::make_GEOUNIT_M (loc());
"KM"           return Parser::make_GEOUNIT_KM (loc());
"MI"           return Parser::make_GEOUNIT_MI (loc());
"FT"           return Parser::make_GEOUNIT_FT (loc());

[0-9]{1,9}                          return Parser::make_UINT32(str(), loc());
[+-]?(([0-9]*[.])?[0-9]+|inf)       return Parser::make_DOUBLE(str(), loc());

{dq}([^"]|{esc_seq})*{dq}           return make_StringLit(matched_view(1, 1), loc());
{sq}([^']|{esc_seq})*{sq}           return make_StringLit(matched_view(1, 1), loc());

"$"{term_ch}+                       return ParseParam(str(), loc());
"@"{term_ch}+                       return Parser::make_FIELD(str(), loc());
{astrsk_ch}{term_ch}+{astrsk_ch}    return Parser::make_INFIX(string{matched_view(1, 1)}, loc());
{term_ch}+{astrsk_ch}               return Parser::make_PREFIX(string{matched_view(0, 1)}, loc());
{astrsk_ch}{term_ch}+               return Parser::make_SUFFIX(string{matched_view(1, 0)}, loc());

{term_ch}+                          return Parser::make_TERM(str(), loc());
{tag_val_ch}+{astrsk_ch}            return make_Tag(str(), TagType::PREFIX, loc());
{astrsk_ch}{tag_val_ch}+            return make_Tag(str(), TagType::SUFFIX, loc());
{astrsk_ch}{tag_val_ch}+{astrsk_ch} return make_Tag(str(), TagType::INFIX, loc());
{tag_val_ch}+                       return make_Tag(str(), TagType::REGULAR, loc());

<<EOF>> return Parser::make_YYEOF(loc());
%%

Parser::symbol_type make_StringLit(string_view src, const Parser::location_type& loc) {
  string res;
  if (!absl::CUnescape(src, &res))
    throw Parser::syntax_error (loc, "bad escaped string: " + string(src));

  return Parser::make_TERM(res, loc);
}

Parser::symbol_type make_Tag(string_view src, TagType type, const Parser::location_type& loc) {
  string res;
  res.reserve(src.size());

  // Determine processing boundaries
  size_t start = (type == TagType::SUFFIX || type == TagType::INFIX) ? 1 : 0;
  size_t end = src.size();
  if (type == TagType::PREFIX || type == TagType::INFIX) {
    end--; // Skip the last '*' character
  }

    // Handle escaping
  bool escaped = false;
  for (size_t i = start; i < end; ++i) {
    if (escaped) {
      escaped = false;
    } else if (src[i] == '\\') {
      escaped = true;
      continue;
    }
    res.push_back(src[i]);
  }

  // Return the appropriate token type
  switch (type) {
    case TagType::PREFIX:
      return Parser::make_PREFIX(res, loc);
    case TagType::SUFFIX:
      return Parser::make_SUFFIX(res, loc);
    case TagType::INFIX:
      return Parser::make_INFIX(res, loc);
    case TagType::REGULAR:
    default:
      return Parser::make_TAG_VAL(res, loc);
  }
}
