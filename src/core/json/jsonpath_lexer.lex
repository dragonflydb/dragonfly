%top{
  // generated in the header file.
  #include "core/json/jsonpath_grammar.hh"
}


%{
  #include <absl/strings/numbers.h>
  #include "base/logging.h"
%}

%o bison-cc-namespace="dfly.json" bison-cc-parser="Parser"
%o namespace="dfly.json"

// Generated class and main function
%o lexer="AbstractLexer" lex="Lex"

// our derived class from AbstractLexer
%o class="Lexer"

/* nodefault removes default echo rule */
%o nodefault batch
%option unicode

/* Declarations before lexer implementation.  */
%{
    #define DFLY_LEXER_CC 1
    #include "src/core/json/lexer_impl.h"
    #undef DFLY_LEXER_CC
%}


%{
  // Code run each time a pattern is matched.
%}

%%

%{
  // Code run each time lex() is called.
%}

[[:space:]]+     ; // skip white space

"$"         return Parser::make_ROOT(loc());
".."        return Parser::make_DESCENT(loc());
"."         return Parser::make_DOT(loc());
":"         return Parser::make_COLON(loc());
"["         return Parser::make_LBRACKET(loc());
"]"         return Parser::make_RBRACKET(loc());
"*"         return Parser::make_WILDCARD(loc());
"("         return Parser::make_LPARENT(loc());
")"         return Parser::make_RPARENT(loc());
"'"         return Parser::make_SINGLE_QUOTE(loc());
"\""        return Parser::make_DOUBLE_QUOTE(loc());
-?[0-9]{1,9}  {
              int val;
              CHECK(absl::SimpleAtoi(str(), &val));
              return Parser::make_INT(val, loc());
            }

\w[\w_\-]*  return Parser::make_UNQ_STR(str(), loc());
<<EOF>>     return Parser::make_YYEOF(loc());
.           throw Parser::syntax_error(loc(), UnknownTokenMsg());
%%

// Function definitions
