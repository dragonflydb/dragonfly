%top{
  // generated in the header file.
  #include "core/json/jsonpath_grammar.hh"
}


%{
  // SECOND SECTION
%}

%o bison-cc-namespace="dfly.json" bison-cc-parser="Parser"
%o namespace="dfly.json"

// Generated class and main function
%o lexer="AbstractLexer" lex="Lex"

// our derived class from AbstractLexer
%o class="Lexer"
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
"."         return Parser::make_DOT(loc());
\w[\w_\-]*  return Parser::make_UNQ_STR(str(), loc());
<<EOF>>     printf("EOF%s\n", matcher().text());
%%

// Function definitions
