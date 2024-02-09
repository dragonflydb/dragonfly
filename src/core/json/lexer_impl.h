// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

// We should not include lexer.h when compiling from lexer.cc file because it already
// includes lexer.h
#ifndef DFLY_LEXER_CC
#include "src/core/json/jsonpath_lexer.h"
#endif

#include "src/core/json/jsonpath_grammar.hh"

namespace dfly {
namespace json {

class Lexer : public AbstractLexer {
 public:
  Lexer();
  ~Lexer();

  Parser::symbol_type Lex() final;

 private:
  dfly::json::location loc() {
    return location();
  }

  std::string UnknownTokenMsg() const;
};

}  // namespace json
}  // namespace dfly
