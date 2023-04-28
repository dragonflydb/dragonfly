// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

// We should not include lexer.h when compiling from lexer.cc file because it already
// includes lexer.h
#ifndef DFLY_LEXER_CC
#include "core/search/lexer.h"
#endif

namespace dfly {
namespace search {

class Scanner : public Lexer {
 public:
  Scanner() {
  }

  Parser::symbol_type Lex();

 private:
  std::string_view matched_view(size_t skip_left = 0, size_t skip_right = 0) const {
    std::string_view res(matcher().begin() + skip_left, matcher().size() - skip_left - skip_right);
    return res;
  }

  dfly::search::location loc() {
    return location();
  }
};

}  // namespace search
}  // namespace dfly
