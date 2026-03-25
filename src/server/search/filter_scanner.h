// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

// Avoid re-including the generated header when compiling filter_lexer.cc itself
#ifndef DFLY_FILTER_LEXER_CC
#include "server/search/filter_lexer.h"
#endif

#include <absl/strings/ascii.h>
#include <absl/strings/escaping.h>

namespace dfly::aggregate {

class FilterScanner : public FilterLexer {
 public:
  FilterScanner() = default;

  FilterParser::symbol_type Lex() override;

 private:
  // Returns a view of the current match, optionally skipping characters at each end.
  std::string_view matched_view(size_t skip_left = 0, size_t skip_right = 0) const {
    return std::string_view(matcher().begin() + skip_left,
                            matcher().size() - skip_left - skip_right);
  }

  dfly::aggregate::location loc() {
    return location();
  }

  // Unescape a string literal body (quotes already stripped) and return a STRING token.
  FilterParser::symbol_type MakeStringLit(std::string_view src,
                                          const FilterParser::location_type& l) {
    std::string res;
    if (!absl::CUnescape(src, &res))
      throw FilterParser::syntax_error(l, "bad escaped string: " + std::string(src));
    return FilterParser::make_STRING(res, l);
  }

  // Return a lowercase copy of s (for identifier/function-name normalisation).
  std::string ToLower(std::string s) const {
    absl::AsciiStrToLower(&s);
    return s;
  }
};

}  // namespace dfly::aggregate
