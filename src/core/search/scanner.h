// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

// We should not include lexer.h when compiling from lexer.cc file because it already
// includes lexer.h
#ifndef DFLY_LEXER_CC
#include "core/search/lexer.h"
#endif

#include <absl/strings/str_cat.h>

#include "base/logging.h"

namespace dfly {
namespace search {

class Scanner : public Lexer {
 public:
  Scanner() : params_{nullptr} {
  }

  Parser::symbol_type Lex();

  void SetParams(const QueryParams* params) {
    params_ = params;
  }

 private:
  std::string_view matched_view(size_t skip_left = 0, size_t skip_right = 0) const {
    std::string_view res(matcher().begin() + skip_left, matcher().size() - skip_left - skip_right);
    return res;
  }

  dfly::search::location loc() {
    return location();
  }

  Parser::symbol_type ParseParam(std::string_view name, const Parser::location_type& loc) {
    name.remove_prefix(1);  // drop $ symbol

    std::string_view str = (*params_)[name];
    if (str.empty())
      throw std::runtime_error(absl::StrCat("Query parameter ", name, " not found"));

    uint32_t val = 0;
    if (!absl::SimpleAtoi(str, &val))
      return Parser::make_TERM(std::string{str}, loc);

    return Parser::make_UINT32(std::string{str}, loc);
  }

 private:
  const QueryParams* params_;
};

}  // namespace search
}  // namespace dfly
