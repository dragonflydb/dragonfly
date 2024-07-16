// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "src/core/json/lexer_impl.h"

#include <absl/strings/str_cat.h>

namespace dfly::json {

Lexer::Lexer() {
}

Lexer::~Lexer() {
}

std::string Lexer::UnknownTokenMsg() const {
  std::string res = absl::StrCat("Unknown token '", text(), "'");
  return res;
}

}  // namespace dfly::json
