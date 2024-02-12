// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "src/core/json/lexer_impl.h"

#include <absl/strings/str_cat.h>

using namespace std;

namespace dfly::json {

Lexer::Lexer() {
}

Lexer::~Lexer() {
}

string Lexer::UnknownTokenMsg() const {
  return absl::StrCat("Unknown token '", text(), "'");
}

}  // namespace dfly::json
