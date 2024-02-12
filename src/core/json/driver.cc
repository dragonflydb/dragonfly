// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "src/core/json/driver.h"

#include "base/logging.h"
#include "src/core/json/lexer_impl.h"

using namespace std;

namespace dfly::json {

Driver::Driver() : lexer_(make_unique<Lexer>()) {
}

Driver::~Driver() {
}

void Driver::SetInput(string str) {
  cur_str_ = std::move(str);
  lexer_->in(cur_str_);
  path_.Clear();
}

void Driver::ResetScanner() {
  lexer_ = make_unique<Lexer>();
}

}  // namespace dfly::json
