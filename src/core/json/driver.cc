// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "src/core/json/driver.h"

#include "src/core/json/lexer_impl.h"

namespace dfly::json {

Driver::Driver() : lexer_(std::make_unique<Lexer>()) {
}

Driver::~Driver() {
}

void Driver::SetInput(std::string str) {
  cur_str_ = std::move(str);
  lexer_->in(cur_str_);
}

}  // namespace dfly::json
