// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <memory>
#include <sstream>

#include "core/search/parser.hh"
#include "core/search/scanner.h"

namespace dfly {

namespace search {

class QueryDriver {
 public:
  QueryDriver();
  ~QueryDriver();

  Scanner* scanner() {
    return scanner_.get();
  }

  void SetInput(const std::string& str) {
    istr_.str(str);
    scanner()->switch_streams(&istr_);
  }

  Parser::symbol_type Lex() {
    return scanner()->ParserLex(*this);
  }

  Parser::location_type location;

 private:
  std::istringstream istr_;
  std::unique_ptr<Scanner> scanner_;
};

}  // namespace search
}  // namespace dfly
