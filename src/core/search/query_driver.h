// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <memory>

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

  Parser::location_type location;

 private:
  std::unique_ptr<Scanner> scanner_;
};

}  // namespace search
}  // namespace dfly
