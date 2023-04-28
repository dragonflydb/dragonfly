// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <ostream>

namespace dfly {

namespace search {

class AstExpr {};

}  // namespace search
}  // namespace dfly

namespace std {

inline std::ostream& operator<<(std::ostream& os, const dfly::search::AstExpr& ast) {
  os << "ast";
  return os;
}

}  // namespace std
