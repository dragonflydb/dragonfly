// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <memory>
#include <optional>
#include <vector>

#include "facade/resp_expr.h"
#include "facade/resp_parser.h"

namespace facade {

class RespExprBuilder {
 public:
  RespExpr BuildExpr(const RESPObj& obj);

 private:
  void SetStringPayload(const RESPObj& obj, RespExpr* expr);

  std::vector<std::unique_ptr<RespExpr::Vec>> owned_arrays_;
  // Own copies of string data so we don't hold references to zmalloc-allocated
  // hiredis replies (which must be freed on the same thread they were allocated).
  std::vector<std::unique_ptr<char[]>> owned_strings_;
};

}  // namespace facade
