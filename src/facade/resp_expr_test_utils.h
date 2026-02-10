// Copyright 2024, DragonflyDB authors.  All rights reserved.
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

  void Clear() {
    owned_arrays_.clear();
  }

 private:
  void SetStringPayload(const RESPObj& obj, RespExpr* expr);

  std::vector<std::unique_ptr<RespExpr::Vec>> owned_arrays_;
};

}  // namespace facade
