// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <memory>
#include <vector>

#include "facade/resp_expr.h"
#include "facade/resp_parser.h"

namespace facade {

// Helper class to transform RESPObj into RespExpr
// Manages lifetime of nested array vectors
class RespExprBuilder {
 public:
  RespExprBuilder() = default;

  // Build RespExpr from RESPObj
  RespExpr Build(const RESPObj& obj);

 private:
  void SetStringPayload(const RESPObj& obj, RespExpr* expr);

  // Stores owned array vectors to manage lifetime
  std::vector<std::unique_ptr<RespExpr::Vec>> owned_arrays_;
};

}  // namespace facade
