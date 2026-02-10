// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "facade/resp_expr_test_utils.h"

namespace facade {

RespExpr RespExprBuilder::BuildExpr(const RESPObj& obj) {
  RespExpr expr{RespExpr::NIL};

  switch (obj.GetType()) {
    case RESPObj::Type::INTEGER: {
      expr.type = RespExpr::INT64;
      expr.u = obj.As<int64_t>().value();
      break;
    }
    case RESPObj::Type::DOUBLE: {
      expr.type = RespExpr::DOUBLE;
      expr.u = obj.As<double>().value();
      break;
    }
    case RESPObj::Type::NIL: {
      expr.type = RespExpr::NIL;
      break;
    }
    case RESPObj::Type::ERROR: {
      expr.type = RespExpr::ERROR;
      SetStringPayload(obj, &expr);
      break;
    }
    case RESPObj::Type::STRING:
    case RESPObj::Type::REPLY_STATUS: {
      expr.type = RespExpr::STRING;
      SetStringPayload(obj, &expr);
      break;
    }
    case RESPObj::Type::ARRAY: {
      expr.type = RespExpr::ARRAY;
      auto arr = obj.As<RESPArray>();
      if (arr.has_value()) {
        auto vec = std::make_unique<RespExpr::Vec>();
        vec->reserve(arr->Size());
        for (size_t i = 0; i < arr->Size(); ++i) {
          vec->push_back(BuildExpr((*arr)[i]));
        }
        expr.u = vec.get();
        owned_arrays_.emplace_back(std::move(vec));
        expr.has_support = true;
      }
      break;
    }
  }

  return expr;
}

void RespExprBuilder::SetStringPayload(const RESPObj& obj, RespExpr* expr) {
  auto sv = obj.As<std::string_view>().value_or(std::string_view{});
  expr->u = RespExpr::Buffer{reinterpret_cast<const uint8_t*>(sv.data()), sv.size()};
  expr->has_support = true;
}

}  // namespace facade
