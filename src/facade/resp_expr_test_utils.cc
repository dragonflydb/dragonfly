// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "facade/resp_expr_test_utils.h"

#include <cstddef>

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
    case RESPObj::Type::ARRAY:
    case RESPObj::Type::MAP:
    case RESPObj::Type::SET: {
      auto arr = obj.As<RESPArray>();
      if (arr.has_value()) {
        // Check if this is a null array (elements == SIZE_MAX which represents -1)
        if (arr->Size() == SIZE_MAX) {
          expr.type = RespExpr::NIL_ARRAY;
          expr.u.emplace<RespExpr::Vec*>(nullptr);
        } else {
          expr.type = RespExpr::ARRAY;
          auto vec = std::make_unique<RespExpr::Vec>();
          vec->reserve(arr->Size());
          for (size_t i = 0; i < arr->Size(); ++i) {
            vec->push_back(BuildExpr((*arr)[i]));
          }
          expr.u = vec.get();
          owned_arrays_.emplace_back(std::move(vec));
          expr.has_support = true;
        }
      }
      break;
    }
  }

  return expr;
}

void RespExprBuilder::SetStringPayload(const RESPObj& obj, RespExpr* expr) {
  auto sv = obj.As<std::string_view>().value_or(std::string_view{});
  // Copy the string data so we don't hold references into zmalloc-allocated
  // hiredis replies. The replies can then be freed on their allocating thread.
  auto owned = std::make_unique<std::string>(sv);
  expr->u = RespExpr::Buffer{reinterpret_cast<const uint8_t*>(owned->data()), owned->size()};
  expr->has_support = true;
  owned_strings_.emplace_back(std::move(owned));
}

}  // namespace facade
