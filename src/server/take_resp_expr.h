// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <variant>

#include "facade/resp_expr.h"

namespace dfly {

class TakeRespExpr {
 public:
  using Buffer = std::vector<uint8_t>;

  using Type = facade::RespExpr::Type;

  using Vec = std::vector<TakeRespExpr>;
  Type type;

  std::variant<int64_t, double, Buffer, Vec> u;

  TakeRespExpr(const facade::RespExpr& expr);

  std::string_view GetView() const {
    const Buffer& buffer = GetBuf();
    return {reinterpret_cast<const char*>(buffer.data()), buffer.size()};
  }

  std::string GetString() const {
    return std::string(GetView());
  }

  const Buffer& GetBuf() const {
    return std::get<Buffer>(u);
  }

  const Vec& GetVec() const {
    return std::get<Vec>(u);
  }

  std::optional<int64_t> GetInt() const {
    return std::holds_alternative<int64_t>(u) ? std::make_optional(std::get<int64_t>(u))
                                              : std::nullopt;
  }
};

using TakeRespVec = TakeRespExpr::Vec;
using TakeRespSpan = absl::Span<const TakeRespExpr>;

}  // namespace dfly

namespace std {

ostream& operator<<(ostream& os, const dfly::TakeRespExpr& e);
ostream& operator<<(ostream& os, dfly::TakeRespSpan rspan);

}  // namespace std
