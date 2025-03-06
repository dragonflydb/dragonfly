// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/strings/ascii.h>
#include <absl/types/span.h>

#include <optional>
#include <string_view>
#include <variant>
#include <vector>

#include "facade/facade_types.h"

namespace facade {

class RespExpr {
 public:
  using Buffer = absl::Span<uint8_t>;

  enum Type : uint8_t { STRING, ARRAY, INT64, DOUBLE, NIL, NIL_ARRAY, ERROR };

  using Vec = std::vector<RespExpr>;
  Type type;
  bool has_support;  // whether pointers in this item are supported by the external storage.

  std::variant<int64_t, double, Buffer, Vec*> u;

  RespExpr(Type t = NIL) : type(t), has_support(false) {
  }

  static Buffer buffer(std::string* s) {
    return Buffer{reinterpret_cast<uint8_t*>(s->data()), s->size()};
  }

  std::string_view GetView() const {
    Buffer buffer = GetBuf();
    return {reinterpret_cast<const char*>(buffer.data()), buffer.size()};
  }

  std::string GetString() const {
    return std::string(GetView());
  }

  Buffer GetBuf() const {
    return std::get<Buffer>(u);
  }

  const Vec& GetVec() const {
    return *std::get<Vec*>(u);
  }

  std::optional<int64_t> GetInt() const {
    return std::holds_alternative<int64_t>(u) ? std::make_optional(std::get<int64_t>(u))
                                              : std::nullopt;
  }

  size_t UsedMemory() const {
    return 0;
  }

  static const char* TypeName(Type t);

  static void VecToArgList(const Vec& src, CmdArgVec* dest);
};

using RespVec = RespExpr::Vec;
using RespSpan = absl::Span<const RespExpr>;

inline std::string_view ToSV(RespExpr::Buffer buf) {
  return std::string_view{reinterpret_cast<char*>(buf.data()), buf.size()};
}

}  // namespace facade

namespace std {

ostream& operator<<(ostream& os, const facade::RespExpr& e);
ostream& operator<<(ostream& os, facade::RespSpan rspan);

}  // namespace std
