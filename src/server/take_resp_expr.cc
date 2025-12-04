// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/take_resp_expr.h"

#include <core/overloaded.h>

namespace dfly {

TakeRespExpr::TakeRespExpr(const facade::RespExpr& expr) {
  type = expr.type;
  std::visit(dfly::Overloaded{
                 [&](facade::RespExpr::Buffer buf) { u = Buffer(buf.begin(), buf.end()); },
                 [&](const facade::RespExpr::Vec* vec) {
                   Vec new_vec;
                   for (const auto& item : *vec) {
                     new_vec.emplace_back(item);
                   }
                   u = new_vec;
                 },
                 [&](int64_t i) { u = i; },
                 [&](double d) { u = d; },
             },
             expr.u);
}

std::string_view ToSV(TakeRespExpr::Buffer buf) {
  return std::string_view{reinterpret_cast<const char*>(buf.data()), buf.size()};
}

}  // namespace dfly

namespace std {
ostream& operator<<(ostream& os, const dfly::TakeRespExpr& e) {
  using dfly::TakeRespExpr;
  using dfly::ToSV;

  switch (e.type) {
    case TakeRespExpr::Type::INT64:
      os << "i" << get<int64_t>(e.u);
      break;
    case TakeRespExpr::Type::DOUBLE:
      os << "d" << get<double>(e.u);
      break;
    case TakeRespExpr::Type::STRING:
      os << "'" << ToSV(get<TakeRespExpr::Buffer>(e.u)) << "'";
      break;
    case TakeRespExpr::Type::NIL:
      os << "nil";
      break;
    case TakeRespExpr::Type::NIL_ARRAY:
      os << "[]";
      break;
    case TakeRespExpr::Type::ARRAY:
      os << dfly::TakeRespSpan{std::get<TakeRespExpr::Vec>(e.u)};
      break;
    case TakeRespExpr::Type::ERROR:
      os << "e(" << ToSV(get<TakeRespExpr::Buffer>(e.u)) << ")";
      break;
  }

  return os;
}

ostream& operator<<(ostream& os, dfly::TakeRespSpan ras) {
  os << "[";
  if (!ras.empty()) {
    for (size_t i = 0; i < ras.size() - 1; ++i) {
      os << ras[i] << ",";
    }
    os << ras.back();
  }
  os << "]";

  return os;
}

}  // namespace std
