// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/owned_resp_expr.h"

#include <core/overloaded.h>

namespace dfly {

OwnedRespExpr::OwnedRespExpr(const facade::RespExpr& expr) {
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

std::string_view ToSV(OwnedRespExpr::Buffer buf) {
  return std::string_view{reinterpret_cast<const char*>(buf.data()), buf.size()};
}

}  // namespace dfly

namespace std {
ostream& operator<<(ostream& os, const dfly::OwnedRespExpr& e) {
  using dfly::OwnedRespExpr;
  using dfly::ToSV;

  switch (e.type) {
    case OwnedRespExpr::Type::INT64:
      os << "i" << get<int64_t>(e.u);
      break;
    case OwnedRespExpr::Type::DOUBLE:
      os << "d" << get<double>(e.u);
      break;
    case OwnedRespExpr::Type::STRING:
      os << "'" << ToSV(get<OwnedRespExpr::Buffer>(e.u)) << "'";
      break;
    case OwnedRespExpr::Type::NIL:
      os << "nil";
      break;
    case OwnedRespExpr::Type::NIL_ARRAY:
      os << "[]";
      break;
    case OwnedRespExpr::Type::ARRAY:
      os << dfly::TakeRespSpan{std::get<OwnedRespExpr::Vec>(e.u)};
      break;
    case OwnedRespExpr::Type::ERROR:
      os << "e(" << ToSV(get<OwnedRespExpr::Buffer>(e.u)) << ")";
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
