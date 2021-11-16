// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "server/resp_expr.h"

#include "base/logging.h"

namespace dfly {

const char* RespExpr::TypeName(Type t) {
  switch (t) {
    case STRING:
      return "string";
    case INT64:
      return "int";
    case ARRAY:
      return "array";
    case NIL_ARRAY:
      return "nil-array";
    case NIL:
      return "nil";
    case ERROR:
      return "error";
  }
  ABSL_INTERNAL_UNREACHABLE;
}

}  // namespace dfly

namespace std {

ostream& operator<<(ostream& os, const dfly::RespExpr& e) {
  using dfly::RespExpr;
  using dfly::ToSV;

  switch (e.type) {
    case RespExpr::INT64:
      os << "i" << get<int64_t>(e.u);
      break;
    case RespExpr::STRING:
      os << "'" << ToSV(e.GetBuf()) << "'";
      break;
    case RespExpr::NIL:
      os << "nil";
      break;
    case RespExpr::NIL_ARRAY:
      os << "[]";
      break;
    case RespExpr::ARRAY:
      os << dfly::RespSpan{*get<RespExpr::Vec*>(e.u)};
      break;
    case RespExpr::ERROR:
      os << "e(" << ToSV(e.GetBuf()) << ")";
      break;
  }

  return os;
}

ostream& operator<<(ostream& os, dfly::RespSpan ras) {
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