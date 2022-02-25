// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "base/logging.h"
#include "core/intent_lock.h"
#include "core/resp_expr.h"

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

const char* IntentLock::ModeName(Mode m) {
  switch (m) {
    case IntentLock::SHARED:
      return "SHARED";
    case IntentLock::EXCLUSIVE:
      return "EXCLUSIVE";
  }
  ABSL_INTERNAL_UNREACHABLE;
}

void IntentLock::VerifyDebug() {
  constexpr uint32_t kMsb = 1ULL << (sizeof(cnt_[0]) * 8 - 1);
  DCHECK_EQ(0u, cnt_[0] & kMsb);
  DCHECK_EQ(0u, cnt_[1] & kMsb);
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
      os << "'" << ToSV(get<RespExpr::Buffer>(e.u)) << "'";
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
      os << "e(" << ToSV(get<RespExpr::Buffer>(e.u)) << ")";
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
