// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <absl/strings/str_cat.h>

#include "base/logging.h"

#include "facade/conn_context.h"
#include "facade/dragonfly_connection.h"
#include "facade/error.h"
#include "facade/facade_types.h"

namespace facade {

using namespace std;

#define ADD(x) (x) += o.x

ConnectionStats& ConnectionStats::operator+=(const ConnectionStats& o) {
  // To break this code deliberately if we add/remove a field to this struct.
  static_assert(sizeof(ConnectionStats) == 64);

  ADD(num_conns);
  ADD(num_replicas);
  ADD(read_buf_capacity);
  ADD(io_read_cnt);
  ADD(io_read_bytes);
  ADD(io_write_cnt);
  ADD(io_write_bytes);
  ADD(pipelined_cmd_cnt);
  ADD(command_cnt);

  return *this;
}

#undef ADD

string WrongNumArgsError(std::string_view cmd) {
  return absl::StrCat("wrong number of arguments for '", cmd, "' command");
}

const char kSyntaxErr[] = "syntax error";
const char kWrongTypeErr[] = "-WRONGTYPE Operation against a key holding the wrong kind of value";
const char kKeyNotFoundErr[] = "no such key";
const char kInvalidIntErr[] = "value is not an integer or out of range";
const char kUintErr[] = "value is out of range, must be positive";
const char kDbIndOutOfRangeErr[] = "DB index is out of range";
const char kInvalidDbIndErr[] = "invalid DB index";
const char kScriptNotFound[] = "-NOSCRIPT No matching script. Please use EVAL.";
const char kAuthRejected[] = "-WRONGPASS invalid username-password pair or user is disabled.";

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

ConnectionContext::ConnectionContext(::io::Sink* stream, Connection* owner) : owner_(owner) {
  switch (owner->protocol()) {
    case Protocol::REDIS:
      rbuilder_.reset(new RedisReplyBuilder(stream));
      break;
    case Protocol::MEMCACHE:
      rbuilder_.reset(new MCReplyBuilder(stream));
      break;
  }

  async_dispatch = false;
  conn_closing = false;
  req_auth = false;
  replica_conn = false;
  authenticated = false;
}

Protocol ConnectionContext::protocol() const {
  return owner_->protocol();
}

RedisReplyBuilder* ConnectionContext::operator->() {
  CHECK(Protocol::REDIS == protocol());

  return static_cast<RedisReplyBuilder*>(rbuilder_.get());
}

}  // namespace facade


namespace std {

ostream& operator<<(ostream& os, facade::CmdArgList ras) {
  os << "[";
  if (!ras.empty()) {
    for (size_t i = 0; i < ras.size() - 1; ++i) {
      os << facade::ArgS(ras, i) << ",";
    }
    os << facade::ArgS(ras, ras.size() - 1);
  }
  os << "]";

  return os;
}

ostream& operator<<(ostream& os, const facade::RespExpr& e) {
  using facade::RespExpr;
  using facade::ToSV;

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
      os << facade::RespSpan{*get<RespExpr::Vec*>(e.u)};
      break;
    case RespExpr::ERROR:
      os << "e(" << ToSV(get<RespExpr::Buffer>(e.u)) << ")";
      break;
  }

  return os;
}

ostream& operator<<(ostream& os, facade::RespSpan ras) {
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
