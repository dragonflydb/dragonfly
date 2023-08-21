// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <absl/strings/escaping.h>
#include <absl/strings/str_cat.h>

#include "base/logging.h"
#include "facade/command_id.h"
#include "facade/conn_context.h"
#include "facade/dragonfly_connection.h"
#include "facade/error.h"

namespace facade {

using namespace std;

#define ADD(x) (x) += o.x
#define ADD_M(m)                  \
  do {                            \
    for (const auto& k_v : o.m) { \
      m[k_v.first] += k_v.second; \
    }                             \
  } while (0)

constexpr size_t kSizeConnStats = sizeof(ConnectionStats);

ConnectionStats& ConnectionStats::operator+=(const ConnectionStats& o) {
  // To break this code deliberately if we add/remove a field to this struct.
  static_assert(kSizeConnStats == 136u);

  ADD(read_buf_capacity);
  ADD(pipeline_cache_capacity);
  ADD(io_read_cnt);
  ADD(io_read_bytes);
  ADD(io_write_cnt);
  ADD(io_write_bytes);
  ADD(command_cnt);
  ADD(pipelined_cmd_cnt);
  ADD(async_writes_cnt);
  ADD(conn_received_cnt);
  ADD(num_conns);
  ADD(num_replicas);
  ADD(num_blocked_clients);

  ADD_M(err_count_map);

  return *this;
}

#undef ADD

string WrongNumArgsError(string_view cmd) {
  return absl::StrCat("wrong number of arguments for '", absl::AsciiStrToLower(cmd), "' command");
}

string InvalidExpireTime(string_view cmd) {
  return absl::StrCat("invalid expire time in '", absl::AsciiStrToLower(cmd), "' command");
}

string UnknownSubCmd(string_view subcmd, string_view cmd) {
  return absl::StrCat("Unknown subcommand or wrong number of arguments for '", subcmd, "'. Try ",
                      cmd, " HELP.");
}

string ConfigSetFailed(string_view config_name) {
  return absl::StrCat("CONFIG SET failed (possibly related to argument '", config_name, "').");
}

const char kSyntaxErr[] = "syntax error";
const char kWrongTypeErr[] = "-WRONGTYPE Operation against a key holding the wrong kind of value";
const char kKeyNotFoundErr[] = "no such key";
const char kInvalidIntErr[] = "value is not an integer or out of range";
const char kInvalidFloatErr[] = "value is not a valid float";
const char kUintErr[] = "value is out of range, must be positive";
const char kIncrOverflow[] = "increment or decrement would overflow";
const char kDbIndOutOfRangeErr[] = "DB index is out of range";
const char kInvalidDbIndErr[] = "invalid DB index";
const char kScriptNotFound[] = "-NOSCRIPT No matching script. Please use EVAL.";
const char kAuthRejected[] = "-WRONGPASS invalid username-password pair or user is disabled.";
const char kExpiryOutOfRange[] = "expiry is out of range";
const char kSyntaxErrType[] = "syntax_error";
const char kScriptErrType[] = "script_error";
const char kIndexOutOfRange[] = "index out of range";
const char kOutOfMemory[] = "Out of memory";
const char kInvalidNumericResult[] = "result is not a number";
const char kClusterNotConfigured[] = "Cluster is not yet configured";

const char* RespExpr::TypeName(Type t) {
  switch (t) {
    case STRING:
      return "string";
    case INT64:
      return "int";
    case DOUBLE:
      return "double";
    case ARRAY:
      return "array";
    case NIL_ARRAY:
      return "nil-array";
    case NIL:
      return "nil";
    case ERROR:
      return "error";
  }
  ABSL_UNREACHABLE();
}

ConnectionContext::ConnectionContext(::io::Sink* stream, Connection* owner) : owner_(owner) {
  if (owner) {
    protocol_ = owner->protocol();
  }

  if (stream) {
    switch (protocol_) {
      case Protocol::REDIS:
        rbuilder_.reset(new RedisReplyBuilder(stream, this));
        break;
      case Protocol::MEMCACHE:
        rbuilder_.reset(new MCReplyBuilder(stream));
        break;
    }
  }

  conn_closing = false;
  req_auth = false;
  replica_conn = false;
  authenticated = false;
  async_dispatch = false;
  sync_dispatch = false;
  journal_emulated = false;

  subscriptions = 0;
}

RedisReplyBuilder* ConnectionContext::operator->() {
  CHECK(Protocol::REDIS == protocol());

  return static_cast<RedisReplyBuilder*>(rbuilder_.get());
}

CommandId::CommandId(const char* name, uint32_t mask, int8_t arity, int8_t first_key,
                     int8_t last_key, int8_t step, uint32_t acl_categories)
    : name_(name), opt_mask_(mask), arity_(arity), first_key_(first_key), last_key_(last_key),
      step_key_(step), acl_categories_(acl_categories) {
}

uint32_t CommandId::OptCount(uint32_t mask) {
  return absl::popcount(mask);
}

}  // namespace facade

namespace std {

using facade::ArgS;

ostream& operator<<(ostream& os, facade::CmdArgList ras) {
  os << "[";
  if (!ras.empty()) {
    for (size_t i = 0; i < ras.size() - 1; ++i) {
      os << absl::CHexEscape(ArgS(ras, i)) << ",";
    }
    os << absl::CHexEscape(ArgS(ras, ras.size() - 1));
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
    case RespExpr::DOUBLE:
      os << "d" << get<int64_t>(e.u);
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
