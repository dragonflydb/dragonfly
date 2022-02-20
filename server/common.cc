// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <absl/strings/str_cat.h>

#include "base/logging.h"
#include "server/common_types.h"
#include "server/error.h"
#include "server/server_state.h"

namespace dfly {

using std::string;

thread_local ServerState ServerState::state_;

ServerState::ServerState() {
}

ServerState::~ServerState() {
}

void ServerState::Init() {
  gstate_ = GlobalState::IDLE;
}

void ServerState::Shutdown() {
  gstate_ = GlobalState::SHUTTING_DOWN;
  interpreter_.reset();
}

Interpreter& ServerState::GetInterpreter() {
  if (!interpreter_) {
    interpreter_.emplace();
  }

  return interpreter_.value();
}

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

const char* GlobalState::Name(S s) {
  switch (s) {
    case GlobalState::IDLE:
      return "IDLE";
    case GlobalState::LOADING:
      return "LOADING";
    case GlobalState::SAVING:
      return "SAVING";
    case GlobalState::SHUTTING_DOWN:
      return "SHUTTING DOWN";
  }
  ABSL_INTERNAL_UNREACHABLE;
}

}  // namespace dfly

namespace std {

ostream& operator<<(ostream& os, dfly::CmdArgList ras) {
  os << "[";
  if (!ras.empty()) {
    for (size_t i = 0; i < ras.size() - 1; ++i) {
      os << dfly::ArgS(ras, i) << ",";
    }
    os << dfly::ArgS(ras, ras.size() - 1);
  }
  os << "]";

  return os;
}

}  // namespace std
