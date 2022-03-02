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
