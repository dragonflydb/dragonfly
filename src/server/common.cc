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

bool ParseHumanReadableBytes(std::string_view str, int64_t* num_bytes) {
  if (str.empty())
    return false;

  const char* cstr = str.data();
  bool neg = (*cstr == '-');
  if (neg) {
    cstr++;
  }
  char* end;
  double d = strtod(cstr, &end);

  // If this didn't consume the entire string, fail.
  if (end + 1 < str.end())
    return false;

  int64 scale = 1;
  switch (*end) {
    // NB: an int64 can only go up to <8 EB.
    case 'E':
      scale <<= 10;  // Fall through...
    case 'P':
      scale <<= 10;
    case 'T':
      scale <<= 10;
    case 'G':
      scale <<= 10;
    case 'M':
      scale <<= 10;
    case 'K':
    case 'k':
      scale <<= 10;
    case 'B':
    case '\0':
      break;  // To here.
    default:
      return false;
  }
  d *= scale;
  if (d > kint64max || d < 0)
    return false;
  *num_bytes = static_cast<int64>(d + 0.5);
  if (neg) {
    *num_bytes = -*num_bytes;
  }
  return true;
}

}  // namespace dfly
