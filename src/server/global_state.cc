// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/global_state.h"

#include "absl/base/optimization.h"
#include "base/logging.h"

namespace dfly {

const char* GlobalStateName(GlobalState s) {
  switch (s) {
    case GlobalState::ACTIVE:
      return "ACTIVE";
    case GlobalState::LOADING:
      return "LOADING";
    case GlobalState::SHUTTING_DOWN:
      return "SHUTTING DOWN";
    case GlobalState::TAKEN_OVER:
      return "TAKEN OVER";
  }
  ABSL_UNREACHABLE();
}

std::ostream& operator<<(std::ostream& os, const GlobalState& state) {
  return os << GlobalStateName(state);
}

}  // namespace dfly
