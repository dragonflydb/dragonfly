// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <atomic>
#include <utility>

namespace dfly {

// Switches from IDLE state to any of the non-idle states.
// Switched from non-idle to IDLE.
// Refuses switching from non-idle to another non-idle state directly.
//
class GlobalState {
 public:
  enum S : uint8_t {
    IDLE,
    LOADING,
    SAVING,
    SHUTTING_DOWN,
  };

  GlobalState(S s = IDLE) : s_(s) {
  }

  const char* Name() const {
    return Name(s_.load(std::memory_order_relaxed));
  }

  static const char* Name(S s);

  std::pair<S, bool> Next(S s) {
    S current{IDLE};
    bool res = s_.compare_exchange_strong(current, s, std::memory_order_acq_rel);
    return std::make_pair(current, res);
  }

  // Switches to IDLE and returns the previous state.
  S Clear() {
    return s_.exchange(IDLE, std::memory_order_acq_rel);
  }

  // Returns the current state.
  S Load() const {
    return s_.load(std::memory_order_acq_rel);
  }

 private:
  std::atomic<S> s_;
};

}  // namespace dfly