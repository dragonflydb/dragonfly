// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <atomic>
#include <functional>
#include <mutex>
#include <string>
#include <system_error>

#include "facade/facade_types.h"
#include "util/fibers/fibers.h"
#include "util/fibers/synchronization.h"

namespace dfly {

// AggregateValue is a thread safe utility to store the first
// truthy value;
template <typename T> struct AggregateValue {
  bool operator=(T val) {
    std::lock_guard l{mu_};
    if (!bool(current_) && bool(val)) {
      current_ = val;
    }
    return bool(val);
  }

  T operator*() {
    std::lock_guard l{mu_};
    return current_;
  }

  operator bool() {
    return bool(**this);
  }

  // Move out of value without critical section. Safe only when no longer in use.
  T Destroy() && {
    return std::move(current_);
  }

 private:
  util::fb2::Mutex mu_{};
  T current_{};
};

// Thread safe utility to store the first non null error.
using AggregateError = AggregateValue<std::error_code>;

// Thread safe utility to store the first non OK status.
using AggregateStatus = AggregateValue<facade::OpStatus>;
static_assert(bool(facade::OpStatus::OK) == false,
              "Default initialization should be a falsy OK value");

// Error wrapper, that stores error_code and optional string message.
class GenericError {
 public:
  GenericError() = default;
  GenericError(std::error_code ec) : ec_{ec}, details_{} {
  }
  GenericError(std::string details) : ec_{}, details_{std::move(details)} {
  }
  GenericError(std::error_code ec, std::string details) : ec_{ec}, details_{std::move(details)} {
  }

  operator std::error_code() const;
  operator bool() const;

  std::string Format() const;  // Get string representation of error.

 private:
  std::error_code ec_;
  std::string details_;
};

// Thread safe utility to store the first non null generic error.
using AggregateGenericError = AggregateValue<GenericError>;

// ExecutionState is a thread-safe utility for managing error reporting and cancellation for complex
// tasks. There are 3 states: RUN, CANCELLED, ERROR RUN and CANCELLED are just a state without any
// actions When report an error, only the first is stored, the next ones will be ignored. Then a
// special error handler is run, if present, and the ExecutionState is ERROR. The error handler is
// run in a separate handler to free up the caller.
// If the state is CANCELLED all errors are ignored
//
// ReportCancelError() reporting an `errc::operation_canceled` error.
class ExecutionState {
 public:
  using ErrHandler = std::function<void(const GenericError&)>;

  ExecutionState() = default;
  ExecutionState(ErrHandler err_handler) : err_handler_{std::move(err_handler)} {
  }

  ~ExecutionState();

  // TODO Remove. This function was created to reduce size of the code that should be refactored
  // Cancel() method should be used instead of this function
  // Report a cancel error the context by submitting an `errc::operation_canceled` error.
  // If the state is CANCELLED does nothing
  void ReportCancelError();

  bool IsRunning() const {
    return state_.load(std::memory_order_relaxed) == State::RUN;
  }

  bool IsError() const {
    return state_.load(std::memory_order_relaxed) == State::ERROR;
  }

  bool IsCancelled() const {
    return state_.load(std::memory_order_relaxed) == State::CANCELLED;
  }

  void Cancel() {
    state_.store(State::CANCELLED, std::memory_order_relaxed);
  }

  GenericError GetError() const;

  // Report an error by submitting arguments for GenericError.
  // If this is the first error that occured, then the error handler is run
  // and the context state set to ERROR.
  // If the state is CANCELLED does nothing
  template <typename... T> GenericError ReportError(T... ts) {
    return ReportErrorInternal(GenericError{std::forward<T>(ts)...});
  }

  // Wait for error handler to stop, reset error and state, assign new error handler.
  void Reset(ErrHandler handler);

  // Atomically replace the error handler if no error is present, and return the
  // current stored error. This function can be used to transfer cleanup responsibility safely
  //
  // Beware, never do this manually in two steps. If you check the state,
  // set the error handler and initialize resources, then the new error handler
  // will never run if the context was cancelled between the first two steps.
  GenericError SwitchErrorHandler(ErrHandler handler);

  // If any error handler is running, wait for it to stop.
  void JoinErrorHandler();

 private:
  GenericError ReportErrorInternal(GenericError&& err);

  enum class State { RUN, CANCELLED, ERROR };
  std::atomic<State> state_{State::RUN};
  GenericError err_;
  ErrHandler err_handler_;
  util::fb2::Fiber err_handler_fb_;

  // We use regular mutexes to be able to call ReportError directly from I/O callbacks.
  mutable std::mutex err_mu_;  // protects err_ and err_handler_
};

}  // namespace dfly
