// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/error_types.h"

#include <absl/strings/str_cat.h>

#include <system_error>

#include "base/logging.h"

namespace dfly {

using namespace std;

GenericError::operator std::error_code() const {
  return ec_;
}

GenericError::operator bool() const {
  return bool(ec_) || !details_.empty();
}

std::string GenericError::Format() const {
  if (!ec_ && details_.empty())
    return "";

  if (details_.empty())
    return ec_.message();
  else if (!ec_)
    return details_;
  else
    return absl::StrCat(ec_.message(), ": ", details_);
}

ExecutionState::~ExecutionState() {
  DCHECK(!err_handler_fb_.IsJoinable());
  err_handler_fb_.JoinIfNeeded();
}

GenericError ExecutionState::GetError() const {
  std::lock_guard lk(err_mu_);
  return err_;
}

void ExecutionState::ReportCancelError() {
  ReportError(std::make_error_code(errc::operation_canceled), "ExecutionState cancelled");
}

void ExecutionState::Reset(ErrHandler handler) {
  util::fb2::Fiber fb;

  unique_lock lk{err_mu_};
  err_ = {};
  err_handler_ = std::move(handler);
  state_.store(State::RUN, std::memory_order_relaxed);
  fb.swap(err_handler_fb_);
  lk.unlock();
  fb.JoinIfNeeded();
}

GenericError ExecutionState::SwitchErrorHandler(ErrHandler handler) {
  std::lock_guard lk{err_mu_};
  if (!err_) {
    // No need to check for the error handler - it can't be running
    // if no error is set.
    err_handler_ = std::move(handler);
  }
  return err_;
}

void ExecutionState::JoinErrorHandler() {
  util::fb2::Fiber fb;
  unique_lock lk{err_mu_};
  fb.swap(err_handler_fb_);
  lk.unlock();
  fb.JoinIfNeeded();
}

GenericError ExecutionState::ReportErrorInternal(GenericError&& err) {
  if (IsCancelled()) {
    LOG_IF(INFO, err != errc::operation_canceled) << err.Format();
    return {};
  }
  lock_guard lk{err_mu_};
  if (err_)
    return err_;

  err_ = std::move(err);

  // This context is either new or was Reset, where the handler was joined
  CHECK(!err_handler_fb_.IsJoinable());

  LOG(WARNING) << "ReportError: " << err_.Format();

  // We can move err_handler_ because it should run at most once.
  if (err_handler_)
    err_handler_fb_ = util::fb2::Fiber("report_internal_error", std::move(err_handler_), err_);
  state_.store(State::ERROR, std::memory_order_relaxed);
  return err_;
}

}  // namespace dfly
