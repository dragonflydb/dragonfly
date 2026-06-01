// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cmd_support.h"

#include <absl/cleanup/cleanup.h>

#include <stdexcept>

#include "base/logging.h"
#include "facade/error.h"

namespace dfly::cmd {

bool SingleHopWaiter::await_ready() {
  auto* tx = cmd_cntx->tx();

  if (!cmd_cntx->IsDeferredReply()) {
    // Use fiber blocking in synchronous mode
    tx->ScheduleSingleHop(callback);
    return true;
  } else {
    // Schedule async hop and keep transaction alive
    tx->SingleHopAsync(callback);
    tx_keepalive_ = tx;
    return false;
  }
}

void SingleHopWaiter::await_suspend(std::coroutine_handle<> handle) const noexcept {
  cmd_cntx->Resolve(tx_keepalive_->Blocker(), handle);
}

facade::OpStatus SingleHopWaiter::await_resume() const noexcept {
  return *cmd_cntx->tx()->LocalResultPtr();
}

void CmdR::Coro::return_value(const facade::ErrorReply& err) const noexcept {
  cmd_cntx->SendError(err);
}

void CmdR::Coro::unhandled_exception() const noexcept {
  try {
    throw;
  } catch (const facade::CancellationException&) {
    cmd_cntx->SendError("Cancelled");
  } catch (const std::exception& e) {
    LOG(ERROR) << "Unhandled exception in command coroutine: " << e.what();
  }
}

}  // namespace dfly::cmd
