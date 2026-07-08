// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cmd_support.h"

#include <absl/cleanup/cleanup.h>

#include "base/logging.h"

namespace dfly::cmd {

Transaction::RunnableResult SingleHopWaiter::operator()(Transaction* tx, EngineShard* es) const {
  auto res = callback(tx, es);
  status_ = res.status;
  return res;
}

bool SingleHopWaiter::await_ready() noexcept {
  auto* tx = cmd_cntx->tx();

  if (!cmd_cntx->IsDeferredReply()) {
    // Use fiber blocking in synchronous mode
    tx->ScheduleSingleHop(*this);
    return true;
  } else {
    // Schedule async hop and keep transaction alive
    tx->SingleHopAsync(*this);
    tx_keepalive_ = tx;
    return false;
  }
}

void SingleHopWaiter::await_suspend(std::coroutine_handle<> handle) const noexcept {
  cmd_cntx->Resolve(tx_keepalive_->Blocker(), handle);
}

facade::OpStatus SingleHopWaiter::await_resume() const noexcept {
  return status_;
}

void CmdR::Coro::return_value(const facade::ErrorReply& err) const noexcept {
  cmd_cntx->SendError(err);
}

}  // namespace dfly::cmd
