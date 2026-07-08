// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cmd_support.h"

#include <absl/cleanup/cleanup.h>

#include <new>

#include "base/logging.h"

namespace dfly::cmd {

Transaction::RunnableResult SingleHopWaiter::operator()(Transaction* tx, EngineShard* es) const {
  try {
    auto res = callback(tx, es);
    status_ = res.status;
    return res;
  } catch (const std::bad_alloc&) {
    // RunCallback maps this to OUT_OF_MEMORY; mirror it so the snapshot matches.
    status_ = facade::OpStatus::OUT_OF_MEMORY;
    throw;
  }
}

bool SingleHopWaiter::await_ready() noexcept {
  auto* tx = cmd_cntx->tx();

  if (!cmd_cntx->IsDeferredReply()) {
    // Use fiber blocking in synchronous mode
    tx->ScheduleSingleHop(callback);
    return true;
  }

  // Async hop. Only single-shard commands can be pipeline-squashed onto a reused
  // transaction, so snapshot their status during the hop. Multi-shard hops run
  // callbacks on several shards concurrently, so writing status_ from them would
  // race; they instead read the transaction's aggregated result, which their own
  // (non-reused) transaction preserves.
  use_snapshot_ = tx->GetUniqueShardCnt() == 1;
  if (use_snapshot_)
    tx->SingleHopAsync(*this);
  else
    tx->SingleHopAsync(callback);
  tx_keepalive_ = tx;
  return false;
}

void SingleHopWaiter::await_suspend(std::coroutine_handle<> handle) const noexcept {
  cmd_cntx->Resolve(tx_keepalive_->Blocker(), handle);
}

facade::OpStatus SingleHopWaiter::await_resume() const noexcept {
  return use_snapshot_ ? status_ : *cmd_cntx->tx()->LocalResultPtr();
}

void CmdR::Coro::return_value(const facade::ErrorReply& err) const noexcept {
  cmd_cntx->SendError(err);
}

}  // namespace dfly::cmd
