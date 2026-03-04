// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cmd_support.h"

#include "base/logging.h"

namespace dfly::cmd {

#define RETURN_ON_ERR(result)                                         \
  if (std::holds_alternative<facade::ErrorReply>(result)) {           \
    return cmd_cntx->SendError(std::get<facade::ErrorReply>(result)); \
  }

void AsyncContextInterface::RunSync(AsyncContextInterface* async_cntx, ArgSlice args,
                                    CommandContext* cmd_cntx) {
  auto result = async_cntx->Prepare(args, cmd_cntx);
  RETURN_ON_ERR(result);

  DCHECK(std::holds_alternative<JustReplySentinel>(result) ||
         std::get<BlockResult>(result) == nullptr);  // Nothing to await
  async_cntx->Reply(cmd_cntx->rb());
}

void AsyncContextInterface::RunAsync(std::unique_ptr<AsyncContextInterface> async_cntx,
                                     ArgSlice args, CommandContext* cmd_cntx) {
  auto result = async_cntx->Prepare(args, cmd_cntx);
  RETURN_ON_ERR(result);

  auto replier = [me = std::move(async_cntx)](facade::SinkReplyBuilder* rb) { me->Reply(rb); };

  if (std::holds_alternative<BlockResult>(result)) {
    auto* blocker = std::get<BlockResult>(result);
    DCHECK(blocker);
    cmd_cntx->Resolve(blocker, std::move(replier));
  } else {
    DCHECK(std::holds_alternative<JustReplySentinel>(result));
    // TODO: use nullptr blocker or captures once ReplyWith was removed
    cmd_cntx->ReplyWith(std::move(replier));
  }
}

BlockResult HopCoordinator::SingleHop(CommandContext* cmd_cntx, Transaction::RunnableType cb) {
  if (!cmd_cntx->IsDeferredReply()) {
    cmd_cntx->tx()->ScheduleSingleHop(cb);
    return static_cast<util::fb2::EmbeddedBlockingCounter*>(nullptr);
  }

  // Keep transaction alive
  DCHECK(!tx_keepalive_) << "Only a single hop is allowed";
  tx_keepalive_ = cmd_cntx->tx();

  // Schedule single hop and return blocker
  tx_keepalive_->SingleHopAsync(cb);
  return tx_keepalive_->Blocker();
}

bool SingleHopWaiter::await_ready() noexcept {
  return (blocker = SingleHop(cmd_cntx, callback)) == nullptr;
}

void SingleHopWaiter::await_suspend(std::coroutine_handle<> handle) const noexcept {
  // TODO: functor calling resume is double indirection
  cmd_cntx->Resolve(blocker, [handle](auto* rb) { handle.resume(); });
}

facade::OpStatus SingleHopWaiter::await_resume() const noexcept {
  return *cmd_cntx->tx()->LocalResultPtr();
}

void CmdR::Coro::return_value(facade::ErrorReply&& err) const noexcept {
  cmd_cntx->SendError(err);
}

}  // namespace dfly::cmd
