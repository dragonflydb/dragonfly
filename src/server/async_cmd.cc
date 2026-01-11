#include "server/async_cmd.h"

#include "server/transaction.h"

namespace dfly::cmd {

namespace detail {

facade::SinkReplyBuilder* ReplyEventWaiter::await_resume() const noexcept {
  return cmnd_cntx->rb();
}

bool LastHopWaiter::await_ready() const noexcept {
  cmnd_cntx->tx->Execute(callback, true, true);

  if (cmnd_cntx->IsDeferredReply())
    return false;

  // Sync execution
  cmnd_cntx->tx->Blocker()->Wait();
  return true;
}

void LastHopWaiter::await_suspend(std::coroutine_handle<> handle) const noexcept {
  cmnd_cntx->Resolve(cmnd_cntx->tx->Blocker(), handle);
}

facade::OpStatus LastHopWaiter::await_resume() const noexcept {
  return cmnd_cntx->tx->GetLocalResult();
}

}  // namespace detail

void Task::Promise::return_value(facade::ErrorReply&& err) noexcept {
  cmnd_cntx->SendError(err);
}
}  // namespace dfly::cmd
