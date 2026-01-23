#include "server/cmd_support.h"

namespace dfly::cmd {

bool AsyncContextBase::InitAndPrepare(AsyncContextBase* async_cntx, ArgSlice args,
                                      CommandContext* cmd_cntx) {
  async_cntx->Init(cmd_cntx);

  auto result = async_cntx->Prepare(args, cmd_cntx);
  if (std::holds_alternative<facade::ErrorReply>(result)) {
    cmd_cntx->SendError(std::get<facade::ErrorReply>(result));
    return false;
  }
  return true;
}

void AsyncContextBase::RunSync(AsyncContextBase* async_cntx, ArgSlice args,
                               CommandContext* cmd_cntx) {
  if (!InitAndPrepare(async_cntx, args, cmd_cntx))
    return;

  DCHECK(cmd_cntx->tx()->Blocker()->IsCompleted());  // Must be ready
  async_cntx->Reply(cmd_cntx->rb());
}

void AsyncContextBase::RunAsync(std::unique_ptr<AsyncContextBase> async_cntx, ArgSlice args,
                                CommandContext* cmd_cntx) {
  if (!InitAndPrepare(&*async_cntx, args, cmd_cntx))
    return;

  auto replier = [async_cntx = std::move(async_cntx)](facade::SinkReplyBuilder* rb) {
    async_cntx->Reply(rb);
  };
  cmd_cntx->Resolve(cmd_cntx->tx()->Blocker(), std::move(replier));
}

AsyncContextBase::BlockerSentinel AsyncContextBase::SingleHop(Transaction::RunnableType cb) {
  if (cntx_->IsDeferredReply()) {
    DCHECK(!tx_keepalive_) << "Only a single hop is allowed";
    cntx_->tx()->SingleHopAsync(cb);
    tx_keepalive_ = cntx_->tx();
  } else {
    cntx_->tx()->ScheduleSingleHop(cb);
  }
  return BlockerSentinel{};
}

}  // namespace dfly::cmd
