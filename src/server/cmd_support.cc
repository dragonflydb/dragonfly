#include "server/cmd_support.h"

namespace dfly::cmd {

AsyncContextBase::TxBlockSentiel AsyncContextBase::SingleHop(Transaction::RunnableType cb) {
  if (cntx_->IsDeferredReply()) {
    DCHECK(!tx_keepalive_) << "Only a single hop is allowed";
    cntx_->tx()->SingleHopAsync(cb);
    tx_keepalive_ = cntx_->tx();
  } else {
    cntx_->tx()->ScheduleSingleHop(cb);
  }
  return TxBlockSentiel{};
}

}  // namespace dfly::cmd
