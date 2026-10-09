// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/journal/journal_applier.h"

#include "base/logging.h"

namespace dfly {

JournalApplier::JournalApplier(Service* service,
                               std::shared_ptr<MultiShardExecution> multi_shard_exe,
                               ExecuteHook execute_hook)
    : executor_(service),
      multi_shard_exe_(std::move(multi_shard_exe)),
      execute_hook_(std::move(execute_hook)) {
}

bool JournalApplier::Execute(TransactionData&& tx_data) {
  // Run the hook first so a crash during execution still leaves the record logged.
  if (execute_hook_)
    execute_hook_(tx_data);
  return executor_.Execute(tx_data.dbid, tx_data.command) == facade::DispatchResult::OK;
}

bool JournalApplier::Apply(TransactionData&& tx_data, ExecutionState* cntx) {
  if (!cntx->IsRunning()) {
    return false;
  }

  if (!tx_data.IsGlobalCmd()) {
    VLOG(3) << "Execute cmd without sync between shards. txid: " << tx_data.txid;
    return Execute(std::move(tx_data));
  }

  TxId txid = tx_data.txid;
  VLOG(2) << "Execute txid: " << txid << " waiting for data in all shards";
  return multi_shard_exe_->Execute(txid, [&] { return Execute(std::move(tx_data)); }) &&
         cntx->IsRunning();
}

}  // namespace dfly
