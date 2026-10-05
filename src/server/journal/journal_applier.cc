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

bool JournalApplier::Apply(TransactionData&& tx_data, ExecutionState* cntx) {
  if (!cntx->IsRunning()) {
    return false;
  }

  auto execute = [&] {
    // Traffic logger hook: gate is inside LogReplicaCommand, so the no-op path
    // (logger disabled) is cheap. Log before Execute so a crash during execute
    // still leaves the record on disk for post-mortem replay.
    if (execute_hook_)
      execute_hook_(tx_data);
    return executor_.Execute(tx_data.dbid, tx_data.command) == facade::DispatchResult::OK;
  };

  if (!tx_data.IsGlobalCmd()) {
    VLOG(3) << "Execute cmd without sync between shards. txid: " << tx_data.txid;
    return execute();
  }

  VLOG(2) << "Execute txid: " << tx_data.txid << " waiting for all flows";
  return multi_shard_exe_->Execute(tx_data.txid, execute) && cntx->IsRunning();
}

}  // namespace dfly
