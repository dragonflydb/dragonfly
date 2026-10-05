// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/journal/journal_applier.h"

#include "base/logging.h"

namespace dfly {

JournalApplier::JournalApplier(Service* service,
                               std::shared_ptr<MultiShardExecution> multi_shard_exe,
                               uint32_t num_flows, ExecuteHook execute_hook)
    : executor_(service),
      multi_shard_exe_(std::move(multi_shard_exe)),
      num_flows_(num_flows),
      execute_hook_(std::move(execute_hook)) {
}

bool JournalApplier::Apply(TransactionData&& tx_data, ExecutionState* cntx) {
  if (!cntx->IsRunning()) {
    return false;
  }

  if (!tx_data.IsGlobalCmd()) {
    VLOG(3) << "Execute cmd without sync between shards. txid: " << tx_data.txid;
    // Traffic logger hook: gate is inside LogReplicaCommand, so the no-op path
    // (logger disabled) is cheap. Log before Execute so a crash during execute
    // still leaves the record on disk for post-mortem replay.
    if (execute_hook_)
      execute_hook_(tx_data);
    return executor_.Execute(tx_data.dbid, tx_data.command) == facade::DispatchResult::OK;
  }

  bool inserted_by_me = multi_shard_exe_->InsertTxToSharedMap(tx_data.txid, num_flows_);

  auto& multi_shard_data = multi_shard_exe_->Find(tx_data.txid);

  VLOG(2) << "Execute txid: " << tx_data.txid << " waiting for data in all shards";
  // Wait until shards flows got transaction data and inserted to map.
  // This step enforces that replica will execute multi shard commands that finished on master
  // and replica received all the commands from all shards.
  multi_shard_data.block->Wait();
  // Check if we woke up due to cancellation.
  if (!cntx->IsRunning())
    return false;
  VLOG(2) << "Execute txid: " << tx_data.txid << " block wait finished";

  VLOG(2) << "Execute txid: " << tx_data.txid << " global command execution";
  // Wait until all shards flows get to execution step of this transaction.
  multi_shard_data.barrier.Wait();
  // Check if we woke up due to cancellation.
  if (!cntx->IsRunning())
    return false;
  // Global command will be executed only from one flow fiber. This ensure corectness of data in
  // replica.
  bool execution_res = true;
  if (inserted_by_me) {
    // Global command — log exactly once (only the inserter flow runs Execute,
    // so this guard naturally dedups across per-shard flows).
    if (execute_hook_)
      execute_hook_(tx_data);
    execution_res = executor_.Execute(tx_data.dbid, tx_data.command) == facade::DispatchResult::OK;
  }
  // Wait until exection is done, to make sure we done execute next commands while the global is
  // executed.
  multi_shard_data.barrier.Wait();
  // Check if we woke up due to cancellation.
  if (!cntx->IsRunning())
    return false;

  // Erase from map can be done only after all flow fibers executed the transaction commands.
  // The last fiber which will decrease the counter to 0 will be the one to erase the data from
  // map
  auto val = multi_shard_data.counter.fetch_sub(1, std::memory_order_relaxed);
  VLOG(2) << "txid: " << tx_data.txid << " counter: " << val;
  if (val == 1) {
    multi_shard_exe_->Erase(tx_data.txid);
  }
  return execution_res;
}

}  // namespace dfly
