// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "tx_executor.h"

#include <absl/strings/match.h>

#include "base/logging.h"
#include "server/execution_state.h"
#include "server/journal/serializer.h"

using namespace std;
using namespace facade;

namespace dfly {

// Runs the wait expression and returns false if woken up by cancellation.
#define WAIT_OR_RETURN(wait_expr) \
  do {                            \
    wait_expr;                    \
    if (!cntx->IsRunning())       \
      return false;               \
  } while (0)

MultiShardExecution::MultiShardExecution(uint32_t num_flows) : num_flows_(num_flows) {
}

bool MultiShardExecution::Execute(TxId txid, ExecutionState* cntx,
                                  absl::FunctionRef<bool()> apply) {
  bool inserted_by_me = InsertTxToSharedMap(txid);

  auto& multi_shard_data = Find(txid);

  VLOG(2) << "Execute txid: " << txid << " waiting for data in all shards";
  // Wait until shards flows got transaction data and inserted to map.
  // This step enforces that replica will execute multi shard commands that finished on master
  // and replica received all the commands from all shards.
  WAIT_OR_RETURN(multi_shard_data.block->Wait());

  VLOG(2) << "Execute txid: " << txid << " global command execution";
  // Wait until all shards flows get to execution step of this transaction.
  WAIT_OR_RETURN(multi_shard_data.barrier.Wait());
  // Global command will be executed only from one flow fiber. This ensure corectness of data in
  // replica.
  bool execution_res = true;
  if (inserted_by_me) {
    execution_res = apply();
  }
  // Wait until exection is done, to make sure we done execute next commands while the global is
  // executed.
  WAIT_OR_RETURN(multi_shard_data.barrier.Wait());

  // Erase from map can be done only after all flow fibers executed the transaction commands.
  // The last fiber which will decrease the counter to 0 will be the one to erase the data from
  // map
  auto val = multi_shard_data.counter.fetch_sub(1, std::memory_order_relaxed);
  VLOG(2) << "txid: " << txid << " counter: " << val;
  if (val == 1) {
    Erase(txid);
  }
  return execution_res;
}

#undef WAIT_OR_RETURN

bool MultiShardExecution::InsertTxToSharedMap(TxId txid) {
  std::unique_lock lk(map_mu_);
  auto [it, was_insert] = tx_sync_execution_.emplace(txid, num_flows_);
  // If CancelAllBlockingEntities() already ran, cancel the new entry immediately
  // to prevent shard fibers from blocking on entries that will never complete.
  if (cancelled_) {
    it->second.barrier.Cancel();
    it->second.block->Cancel();
  }
  lk.unlock();

  VLOG(2) << "txid: " << txid << " unique_shard_cnt_: " << num_flows_
          << " was_insert: " << was_insert;
  it->second.block->Dec();

  return was_insert;
}

MultiShardExecution::TxExecutionSync& MultiShardExecution::Find(TxId txid) {
  std::lock_guard lk(map_mu_);
  VLOG(2) << "Execute txid: " << txid;
  auto it = tx_sync_execution_.find(txid);
  DCHECK(it != tx_sync_execution_.end());
  return it->second;
}

void MultiShardExecution::Erase(TxId txid) {
  std::lock_guard lg{map_mu_};
  tx_sync_execution_.erase(txid);
}

void MultiShardExecution::CancelAllBlockingEntities() {
  lock_guard lk{map_mu_};
  cancelled_ = true;
  for (auto& tx_data : tx_sync_execution_) {
    tx_data.second.barrier.Cancel();
    tx_data.second.block->Cancel();
  }
}

void TransactionData::AddEntry(journal::ParsedEntry&& entry) {
  opcode = entry.opcode;

  switch (entry.opcode) {
    case journal::Op::LSN:
      lsn = entry.lsn;
      return;
    case journal::Op::PING:
      return;
    case journal::Op::EXPIRED:
    case journal::Op::COMMAND:
      command = std::move(entry.cmd);
      dbid = entry.dbid;
      txid = entry.txid;
      return;
    default:
      DCHECK(false) << "Unsupported opcode";
  }
}

bool TransactionData::IsGlobalCmd() const {
  if (command.empty()) {
    return false;
  }

  string_view front = command.Front();

  if (absl::EqualsIgnoreCase(front, "FLUSHDB"sv) || absl::EqualsIgnoreCase(front, "FLUSHALL"sv))
    return true;

  if (command.size() > 1 && absl::EqualsIgnoreCase(front, "DFLYCLUSTER"sv) &&
      absl::EqualsIgnoreCase(command[1], "FLUSHSLOTS"sv)) {
    return true;
  }

  return false;
}

bool TransactionReader::NextTxData(JournalReader* reader, ExecutionState* cntx,
                                   TransactionData* dest) {
  if (!cntx->IsRunning()) {
    return false;
  }
  journal::ParsedEntry entry;
  if (auto ec = reader->ReadEntry(&entry); ec) {
    cntx->ReportError(ec);
    return false;
  }

  // When LSN opcode is sent master does not increase journal lsn.
  if (lsn_.has_value() && entry.opcode != journal::Op::LSN) {
    ++*lsn_;
    VLOG(2) << "read lsn: " << *lsn_;
  }

  dest->command.clear();
  dest->AddEntry(std::move(entry));

  if (lsn_.has_value() && dest->opcode == journal::Op::LSN) {
    DCHECK_NE(dest->lsn, 0u);
    if (dest->lsn != *lsn_) {
      LOG_EVERY_T(WARNING, 2) << "master lsn:" << dest->lsn << " replica lsn" << *lsn_;
    }
    DCHECK_EQ(dest->lsn, *lsn_);
  }
  return true;
}

}  // namespace dfly
