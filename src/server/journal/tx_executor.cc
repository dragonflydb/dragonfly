// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "tx_executor.h"

#include <absl/strings/match.h>

#include "base/logging.h"
#include "server/journal/serializer.h"

using namespace std;
using namespace facade;

namespace dfly {

bool MultiShardExecution::InsertTxToSharedMap(TxId txid, uint32_t shard_cnt) {
  std::unique_lock lk(map_mu);
  auto [it, was_insert] = tx_sync_execution.emplace(txid, shard_cnt);
  lk.unlock();

  VLOG(2) << "txid: " << txid << " unique_shard_cnt_: " << shard_cnt
          << " was_insert: " << was_insert;
  it->second.block->Dec();

  return was_insert;
}

MultiShardExecution::TxExecutionSync& MultiShardExecution::Find(TxId txid) {
  std::lock_guard lk(map_mu);
  VLOG(2) << "Execute txid: " << txid;
  auto it = tx_sync_execution.find(txid);
  DCHECK(it != tx_sync_execution.end());
  return it->second;
}

void MultiShardExecution::Erase(TxId txid) {
  std::lock_guard lg{map_mu};
  tx_sync_execution.erase(txid);
}

void MultiShardExecution::CancelAllBlockingEntities() {
  lock_guard lk{map_mu};
  for (auto& tx_data : tx_sync_execution) {
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
    LOG_IF_EVERY_N(WARNING, dest->lsn != *lsn_, 10000)
        << "master lsn:" << dest->lsn << " replica lsn" << *lsn_;
    DCHECK_EQ(dest->lsn, *lsn_);
  }
  return true;
}

}  // namespace dfly
