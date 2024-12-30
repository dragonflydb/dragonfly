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
    case journal::Op::FIN:
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
  if (command.cmd_args.empty()) {
    return false;
  }

  auto& args = command.cmd_args;
  if (absl::EqualsIgnoreCase(ToSV(args[0]), "FLUSHDB"sv) ||
      absl::EqualsIgnoreCase(ToSV(args[0]), "FLUSHALL"sv) ||
      (absl::EqualsIgnoreCase(ToSV(args[0]), "DFLYCLUSTER"sv) &&
       absl::EqualsIgnoreCase(ToSV(args[1]), "FLUSHSLOTS"sv))) {
    return true;
  }

  return false;
}

TransactionData TransactionData::FromEntry(journal::ParsedEntry&& entry) {
  TransactionData data;
  data.AddEntry(std::move(entry));
  return data;
}

std::optional<TransactionData> TransactionReader::NextTxData(JournalReader* reader, Context* cntx) {
  io::Result<journal::ParsedEntry> res;
  if (res = reader->ReadEntry(); !res) {
    cntx->ReportError(res.error());
    return std::nullopt;
  }

  // When LSN opcode is sent master does not increase journal lsn.
  if (lsn_.has_value() && res->opcode != journal::Op::LSN) {
    ++*lsn_;
    VLOG(2) << "read lsn: " << *lsn_;
  }

  TransactionData tx_data = TransactionData::FromEntry(std::move(res.value()));
  if (lsn_.has_value() && tx_data.opcode == journal::Op::LSN) {
    DCHECK_NE(tx_data.lsn, 0u);
    LOG_IF_EVERY_N(WARNING, tx_data.lsn != *lsn_, 10000)
        << "master lsn:" << tx_data.lsn << " replica lsn" << *lsn_;
    DCHECK_EQ(tx_data.lsn, *lsn_);
  }
  return tx_data;
}

}  // namespace dfly
