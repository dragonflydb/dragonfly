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
  it->second.block.Dec();

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
    tx_data.second.block.Cancel();
  }
}

bool TransactionData::AddEntry(journal::ParsedEntry&& entry) {
  ++journal_rec_count;
  opcode = entry.opcode;

  switch (entry.opcode) {
    case journal::Op::PING:
      return true;
    case journal::Op::FIN:
      return true;
    case journal::Op::EXPIRED:
    case journal::Op::COMMAND:
      commands.push_back(std::move(entry.cmd));
      [[fallthrough]];
    case journal::Op::EXEC:
      shard_cnt = entry.shard_cnt;
      dbid = entry.dbid;
      txid = entry.txid;
      return true;
    case journal::Op::MULTI_COMMAND:
      commands.push_back(std::move(entry.cmd));
      dbid = entry.dbid;
      return false;
    default:
      DCHECK(false) << "Unsupported opcode";
  }
  return false;
}

bool TransactionData::IsGlobalCmd() const {
  if (commands.size() > 1) {
    return false;
  }

  auto& command = commands.front();
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

TransactionData TransactionData::FromSingle(journal::ParsedEntry&& entry) {
  TransactionData data;
  bool res = data.AddEntry(std::move(entry));
  DCHECK(res);
  return data;
}

std::optional<TransactionData> TransactionReader::NextTxData(JournalReader* reader, Context* cntx) {
  io::Result<journal::ParsedEntry> res;
  while (true) {
    if (res = reader->ReadEntry(); !res) {
      cntx->ReportError(res.error());
      return std::nullopt;
    }

    // Check if journal command can be executed right away.
    // Expiration checks lock on master, so it never conflicts with running multi transactions.
    if (res->opcode == journal::Op::EXPIRED || res->opcode == journal::Op::COMMAND ||
        res->opcode == journal::Op::PING || res->opcode == journal::Op::FIN)
      return TransactionData::FromSingle(std::move(res.value()));

    // Otherwise, continue building multi command.
    DCHECK(res->opcode == journal::Op::MULTI_COMMAND || res->opcode == journal::Op::EXEC);
    DCHECK(res->txid > 0 || res->shard_cnt == 1);

    auto txid = res->txid;
    auto& txdata = current_[txid];
    if (txdata.AddEntry(std::move(res.value()))) {
      auto out = std::move(txdata);
      current_.erase(txid);
      return out;
    }
  }

  return std::nullopt;
}

}  // namespace dfly
