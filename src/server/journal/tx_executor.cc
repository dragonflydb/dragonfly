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

MultiShardExecution::MultiShardExecution(uint32_t num_flows) : flows_(num_flows) {
}

bool MultiShardExecution::Execute(TxId txid, absl::FunctionRef<bool()> apply) {
  std::unique_lock lk(mu_);
  if (cancelled_)
    return false;
  uint64_t generation = generation_;
  if (arrived_++ == 0)
    round_txid_ = txid;
  // The flows meet the global commands in the same order.
  DCHECK_EQ(round_txid_, txid);
  VLOG(2) << "txid: " << txid << " arrived: " << arrived_ << " flows: " << flows_;

  // Wait until the round is complete, or another flow ran it.
  cv_.wait(lk, [&] { return cancelled_ || generation_ != generation || arrived_ >= flows_; });
  if (cancelled_)
    return false;
  if (generation_ != generation)
    return true;

  // The first flow to see the round complete runs it. Under the lock, so the others cannot pass
  // before it finished.
  bool res = apply();
  arrived_ = 0;
  ++generation_;
  cv_.notify_all();
  return res;
}

void MultiShardExecution::RemoveFlow() {
  std::lock_guard lk(mu_);
  DCHECK_GT(flows_, 0u);
  --flows_;
  // Wakes the flows waiting for this one, so they re-check against the lower count.
  cv_.notify_all();
}

void MultiShardExecution::CancelAllBlockingEntities() {
  std::lock_guard lk(mu_);
  cancelled_ = true;
  cv_.notify_all();
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

  // Global transactions journaled on every shard. MOVE is journaled once, so it is not here.
  static constexpr string_view kGlobalCmds[] = {"FLUSHDB",  "FLUSHALL",     "FT.CREATE",
                                                "FT.ALTER", "FT.DROPINDEX", "FT.SYNUPDATE"};
  string_view front = command.Front();
  for (string_view cmd : kGlobalCmds) {
    if (absl::EqualsIgnoreCase(front, cmd))
      return true;
  }

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
