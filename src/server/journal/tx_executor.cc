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

#define RETURN_ON_CANCELED()  \
  do {                        \
    if (cancelled_)           \
      return Status::kFailed; \
  } while (0)

bool MultiShardExecution::JoinRound(TxId txid) {
  if (arrived_ == 0) {
    round_txid_ = txid;
  } else if (round_txid_ != txid) {
    LOG(DFATAL) << "Global commands out of order across flows: " << round_txid_ << " vs " << txid;
    cancelled_ = true;
    cv_.notify_all();
    return false;
  }
  ++arrived_;
  return true;
}

// One round per global command, all under mu_:
//   arrive:  join the round; a different txid cancels the rendezvous (kOutOfOrder).
//   gather:  wait until all flows arrived.
//   run:     the flow that sees the round complete runs apply; the others stay blocked.
//   release: arrived_ = 0, ++generation_; every flow returns the round's result.
// A cancel fails a round only before it ran; a finished round's result stands.
MultiShardExecution::Status MultiShardExecution::Execute(TxId txid,
                                                         absl::FunctionRef<bool()> apply) {
  std::unique_lock lk(mu_);
  RETURN_ON_CANCELED();
  if (!JoinRound(txid))
    return Status::kOutOfOrder;
  uint64_t my_round = generation_;
  VLOG(2) << "Execute txid: " << txid << " waiting for data in all shards";

  // Wait until the round is complete, or another flow ran it.
  cv_.wait(lk, [&] { return cancelled_ || generation_ != my_round || arrived_ >= flows_; });
  // Once the round finished, every flow returns its result, even if cancelled since.
  if (generation_ != my_round)
    return global_cmd_status_;
  RETURN_ON_CANCELED();

  // The first flow to see the round complete runs it. Under the lock, so the others cannot pass
  // before it finished.
  global_cmd_status_ = apply() ? Status::kOk : Status::kFailed;
  arrived_ = 0;
  ++generation_;
  cv_.notify_all();
  return global_cmd_status_;
}

#undef RETURN_ON_CANCELED

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
