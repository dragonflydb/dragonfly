// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <absl/functional/function_ref.h>

#include "server/execution_state.h"
#include "server/journal/types.h"
#include "util/fibers/synchronization.h"

namespace dfly {

struct JournalReader;

// Coordinator for multi shard execution.
// Coordinates global commands across flows applied in parallel (e.g. replica flows).
// Each global command is a rendezvous: every flow waits for the others, exactly one executes it,
// and none continues before it finishes. Global transactions run in the same order on every
// shard, so the flows meet them in the same order, one round after another.
class MultiShardExecution {
 public:
  enum class Status : uint8_t {
    kOk,
    // The round's command did not succeed, the same in every flow of the round. Either the
    // session was cancelled before it ran (CancelAllBlockingEntities), which is permanent:
    // cancelled_ stays set, so every later Execute fails too. Or apply returned false, which is
    // per round: later rounds still run, like any failed record on the replica.
    kFailed,
    // The flows reached different global commands. The rendezvous is cancelled for good.
    kOutOfOrder,
  };

  explicit MultiShardExecution(uint32_t num_flows);

  // Blocks until all flows reach txid, runs apply in exactly one of them, then releases them all.
  // apply runs under mu_: the others cannot pass before it finished anyway. It must not call back
  // into this object.
  // Once the round's command ran, every flow gets its result, even if cancelled since: each
  // replica flow resumes partial sync from its own count of applied records, so they must agree.
  Status Execute(TxId txid, absl::FunctionRef<bool()> apply);

  // Wakes every waiting flow; Execute() then fails. Used when sync fails.
  void CancelAllBlockingEntities();

 private:
  // Adds the flow to the current round. False if txid is not the round's: the flows got out of
  // order, so it cancels the rendezvous rather than combining different commands in one round.
  bool JoinRound(TxId txid);

  util::fb2::Mutex mu_;
  util::fb2::CondVar cv_;
  const uint32_t flows_;
  // Flows that arrived at the current round, and its txid.
  uint32_t arrived_ = 0;
  TxId round_txid_ = 0;
  // Bumped when a round's command finished, which releases its flows with global_cmd_status_. The
  // next round cannot finish before every flow left this one, so they all read this round's result.
  uint64_t generation_ = 0;
  Status global_cmd_status_ = Status::kFailed;
  bool cancelled_ = false;
};

// This class holds the commands of transaction in single shard.
// Once all commands were received, the transaction can be executed.
struct TransactionData {
  // Update the data from ParsedEntry
  void AddEntry(journal::ParsedEntry&& entry);

  bool IsGlobalCmd() const;

  TxId txid{0};
  DbIndex dbid{0};
  journal::ParsedEntry::CmdData command;

  journal::Op opcode;
  uint64_t lsn = 0;
};

// Utility for reading TransactionData from a journal reader.
// The journal stream can contain interleaved data for multiple multi transactions,
// expiries and out of order executed transactions that need to be grouped on the replica side.
struct TransactionReader {
  TransactionReader(std::optional<uint64_t> lsn = std::nullopt) : lsn_(lsn) {
  }

  bool NextTxData(JournalReader* reader, ExecutionState* cntx, TransactionData* dest);

 private:
  std::optional<uint64_t> lsn_ = 0;
};

}  // namespace dfly
