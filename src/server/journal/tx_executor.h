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

// Coordinates global commands across flows applied in parallel (replica flows, AOF shards).
// Each global command is a rendezvous: every flow waits for the others, exactly one executes it,
// and none continues before it finishes. Global transactions run in the same order on every
// shard, so the flows meet them in the same order, one round after another. The participant
// count is not fixed: a flow whose log ended leaves for good (RemoveFlow), which fixed-count
// barriers cannot express.
class MultiShardExecution {
 public:
  explicit MultiShardExecution(uint32_t num_flows);

  // Blocks until all flows reach txid, runs apply in exactly one of them, then releases them all.
  // apply runs under mu_: the others cannot pass before it finished anyway.
  // Returns false if cancelled; else apply's result in the flow that ran it, true in the others.
  bool Execute(TxId txid, absl::FunctionRef<bool()> apply);

  // Called when a flow reaches the end of its log. In AOF replay a shard's log can end, cleanly
  // or at a torn tail, before a global command that the other shards still reach; that flow never
  // arrives, so without this the others would wait forever. It lowers the participant count, so
  // the flow counts as arrived at the current and every future round; one call is enough. A flow
  // reads its log in order, so it calls this only after leaving every round it was in.
  void RemoveFlow();

  // Wakes every waiting flow; Execute() then returns false. Used when sync or replay fails.
  void CancelAllBlockingEntities();

 private:
  util::fb2::Mutex mu_;
  util::fb2::CondVar cv_;
  uint32_t flows_;
  // Flows that arrived at the current round, and its txid.
  uint32_t arrived_ = 0;
  TxId round_txid_ = 0;
  // Bumped when a round's command finished, which releases its flows.
  uint64_t generation_ = 0;
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
