// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <absl/functional/function_ref.h>

#include <unordered_map>

#include "server/execution_state.h"
#include "server/journal/types.h"
#include "util/fibers/synchronization.h"

namespace dfly {

struct JournalReader;

// Coordinates global commands across flows applied in parallel (replica flows, AOF shards).
class MultiShardExecution {
 public:
  explicit MultiShardExecution(uint32_t num_flows);

  // Blocks until all flows reach txid, runs apply in exactly one of them, then releases them all.
  // Returns false if cancelled; else apply's result in the flow that ran it, true in the others.
  bool Execute(TxId txid, absl::FunctionRef<bool()> apply);

  // Called when a flow reaches end of log: it counts as arrived at every pending and future txid.
  void RemoveFlow();

  void CancelAllBlockingEntities();

 private:
  struct Barrier {
    enum class State : uint8_t { kWaiting, kRunning, kDone };

    uint32_t arrived = 0;
    uint32_t left = 0;
    State state = State::kWaiting;
  };

  util::fb2::Mutex mu_;
  util::fb2::CondVar cv_;
  std::unordered_map<TxId, Barrier> barriers_;
  uint32_t flows_;
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
