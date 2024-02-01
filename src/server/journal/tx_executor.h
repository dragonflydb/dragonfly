// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <unordered_map>

#include "server/common.h"
#include "server/journal/types.h"

namespace dfly {

struct JournalReader;

// Coordinator for multi shard execution.
class MultiShardExecution {
 public:
  struct TxExecutionSync {
    Barrier barrier;
    std::atomic_uint32_t counter;
    BlockingCounter block;

    explicit TxExecutionSync(uint32_t counter)
        : barrier(counter), counter(counter), block(counter) {
    }
  };

  bool InsertTxToSharedMap(TxId txid, uint32_t shard_cnt);
  TxExecutionSync& Find(TxId txid);
  void Erase(TxId txid);
  void CancelAllBlockingEntities();

 private:
  Mutex map_mu;
  std::unordered_map<TxId, TxExecutionSync> tx_sync_execution;
};

// This class holds the commands of transaction in single shard.
// Once all commands were received, the transaction can be executed.
struct TransactionData {
  // Update the data from ParsedEntry and return true if all shard transaction commands were
  // received.
  bool AddEntry(journal::ParsedEntry&& entry);

  bool IsGlobalCmd() const;

  static TransactionData FromSingle(journal::ParsedEntry&& entry);

  TxId txid{0};
  DbIndex dbid{0};
  uint32_t shard_cnt{0};
  absl::InlinedVector<journal::ParsedEntry::CmdData, 1> commands{0};
  uint32_t journal_rec_count{0};  // Count number of source entries to check offset.
  journal::Op opcode = journal::Op::NOOP;
};

// Utility for reading TransactionData from a journal reader.
// The journal stream can contain interleaved data for multiple multi transactions,
// expiries and out of order executed transactions that need to be grouped on the replica side.
struct TransactionReader {
  std::optional<TransactionData> NextTxData(JournalReader* reader, Context* cntx);

 private:
  // Stores ongoing multi transaction data.
  absl::flat_hash_map<TxId, TransactionData> current_;
};

}  // namespace dfly
