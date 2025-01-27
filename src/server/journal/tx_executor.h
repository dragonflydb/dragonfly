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
    util::fb2::Barrier barrier;
    std::atomic_uint32_t counter;
    util::fb2::BlockingCounter block;

    explicit TxExecutionSync(uint32_t counter)
        : barrier(counter), counter(counter), block(counter) {
    }
  };

  bool InsertTxToSharedMap(TxId txid, uint32_t shard_cnt);
  TxExecutionSync& Find(TxId txid);
  void Erase(TxId txid);
  void CancelAllBlockingEntities();

 private:
  util::fb2::Mutex map_mu;
  std::unordered_map<TxId, TxExecutionSync> tx_sync_execution;
};

// This class holds the commands of transaction in single shard.
// Once all commands were received, the transaction can be executed.
struct TransactionData {
  // Update the data from ParsedEntry
  void AddEntry(journal::ParsedEntry&& entry);

  bool IsGlobalCmd() const;

  static TransactionData FromEntry(journal::ParsedEntry&& entry);

  TxId txid{0};
  DbIndex dbid{0};
  journal::ParsedEntry::CmdData command;

  journal::Op opcode = journal::Op::NOOP;
  uint64_t lsn = 0;
};

// Utility for reading TransactionData from a journal reader.
// The journal stream can contain interleaved data for multiple multi transactions,
// expiries and out of order executed transactions that need to be grouped on the replica side.
struct TransactionReader {
  TransactionReader(std::optional<uint64_t> lsn = std::nullopt) : lsn_(lsn) {
  }
  std::optional<TransactionData> NextTxData(JournalReader* reader, Context* cntx);

 private:
  std::optional<uint64_t> lsn_ = 0;
};

}  // namespace dfly
