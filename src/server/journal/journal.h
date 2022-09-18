// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "server/common.h"
#include "server/table.h"
#include "util/proactor_pool.h"

namespace dfly {

class Transaction;

namespace journal {

enum class Op : uint8_t {
  NOOP = 0,
  LOCK = 1,
  UNLOCK = 2,
  LOCK_SHARD = 3,
  UNLOCK_SHARD = 4,
  SCHED = 5,
  VAL = 10,
  DEL,
  MSET,
};

class Journal {
 public:
  using Span = absl::Span<const std::string_view>;

  Journal();

  std::error_code StartLogging(std::string_view dir);

  // Returns true if journal has been active and changed its state to lameduck mode
  // and false otherwise.
  bool EnterLameDuck();  // still logs ongoing transactions but refuses to start new ones.

  // Requires: journal is in lameduck mode.
  std::error_code Close();

  // Returns true if transaction was scheduled, false if journal is inactive
  // or in lameduck mode and does not log new transactions.
  bool SchedStartTx(TxId txid, unsigned num_keys, unsigned num_shards);

  void AddCmd(TxId txid, Op opcode, Span args) {
    OpArgs(txid, opcode, args);
  }

  void Lock(TxId txid, Span keys) {
    OpArgs(txid, Op::LOCK, keys);
  }

  void Unlock(TxId txid, Span keys) {
    OpArgs(txid, Op::UNLOCK, keys);
  }

  LSN GetLsn() const;

  void RecordEntry(TxId txid, const PrimeKey& key, const PrimeValue& pval);

 private:
  void OpArgs(TxId id, Op opcode, Span keys);

  mutable boost::fibers::mutex state_mu_;

  std::atomic_bool lameduck_{false};
};

}  // namespace journal
}  // namespace dfly
