// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "server/journal/types.h"
#include "util/proactor_pool.h"

namespace dfly {

class Transaction;

namespace journal {

class Journal {
 public:
  using Span = absl::Span<const std::string_view>;

  Journal();

  // Returns true if journal has been active and changed its state to lameduck mode
  // and false otherwise.
  bool EnterLameDuck();  // still logs ongoing transactions but refuses to start new ones.

  // Requires: journal is in lameduck mode.
  std::error_code Close();

  // Opens journal inside a Dragonfly thread. Must be called in each thread.
  std::error_code OpenInThread(bool persistent, std::string_view dir);

  //******* The following functions must be called in the context of the owning shard *********//

  uint32_t RegisterOnChange(ChangeCallback cb);
  void Unregister(uint32_t id);

  // Returns true if transaction was scheduled, false if journal is inactive
  // or in lameduck mode and does not log new transactions.
  bool SchedStartTx(TxId txid, unsigned num_keys, unsigned num_shards);

  /*
  void AddCmd(TxId txid, Op opcode, Span args) {
    OpArgs(txid, opcode, args);
  }

  void Lock(TxId txid, Span keys) {
    OpArgs(txid, Op::LOCK, keys);
  }

  void Unlock(TxId txid, Span keys) {
    OpArgs(txid, Op::UNLOCK, keys);
  }
*/
  LSN GetLsn() const;

  void Record(TxId txid, DbIndex dbid, Entry::Payload payload);

 private:
  mutable boost::fibers::mutex state_mu_;

  std::atomic_bool lameduck_{false};
};

}  // namespace journal
}  // namespace dfly
