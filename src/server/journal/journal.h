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

  void StartInThread();

  // Requires: journal is in lameduck mode.
  std::error_code Close();

#if 0
  // Opens journal inside a Dragonfly thread. Must be called in each thread.
  std::error_code OpenInThread(bool persistent, std::string_view dir);
#endif

  //******* The following functions must be called in the context of the owning shard *********//

  uint32_t RegisterOnChange(ChangeCallback cb);
  void UnregisterOnChange(uint32_t id);
  bool HasRegisteredCallbacks() const;

  bool IsLSNInBuffer(LSN lsn) const;
  std::string_view GetEntry(LSN lsn) const;

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

  void RecordEntry(TxId txid, Op opcode, DbIndex dbid, unsigned shard_cnt, Entry::Payload payload,
                   bool await);

 private:
  mutable Mutex state_mu_;

  std::atomic_bool lameduck_{false};
};

}  // namespace journal
}  // namespace dfly
