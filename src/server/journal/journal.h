// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once
#include "server/journal/types.h"
#include "util/fibers/detail/fiber_interface.h"
#include "util/proactor_pool.h"

namespace dfly {

class Transaction;

namespace journal {

class Journal {
 public:
  using Span = absl::Span<const std::string_view>;

  Journal();

  void StartInThread();

  std::error_code Close();

  //******* The following functions must be called in the context of the owning shard *********//

  uint32_t RegisterOnChange(ChangeCallback cb);
  void UnregisterOnChange(uint32_t id);
  bool HasRegisteredCallbacks() const;

  bool IsLSNInBuffer(LSN lsn) const;
  std::string_view GetEntry(LSN lsn) const;

  LSN GetLsn() const;

  void RecordEntry(TxId txid, Op opcode, DbIndex dbid, unsigned shard_cnt,
                   std::optional<SlotId> slot, Entry::Payload payload);

  void SetFlushMode(bool allow_flush);

 private:
  mutable util::fb2::Mutex state_mu_;
};

class JournalFlushGuard {
 public:
  explicit JournalFlushGuard(Journal* journal) : journal_(journal) {
    if (journal_) {
      journal_->SetFlushMode(false);
    }
    util::fb2::detail::EnterFiberAtomicSection();
  }

  ~JournalFlushGuard() {
    util::fb2::detail::LeaveFiberAtomicSection();
    if (journal_) {
      journal_->SetFlushMode(true);  // Restore the state on destruction
    }
  }

  JournalFlushGuard(const JournalFlushGuard&) = delete;
  JournalFlushGuard& operator=(const JournalFlushGuard&) = delete;

 private:
  Journal* journal_;
};

}  // namespace journal
}  // namespace dfly
