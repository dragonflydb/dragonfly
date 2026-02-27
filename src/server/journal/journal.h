// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once
#include "server/journal/types.h"
#include "util/fibers/detail/fiber_interface.h"

namespace dfly {

namespace journal {

void StartInThread();

// Starts the journal at specified LSN
// Also drops the (resets) the partial sync buffers
void StartInThreadAtLsn(LSN lsn);

std::error_code Close();

//******* The following functions must be called in the context of the owning shard *********//

bool HasRegisteredCallbacks();

bool IsLSNInBuffer(LSN lsn);

std::string_view GetEntry(LSN lsn);

LSN GetLsn();
uint32_t RegisterConsumer(JournalConsumerInterface* consumer);
void UnregisterConsumer(uint32_t id);

void RecordEntry(TxId txid, Op opcode, DbIndex dbid, unsigned shard_cnt, std::optional<SlotId> slot,
                 Entry::Payload payload);

size_t LsnBufferSize();
size_t LsnBufferBytes();

// Resets the partial sync ring buffer. Must be called in the context of the owning shard.
void ResetBuffer();

void SetFlushMode(bool allow_flush);

class DisableFlushGuard {
 public:
  explicit DisableFlushGuard(bool j) : journal_(j) {
    if (journal_ && counter_ == 0) {
      SetFlushMode(false);
    }
    util::fb2::detail::EnterFiberAtomicSection();
    ++counter_;
  }

  ~DisableFlushGuard() {
    util::fb2::detail::LeaveFiberAtomicSection();
    --counter_;
    if (journal_ && counter_ == 0) {
      SetFlushMode(true);  // Restore the state on destruction
    }
  }

  DisableFlushGuard(const DisableFlushGuard&) = delete;
  DisableFlushGuard& operator=(const DisableFlushGuard&) = delete;

 private:
  bool journal_;
  static size_t thread_local counter_;
};

}  // namespace journal
}  // namespace dfly
