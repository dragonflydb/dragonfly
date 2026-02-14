// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/journal/journal.h"

#include <filesystem>

#include "base/logging.h"
#include "server/engine_shard_set.h"
#include "server/journal/journal_slice.h"
#include "server/server_state.h"

namespace dfly {
namespace journal {

using namespace std;
using namespace util;

namespace {

// Present in all threads (not only in shard threads).
thread_local JournalSlice journal_slice;

}  // namespace

Journal::Journal() {
}

void Journal::StartInThread() {
  journal_slice.Init();

  ServerState::tlocal()->set_journal(this);
  EngineShard* shard = EngineShard::tlocal();
  if (shard) {
    shard->set_journal(this);
  }
}

void Journal::StartInThreadAtLsn(LSN lsn) {
  StartInThread();
  journal_slice.ResetRingBuffer();
  journal_slice.SetStartingLSN(lsn);
}

error_code Journal::Close() {
  VLOG(1) << "Journal::Close";

  auto close_cb = [&](unsigned, auto*) {
    journal_slice.ResetRingBuffer();
    ServerState::tlocal()->set_journal(nullptr);
    EngineShard* shard = EngineShard::tlocal();
    if (shard) {
      shard->set_journal(nullptr);
    }
  };

  shard_set->pool()->AwaitBrief(close_cb);

  return {};
}

bool Journal::HasRegisteredCallbacks() const {
  return journal_slice.HasRegisteredCallbacks();
}

bool Journal::IsLSNInBuffer(LSN lsn) const {
  return journal_slice.IsLSNInBuffer(lsn);
}

std::string_view Journal::GetEntry(LSN lsn) const {
  return journal_slice.GetEntry(lsn);
}

size_t Journal::LsnBufferSize() const {
  return journal_slice.GetRingBufferSize();
}

size_t Journal::LsnBufferBytes() const {
  return journal_slice.GetRingBufferBytes();
}

uint32_t RegisterConsumer(JournalConsumerInterface* consumer) {
  return journal_slice.RegisterOnChange(consumer);
}

void UnregisterConsumer(uint32_t id) {
  journal_slice.UnregisterOnChange(id);
}

LSN GetLsn() {
  return journal_slice.cur_lsn();
}

void RecordEntry(TxId txid, Op opcode, DbIndex dbid, unsigned shard_cnt, std::optional<SlotId> slot,
                 Entry::Payload payload) {
  journal_slice.AddLogRecord(Entry{txid, opcode, dbid, shard_cnt, slot, std::move(payload)});
}

void SetFlushMode(bool allow_flush) {
  journal_slice.SetFlushMode(allow_flush);
}

size_t thread_local DisableFlushGuard::counter_ = 0;

}  // namespace journal
}  // namespace dfly
