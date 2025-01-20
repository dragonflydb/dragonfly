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

namespace fs = std::filesystem;
using namespace std;
using namespace util;

namespace {

// Present in all threads (not only in shard threads).
thread_local JournalSlice journal_slice;

}  // namespace

Journal::Journal() {
}

void Journal::StartInThread() {
  journal_slice.Init(unsigned(ProactorBase::me()->GetPoolIndex()));

  ServerState::tlocal()->set_journal(this);
  EngineShard* shard = EngineShard::tlocal();
  if (shard) {
    shard->set_journal(this);
  }
}

error_code Journal::Close() {
  VLOG(1) << "Journal::Close";

  if (!journal_slice.IsOpen()) {
    return {};
  }

  lock_guard lk(state_mu_);
  auto close_cb = [&](auto*) {
    ServerState::tlocal()->set_journal(nullptr);
    EngineShard* shard = EngineShard::tlocal();
    if (shard) {
      shard->set_journal(nullptr);
    }
  };

  shard_set->pool()->AwaitFiberOnAll(close_cb);

  return {};
}

uint32_t Journal::RegisterOnChange(ChangeCallback cb) {
  return journal_slice.RegisterOnChange(cb);
}

void Journal::UnregisterOnChange(uint32_t id) {
  journal_slice.UnregisterOnChange(id);
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

LSN Journal::GetLsn() const {
  return journal_slice.cur_lsn();
}

void Journal::RecordEntry(TxId txid, Op opcode, DbIndex dbid, unsigned shard_cnt,
                          std::optional<SlotId> slot, Entry::Payload payload) {
  journal_slice.AddLogRecord(Entry{txid, opcode, dbid, shard_cnt, slot, std::move(payload)});
}

void Journal::SetFlushMode(bool allow_flush) {
  journal_slice.SetFlushMode(allow_flush);
}

}  // namespace journal
}  // namespace dfly
