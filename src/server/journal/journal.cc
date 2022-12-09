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
namespace fibers = boost::fibers;

namespace {

// Present in all threads (not only in shard threads).
thread_local JournalSlice journal_slice;

}  // namespace

Journal::Journal() {
}

error_code Journal::OpenInThread(bool persistent, string_view dir) {
  journal_slice.Init(unsigned(ProactorBase::GetIndex()));

  error_code ec;

  if (persistent) {
    ec = journal_slice.Open(dir);
    if (ec) {
      return ec;
    }
  }

  ServerState::tlocal()->set_journal(this);
  EngineShard* shard = EngineShard::tlocal();
  if (shard) {
    shard->set_journal(this);
  }

  return ec;
}

error_code Journal::Close() {
  CHECK(lameduck_.load(memory_order_relaxed));

  VLOG(1) << "Journal::Close";

  fibers::mutex ec_mu;
  error_code res;

  lock_guard lk(state_mu_);
  auto close_cb = [&](auto*) {
    ServerState::tlocal()->set_journal(nullptr);
    EngineShard* shard = EngineShard::tlocal();
    if (shard) {
      shard->set_journal(nullptr);
    }

    auto ec = journal_slice.Close();

    if (ec) {
      lock_guard lk2(ec_mu);
      res = ec;
    }
  };

  shard_set->pool()->AwaitFiberOnAll(close_cb);

  return res;
}

uint32_t Journal::RegisterOnChange(ChangeCallback cb) {
  return journal_slice.RegisterOnChange(cb);
}

void Journal::Unregister(uint32_t id) {
  journal_slice.Unregister(id);
}

bool Journal::SchedStartTx(TxId txid, unsigned num_keys, unsigned num_shards) {
  if (!journal_slice.IsOpen() || lameduck_.load(memory_order_relaxed))
    return false;

  // TODO: to complete the metadata.
  // journal_slice.AddLogRecord(Entry::Sched(txid));

  return true;
}

LSN Journal::GetLsn() const {
  return journal_slice.cur_lsn();
}

bool Journal::EnterLameDuck() {
  if (!journal_slice.IsOpen()) {
    return false;
  }

  bool val = false;
  bool res = lameduck_.compare_exchange_strong(val, true, memory_order_acq_rel);
  return res;
}

void Journal::Record(TxId txid, DbIndex dbid, Entry::Payload payload) {
  journal_slice.AddLogRecord(journal::Entry{txid, dbid, payload});
}

/*
void Journal::OpArgs(TxId txid, Op opcode, Span keys) {
  DCHECK(journal_slice.IsOpen());

  journal_slice.AddLogRecord(txid, opcode);
}
*/

}  // namespace journal
}  // namespace dfly
