// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/journal/journal.h"

#include <filesystem>

#include "base/logging.h"
#include "server/engine_shard_set.h"
#include "server/journal/journal_shard.h"
#include "server/server_state.h"

namespace dfly {
namespace journal {

namespace fs = std::filesystem;
using namespace std;
using namespace util;
namespace fibers = boost::fibers;

namespace {

thread_local JournalShard journal_shard;

}  // namespace

Journal::Journal() {
}

error_code Journal::StartLogging(std::string_view dir) {
  if (journal_shard.IsOpen()) {
    return error_code{};
  }

  auto* pool = shard_set->pool();
  atomic_uint32_t created{0};
  lock_guard lk(state_mu_);

  auto open_cb = [&](auto* pb) {
    auto ec = journal_shard.Open(dir, unsigned(ProactorBase::GetIndex()));
    if (ec) {
      LOG(FATAL) << "Could not create journal " << ec;  // TODO
    } else {
      created.fetch_add(1, memory_order_relaxed);
      ServerState::tlocal()->set_journal(this);
      EngineShard* shard = EngineShard::tlocal();
      if (shard) {
        shard->set_journal(this);
      }
    }
  };

  pool->AwaitFiberOnAll(open_cb);

  if (created.load(memory_order_acquire) != pool->size()) {
    LOG(FATAL) << "TBD / revert";
  }

  return error_code{};
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

    auto ec = journal_shard.Close();

    if (ec) {
      lock_guard lk2(ec_mu);
      res = ec;
    }
  };

  shard_set->pool()->AwaitFiberOnAll(close_cb);

  return res;
}

bool Journal::SchedStartTx(TxId txid, unsigned num_keys, unsigned num_shards) {
  if (!journal_shard.IsOpen() || lameduck_.load(memory_order_relaxed))
    return false;

  journal_shard.AddLogRecord(txid, unsigned(Op::SCHED));

  return true;
}

LSN Journal::GetLsn() const {
  return journal_shard.cur_lsn();
}

bool Journal::EnterLameDuck() {
  if (!journal_shard.IsOpen()) {
    return false;
  }

  bool val = false;
  bool res = lameduck_.compare_exchange_strong(val, true, memory_order_acq_rel);
  return res;
}

void Journal::OpArgs(TxId txid, Op opcode, Span keys) {
  DCHECK(journal_shard.IsOpen());

  journal_shard.AddLogRecord(txid, unsigned(opcode));
}

void Journal::RecordEntry(TxId txid, const PrimeKey& key, const PrimeValue& pval) {
  journal_shard.AddLogRecord(txid, unsigned(Op::VAL));
}

}  // namespace journal
}  // namespace dfly
