// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/uni_transaction.h"

#include <absl/strings/str_cat.h>

#include <boost/smart_ptr/intrusive_ptr.hpp>

#include "base/logging.h"
#include "server/blocking_controller.h"
#include "server/command_registry.h"
#include "server/db_slice.h"
#include "server/engine_shard_set.h"
#include "server/journal/journal.h"
#include "server/namespaces.h"
#include "server/server_state.h"

namespace dfly {

using namespace std;
using namespace util;

UniTransaction::UniTransaction(const CommandId* cid) : TransactionBase(cid) {
}

UniTransaction::~UniTransaction() {
}

OpStatus UniTransaction::InitByArgs(Namespace* ns, DbIndex index, CmdArgList args) {
  global_ = false;
  db_index_ = index;
  full_args_ = args;
  local_result_ = OpStatus::OK;

  if (IsScheduled()) {
    DCHECK_EQ(namespace_, ns);
  } else {
    DCHECK(namespace_ == nullptr || namespace_ == ns);
    namespace_ = ns;
  }

  DCHECK_EQ((cid_->opt_mask() & CO::GLOBAL_TRANS), 0u);
  DCHECK_EQ((cid_->opt_mask() & CO::NO_KEY_TRANSACTIONAL), 0u);

  DCHECK(args_slices_.empty());
  DCHECK(kv_fp_.empty());

  OpResult<KeyIndex> key_index = DetermineKeys(cid_, args);
  if (!key_index)
    return key_index.status();

  if ((key_index->end - key_index->start) + int(bool(key_index->bonus)) == 0)
    return OpStatus::OK;

  DCHECK_LT(key_index->start, full_args_.size());

  // Store keys directly — single shard, no need to build shard index
  if (key_index->bonus)
    args_slices_.emplace_back(*key_index->bonus, *key_index->bonus + 1);
  args_slices_.emplace_back(key_index->start, key_index->end);

  for (string_view key : key_index->Range(full_args_))
    kv_fp_.push_back(LockTag(key).Fingerprint());

  unique_shard_cnt_ = 1;
  string_view akey = full_args_[*(*key_index)];
  unique_slot_checker_.Add(akey);
  unique_shard_id_ = Shard(akey, shard_set->size());

  sd_.local_mask |= ACTIVE;

  return OpStatus::OK;
}

ShardArgs UniTransaction::GetShardArgs(ShardId sid) const {
  return ShardArgs{full_args_, absl::MakeSpan(args_slices_)};
}

OpStatus UniTransaction::ScheduleSingleHop(RunnableType cb) {
  Execute(cb, true);
  return local_result_;
}

void UniTransaction::SingleHopAsync(RunnableType cb) {
  CHECK_EQ(coordinator_state_, 0u);

  coordinator_state_ |= COORD_CONCLUDING;
  cb_ptr_ = cb;

  sd_.is_armed.store(true, memory_order_relaxed);

  run_barrier_.Add(1);
  use_count_.fetch_add(1, memory_order_relaxed);

  auto shard_cb = [this] {
    bool success = ScheduleInShard(EngineShard::tlocal(), true);
    CHECK(success);

    if (sd_.local_mask & OPTIMISTIC_EXECUTION) {
      run_barrier_.Dec();
      intrusive_ptr_release(this);
    } else {
      EngineShard::tlocal()->PollExecution("exec_cb", this);
      intrusive_ptr_release(this);
    }
  };

  if (CanRunInlined())
    shard_cb();
  else
    shard_set->Add(unique_shard_id_, shard_cb);
}

void UniTransaction::Execute(RunnableType cb, bool conclude) {
  local_result_ = OpStatus::OK;
  cb_ptr_ = cb;

  coordinator_state_ =
      conclude ? (coordinator_state_ | COORD_CONCLUDING) : (coordinator_state_ & ~COORD_CONCLUDING);

  if ((coordinator_state_ & COORD_SCHED) == 0) {
    ScheduleInternal();
  }

  DispatchHop();
  run_barrier_.Wait();
  cb_ptr_.reset();

  if (coordinator_state_ & COORD_CONCLUDING)
    coordinator_state_ &= ~COORD_SCHED;
}

void UniTransaction::Conclude() {
  if (!IsScheduled())
    return;
  auto cb = [](TransactionBase* t, EngineShard* shard) { return OpStatus::OK; };
  Execute(std::move(cb), true);
}

void UniTransaction::ScheduleInternal() {
  DCHECK_EQ(txid_, 0u);
  DCHECK_EQ(coordinator_state_ & COORD_SCHED, 0);
  DCHECK_EQ(unique_shard_cnt_, 1u);

  bool optimistic_exec = (coordinator_state_ & COORD_CONCLUDING) != 0;

  while (true) {
    run_barrier_.Start(1);

    if (CanRunInlined()) {
      CHECK(ScheduleInShard(EngineShard::tlocal(), optimistic_exec));
      run_barrier_.Dec();
      EngineShard::tlocal()->PollExecution("after_inline", nullptr);
      break;
    }

    shard_set->Add(unique_shard_id_, [this, optimistic_exec] {
      CHECK(ScheduleInShard(EngineShard::tlocal(), optimistic_exec));
      FinishHop();
    });
    run_barrier_.Wait();
    break;
  }

  coordinator_state_ |= COORD_SCHED;
  RecordTxScheduleStats(this);
}

bool UniTransaction::ScheduleInShard(EngineShard* shard, bool execute_optimistic) {
  DCHECK(sd_.local_mask & ACTIVE);
  DCHECK_EQ(sd_.local_mask & KEYLOCK_ACQUIRED, 0);
  sd_.local_mask &= ~(OUT_OF_ORDER | OPTIMISTIC_EXECUTION);

  TxQueue* txq = shard->txq();
  IntentLock::Mode mode = LockMode();
  KeyLockArgs lock_args = GetLockArgs(shard->shard_id());

  if (txid_ > 0 && shard->committed_txid() >= txid_)
    return false;

  auto release_fp_locks = [&]() {
    GetDbSlice(shard->shard_id()).Release(mode, lock_args);
    sd_.local_mask &= ~KEYLOCK_ACQUIRED;
  };

  const bool shard_unlocked = shard->shard_lock()->Check(mode);
  const bool keys_unlocked = GetDbSlice(shard->shard_id()).Acquire(mode, lock_args);
  bool lock_granted = shard_unlocked && keys_unlocked;

  sd_.local_mask |= KEYLOCK_ACQUIRED;
  if (lock_granted) {
    sd_.local_mask |= OUT_OF_ORDER;
  }

  bool immediate_run = execute_optimistic && lock_granted && shard->running_tx() == nullptr;
  if (immediate_run) {
    sd_.local_mask |= OPTIMISTIC_EXECUTION;
    shard->stats().tx_optimistic_total++;

    RunCallback(shard);

    if (coordinator_state_ & COORD_CONCLUDING) {
      release_fp_locks();
      return true;
    }
  }

  if (txid_ == 0) {
    txid_ = op_seq.fetch_add(1, memory_order_relaxed);
    DCHECK_GT(txid_, shard->committed_txid());
  }

  if (!txq->Empty() && txid_ < txq->TailScore() && !lock_granted) {
    if (sd_.local_mask & KEYLOCK_ACQUIRED) {
      release_fp_locks();
    }
    return false;
  }

  TxQueue::Iterator it = txq->Insert(this);
  DCHECK_EQ(TxQueue::kEnd, sd_.pq_pos);
  sd_.pq_pos = it;

  AnalyzeTxQueue(shard, txq);

  return true;
}

void UniTransaction::DispatchHop() {
  DCHECK_EQ(unique_shard_cnt_, 1u);
  DCHECK_GT(use_count_.load(memory_order_relaxed), 0u);

  if (sd_.local_mask & OPTIMISTIC_EXECUTION) {
    sd_.local_mask &= ~OPTIMISTIC_EXECUTION;
    return;
  }

  run_barrier_.Start(1);

  std::atomic_thread_fence(memory_order_release);
  sd_.is_armed.store(true, memory_order_relaxed);

  if (CanRunInlined()) {
    EngineShard::tlocal()->PollExecution("exec_cb", this);
    return;
  }

  use_count_.fetch_add(1, memory_order_relaxed);

  shard_set->Add(unique_shard_id_, [this] {
    CHECK(namespace_ != nullptr);
    EngineShard::tlocal()->PollExecution("exec_cb", this);
    intrusive_ptr_release(this);
  });
}

void UniTransaction::RunCallback(EngineShard* shard) {
  DCHECK_EQ(shard, EngineShard::tlocal());
  DCHECK(shard->running_tx() == nullptr);
  shard->set_running_tx(this);

  RunnableResult result;
  try {
    result = (*cb_ptr_)(this, shard);
    cb_ptr_.reset();
    local_result_ = result;
  } catch (std::bad_alloc&) {
    LOG_EVERY_T(ERROR, 1) << " out of memory";
    local_result_ = OpStatus::OUT_OF_MEMORY;
  } catch (std::exception& e) {
    LOG(FATAL) << "Unexpected exception " << e.what();
  }

  auto& db_slice = GetDbSlice(shard->shard_id());
  db_slice.OnCbFinishBlocking();

  if (result.flags & RunnableResult::AVOID_CONCLUDING) {
    coordinator_state_ &= ~COORD_CONCLUDING;
  }

  if (coordinator_state_ & COORD_CONCLUDING) {
    LogAutoJournalOnShard(shard, result);
  }

  shard->set_running_tx(nullptr);
}

bool UniTransaction::RunInShard(EngineShard* shard, bool allow_q_removal) {
  DCHECK_GT(txid_, 0u);
  CHECK(cb_ptr_) << DebugId();

  sd_.stats.total_runs++;

  DCHECK_GT(run_barrier_.DEBUG_Count(), 0u);

  IntentLock::Mode mode = LockMode();
  DCHECK(sd_.local_mask & KEYLOCK_ACQUIRED);

  RunCallback(shard);

  DCHECK_GE(GetUseCount(), 1u);

  bool is_concluding = coordinator_state_ & COORD_CONCLUDING;

  if (sd_.pq_pos != TxQueue::kEnd && (is_concluding || allow_q_removal)) {
    shard->txq()->Remove(sd_.pq_pos);
    sd_.pq_pos = TxQueue::kEnd;
  }

  if (is_concluding) {
    KeyLockArgs largs = GetLockArgs(shard->shard_id());
    DCHECK(sd_.local_mask & KEYLOCK_ACQUIRED);
    GetDbSlice(shard->shard_id()).Release(mode, largs);
    sd_.local_mask &= ~KEYLOCK_ACQUIRED;
    sd_.local_mask &= ~OUT_OF_ORDER;

    shard->RemoveContTx(this);

    if (auto* bcontroller = namespace_->GetBlockingController(shard->shard_id()); bcontroller) {
      if (shard->GetContTx() == nullptr) {
        bcontroller->NotifyPending();
      }
    }
  }

  FinishHop();
  return is_concluding;
}

OpArgs UniTransaction::GetOpArgs(EngineShard* shard) const {
  DCHECK(IsActive(shard->shard_id()));
  DCHECK_GT(run_barrier_.DEBUG_Count(), 0u);
  return OpArgs{shard, this, GetDbContext()};
}

KeyLockArgs UniTransaction::GetLockArgs(ShardId sid) const {
  KeyLockArgs res;
  res.db_index = db_index_;
  res.fps = {kv_fp_.data(), kv_fp_.size()};
  return res;
}

uint16_t UniTransaction::DisarmInShard(ShardId sid) {
  return sd_.is_armed.exchange(false, memory_order_acquire) ? sd_.local_mask : 0;
}

pair<uint16_t, bool> UniTransaction::DisarmInShardWhen(ShardId sid, uint16_t relevant_flags) {
  if (sd_.is_armed.load(memory_order_acquire)) {
    bool relevant = sd_.local_mask & relevant_flags;
    if (relevant)
      CHECK(sd_.is_armed.exchange(false, memory_order_release));
    return {sd_.local_mask, relevant};
  }
  return {0, false};
}

bool UniTransaction::IsActive(ShardId sid) const {
  if (unique_shard_cnt_ == 0)
    return false;
  DCHECK(sd_.local_mask & ACTIVE);
  return sid == unique_shard_id_;
}

string UniTransaction::DebugId(optional<ShardId> sid) const {
  string res = absl::StrCat("utx[", txid_, "] ");
  if (cid_)
    absl::StrAppend(&res, cid_->name(), " ");
  if (sid)
    absl::StrAppend(&res, "sid:", *sid);
  return res;
}

void UniTransaction::LogAutoJournalOnShard(EngineShard* shard, RunnableResult result) {
  if (shard == nullptr)
    return;

  if (!cid_->IsJournaled() && (cid_->opt_mask() & CO::NO_KEY_TRANSACTIONAL) == 0)
    return;

  if (!shard->journal())
    return;

  if (result.status != OpStatus::OK)
    return;

  if ((cid_->opt_mask() & CO::NO_AUTOJOURNAL) && !re_enabled_auto_journal_)
    return;

  journal::Entry::Payload entry_payload = journal::Entry::Payload(cid_->name(), full_args_);
  LogJournalOnShard(std::move(entry_payload));
}

void UniTransaction::LogJournalOnShard(journal::Entry::Payload&& payload) const {
  journal::RecordEntry(txid_, journal::Op::COMMAND, db_index_,
                       unique_slot_checker_.GetUniqueSlotId(), std::move(payload));
}

void UniTransaction::ReviveAutoJournal() {
  DCHECK(cid_->opt_mask() & CO::NO_AUTOJOURNAL);
  DCHECK_EQ(run_barrier_.DEBUG_Count(), 0u);
  re_enabled_auto_journal_ = true;
}

optional<SlotId> UniTransaction::GetUniqueSlotId() const {
  return unique_slot_checker_.GetUniqueSlotId();
}

}  // namespace dfly
