// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/transaction.h"

#include <absl/strings/match.h>

#include "base/logging.h"
#include "server/blocking_controller.h"
#include "server/command_registry.h"
#include "server/db_slice.h"
#include "server/engine_shard_set.h"
#include "server/journal/journal.h"
#include "server/server_state.h"

namespace dfly {

using namespace std;
using namespace util;
using absl::StrCat;

thread_local Transaction::TLTmpSpace Transaction::tmp_space;

namespace {

atomic_uint64_t op_seq{1};

constexpr size_t kTransSize [[maybe_unused]] = sizeof(Transaction);

}  // namespace

IntentLock::Mode Transaction::Mode() const {
  return (cid_->opt_mask() & CO::READONLY) ? IntentLock::SHARED : IntentLock::EXCLUSIVE;
}

/**
 * @brief Construct a new Transaction:: Transaction object
 *
 * @param cid
 * @param ess
 * @param cs
 */
Transaction::Transaction(const CommandId* cid) : cid_{cid} {
  string_view cmd_name(cid_->name());
  if (cmd_name == "EXEC" || cmd_name == "EVAL" || cmd_name == "EVALSHA") {
    multi_.reset(new MultiData);
    multi_->shard_journal_write.resize(shard_set->size(), false);

    multi_->mode = NOT_DETERMINED;
    multi_->role = DEFAULT;
  }
}

Transaction::Transaction(const Transaction* parent)
    : multi_{make_unique<MultiData>()}, txid_{parent->txid()} {
  multi_->mode = parent->multi_->mode;
  multi_->role = SQUASHED_STUB;

  time_now_ms_ = parent->time_now_ms_;
}

Transaction::~Transaction() {
  DVLOG(3) << "Transaction " << StrCat(Name(), "@", txid_, "/", unique_shard_cnt_, ")")
           << " destroyed";
}

void Transaction::InitBase(DbIndex dbid, CmdArgList args) {
  global_ = false;
  db_index_ = dbid;
  full_args_ = args;
  local_result_ = OpStatus::OK;
}

void Transaction::InitGlobal() {
  DCHECK(!multi_ || (multi_->mode == GLOBAL || multi_->mode == NON_ATOMIC));

  global_ = true;
  unique_shard_cnt_ = shard_set->size();
  shard_data_.resize(unique_shard_cnt_);
  for (auto& sd : shard_data_)
    sd.local_mask = ACTIVE;
}

void Transaction::InitNoKey() {
  // No key command will use the first shard.
  unique_shard_cnt_ = 1;
  unique_shard_id_ = 0;
  shard_data_.resize(1);
  shard_data_.front().local_mask |= ACTIVE;
}

void Transaction::BuildShardIndex(KeyIndex key_index, bool rev_mapping,
                                  std::vector<PerShardCache>* out) {
  auto args = full_args_;

  auto& shard_index = *out;

  auto add = [this, rev_mapping, &shard_index](uint32_t sid, uint32_t i) {
    string_view val = ArgS(full_args_, i);
    shard_index[sid].args.push_back(val);
    if (rev_mapping)
      shard_index[sid].original_index.push_back(i);
  };

  if (key_index.bonus) {
    DCHECK(key_index.step == 1);
    uint32_t sid = Shard(ArgS(args, *key_index.bonus), shard_data_.size());
    add(sid, *key_index.bonus);
  }

  for (unsigned i = key_index.start; i < key_index.end; ++i) {
    uint32_t sid = Shard(ArgS(args, i), shard_data_.size());
    add(sid, i);

    DCHECK_LE(key_index.step, 2u);
    if (key_index.step == 2) {  // Handle value associated with preceding key.
      add(sid, ++i);
    }
  }
}

void Transaction::InitShardData(absl::Span<const PerShardCache> shard_index, size_t num_args,
                                bool rev_mapping) {
  args_.reserve(num_args);
  if (rev_mapping)
    reverse_index_.reserve(args_.size());

  // Store the concatenated per-shard arguments from the shard index inside args_
  // and make each shard data point to its own sub-span inside args_.
  for (size_t i = 0; i < shard_data_.size(); ++i) {
    auto& sd = shard_data_[i];
    auto& si = shard_index[i];

    sd.arg_count = si.args.size();
    sd.arg_start = args_.size();

    // Multi transactions can re-initialize on different shards, so clear ACTIVE flag.
    if (multi_)
      sd.local_mask &= ~ACTIVE;

    if (sd.arg_count == 0)
      continue;

    sd.local_mask |= ACTIVE;

    unique_shard_cnt_++;
    unique_shard_id_ = i;

    for (size_t j = 0; j < si.args.size(); ++j) {
      args_.push_back(si.args[j]);
      if (rev_mapping)
        reverse_index_.push_back(si.original_index[j]);
    }
  }

  CHECK_EQ(args_.size(), num_args);
}

void Transaction::InitMultiData(KeyIndex key_index) {
  DCHECK(multi_);

  if (multi_->mode == NON_ATOMIC)
    return;

  IntentLock::Mode mode = Mode();

  auto& tmp_uniques = tmp_space.uniq_keys;
  tmp_uniques.clear();

  auto lock_key = [this, mode, &tmp_uniques](string_view key) {
    if (auto [_, inserted] = tmp_uniques.insert(KeyLockArgs::GetLockKey(key)); !inserted)
      return;

    multi_->lock_counts[key][mode]++;
  };

  // With EVAL, we call this function for EVAL itself as well as for each command
  // for eval. currently, we lock everything only during the eval call.
  if (!multi_->locks_recorded) {
    for (size_t i = key_index.start; i < key_index.end; i += key_index.step)
      lock_key(ArgS(full_args_, i));
    if (key_index.bonus)
      lock_key(ArgS(full_args_, *key_index.bonus));
  }

  multi_->locks_recorded = true;
  DCHECK(IsAtomicMulti());
  DCHECK(multi_->mode == GLOBAL || !multi_->lock_counts.empty());
}

void Transaction::StoreKeysInArgs(KeyIndex key_index, bool rev_mapping) {
  DCHECK(!key_index.bonus);
  DCHECK(key_index.step == 1u || key_index.step == 2u);

  // even for a single key we may have multiple arguments per key (MSET).
  for (unsigned j = key_index.start; j < key_index.end; j++) {
    args_.push_back(ArgS(full_args_, j));
    if (key_index.step == 2)
      args_.push_back(ArgS(full_args_, ++j));
  }

  if (rev_mapping) {
    reverse_index_.resize(args_.size());
    for (unsigned j = 0; j < reverse_index_.size(); ++j) {
      reverse_index_[j] = j + key_index.start;
    }
  }
}

/**
 *
 * There are 4 options that we consider here:
 * a. T spans a single shard and it's not multi.
 *    unique_shard_id_ is predefined before the schedule() is called.
 *    In that case only a single thread will be scheduled and it will use shard_data[0] just because
 *    shard_data.size() = 1. Coordinator thread can access any data because there is a
 *    schedule barrier between InitByArgs and RunInShard/IsArmedInShard functions.
 * b. T spans multiple shards and its not multi
 *    In that case multiple threads will be scheduled. Similarly they have a schedule barrier,
 *    and IsArmedInShard can read any variable from shard_data[x].
 * c. Trans spans a single shard and it's multi. shard_data has size of ess_.size.
 *    IsArmedInShard will check shard_data[x].
 * d. Trans spans multiple shards and it's multi. Similarly shard_data[x] will be checked.
 *    unique_shard_cnt_ and unique_shard_id_ are not accessed until shard_data[x] is armed, hence
 *    we have a barrier between coordinator and engine-threads. Therefore there should not be
 *    data races.
 *
 **/

void Transaction::InitByKeys(KeyIndex key_index) {
  auto args = full_args_;

  if (key_index.start == args.size()) {  // eval with 0 keys.
    CHECK(absl::StartsWith(cid_->name(), "EVAL")) << cid_->name();
    return;
  }

  DCHECK_LT(key_index.start, args.size());

  bool needs_reverse_mapping = cid_->opt_mask() & CO::REVERSE_MAPPING;

  // Stub transactions always operate only on single shard.
  if ((key_index.HasSingleKey() && !IsAtomicMulti()) || (multi_ && multi_->role == SQUASHED_STUB)) {
    DCHECK_GT(key_index.step, 0u);
    // We don't have to split the arguments by shards, so we can copy them directly.
    StoreKeysInArgs(key_index, needs_reverse_mapping);

    // Multi transactions that execute commands on their own (not stubs) can't shrink the backing
    // array, as it still might be read by leftover callbacks.
    shard_data_.resize(IsActiveMulti() ? shard_set->size() : 1);
    shard_data_.front().local_mask |= ACTIVE;

    unique_shard_cnt_ = 1;
    unique_shard_id_ = Shard(args_.front(), shard_set->size());  // TODO: Squashed bug

    return;
  }

  shard_data_.resize(shard_set->size());  // shard_data isn't sparse, so we must allocate for all :(
  CHECK(key_index.step == 1 || key_index.step == 2);
  DCHECK(key_index.step == 1 || (args.size() % 2) == 0);

  // Safe, because flow below is not preemptive.
  auto& shard_index = tmp_space.GetShardIndex(shard_data_.size());

  // Distribute all the arguments by shards.
  BuildShardIndex(key_index, needs_reverse_mapping, &shard_index);

  // Initialize shard data based on distributed arguments.
  InitShardData(shard_index, key_index.num_args(), needs_reverse_mapping);

  if (multi_)
    InitMultiData(key_index);

  DVLOG(1) << "InitByArgs " << DebugId() << " " << args_.front();

  // Compress shard data, if we occupy only one shard.
  if (unique_shard_cnt_ == 1) {
    PerShardData* sd;
    if (IsActiveMulti()) {
      sd = &shard_data_[unique_shard_id_];
    } else {
      shard_data_.resize(1);
      sd = &shard_data_.front();
    }
    sd->local_mask |= ACTIVE;
    sd->arg_count = -1;
    sd->arg_start = -1;
  }

  // Validation. Check reverse mapping was built correctly.
  if (needs_reverse_mapping) {
    for (size_t i = 0; i < args_.size(); ++i) {
      DCHECK_EQ(args_[i], ArgS(args, reverse_index_[i]));
    }
  }

  // Validation.
  for (const auto& sd : shard_data_) {
    // sd.local_mask may be non-zero for multi transactions with instant locking.
    // Specifically EVALs may maintain state between calls.
    DCHECK(!sd.is_armed.load(std::memory_order_relaxed));
    if (!multi_) {
      DCHECK_EQ(TxQueue::kEnd, sd.pq_pos);
    }
  }
}

OpStatus Transaction::InitByArgs(DbIndex index, CmdArgList args) {
  InitBase(index, args);

  if ((cid_->opt_mask() & CO::GLOBAL_TRANS) > 0) {
    InitGlobal();
    return OpStatus::OK;
  }

  if ((cid_->opt_mask() & CO::NO_KEY_JOURNAL) > 0) {
    InitNoKey();
    return OpStatus::OK;
  }

  DCHECK_EQ(unique_shard_cnt_, 0u);
  DCHECK(args_.empty());

  OpResult<KeyIndex> key_index = DetermineKeys(cid_, args);
  if (!key_index)
    return key_index.status();

  InitByKeys(*key_index);
  return OpStatus::OK;
}

void Transaction::PrepareSquashedMultiHop(const CommandId* cid,
                                          absl::FunctionRef<bool(ShardId)> enabled) {
  CHECK(multi_->mode == GLOBAL || multi_->mode == LOCK_AHEAD);

  MultiSwitchCmd(cid);

  multi_->role = SQUASHER;
  InitBase(db_index_, {});

  DCHECK_EQ(shard_data_.size(), shard_set->size());
  for (unsigned i = 0; i < shard_data_.size(); i++) {
    if (enabled(i)) {
      shard_data_[i].local_mask |= ACTIVE;
      unique_shard_cnt_++;
      unique_shard_id_ = i;
    } else {
      shard_data_[i].local_mask &= ~ACTIVE;
    }
  }
}

void Transaction::StartMultiGlobal(DbIndex dbid) {
  CHECK(multi_);
  CHECK(shard_data_.empty());  // Make sure default InitByArgs didn't run.

  multi_->mode = GLOBAL;
  InitBase(dbid, {});
  InitGlobal();
  multi_->locks_recorded = true;

  ScheduleInternal();
}

void Transaction::StartMultiLockedAhead(DbIndex dbid, CmdArgList keys) {
  DCHECK(multi_);
  DCHECK(shard_data_.empty());  // Make sure default InitByArgs didn't run.

  multi_->mode = LOCK_AHEAD;
  InitBase(dbid, keys);
  InitByKeys(KeyIndex::Range(0, keys.size()));

  ScheduleInternal();
}

void Transaction::StartMultiNonAtomic() {
  DCHECK(multi_);
  multi_->mode = NON_ATOMIC;
}

void Transaction::MultiSwitchCmd(const CommandId* cid) {
  DCHECK(multi_);
  DCHECK(!cb_ptr_);

  unique_shard_id_ = 0;
  unique_shard_cnt_ = 0;
  args_.clear();
  cid_ = cid;
  cb_ptr_ = nullptr;

  if (multi_->mode == NON_ATOMIC || multi_->role == SQUASHED_STUB) {
    // Reset shard data without resizing because armed might be read from cancelled callbacks.
    for (auto& sd : shard_data_) {
      sd.arg_count = sd.arg_start = sd.local_mask = 0;
      sd.pq_pos = TxQueue::kEnd;
      DCHECK_EQ(sd.is_armed.load(memory_order_relaxed), false);
    }
    coordinator_state_ = 0;
  }

  if (multi_->mode == NON_ATOMIC)
    txid_ = 0;

  if (multi_->role == SQUASHER)
    multi_->role = DEFAULT;
}

string Transaction::DebugId() const {
  DCHECK_GT(use_count_.load(memory_order_relaxed), 0u);

  return StrCat(Name(), "@", txid_, "/", unique_shard_cnt_, " (", trans_id(this), ")");
}

// Runs in the dbslice thread. Returns true if the transaction continues running in the thread.
bool Transaction::RunInShard(EngineShard* shard, bool txq_ooo) {
  DCHECK_GT(run_count_.load(memory_order_relaxed), 0u);
  CHECK(cb_ptr_) << DebugId();
  DCHECK_GT(txid_, 0u);

  // Unlike with regular transactions we do not acquire locks upon scheduling
  // because Scheduling is done before multi-exec batch is executed. Therefore we
  // lock keys right before the execution of each statement.

  unsigned idx = SidToId(shard->shard_id());
  auto& sd = shard_data_[idx];

  bool prev_armed = sd.is_armed.load(memory_order_relaxed);
  DCHECK(prev_armed);
  sd.is_armed.store(false, memory_order_relaxed);

  VLOG(2) << "RunInShard: " << DebugId() << " sid:" << shard->shard_id() << " " << sd.local_mask;

  bool was_suspended = sd.local_mask & SUSPENDED_Q;
  bool awaked_prerun = sd.local_mask & AWAKED_Q;

  bool is_concluding = (coordinator_state_ & COORD_EXEC_CONCLUDING);
  bool tx_stop_runnig = is_concluding && !IsAtomicMulti();

  IntentLock::Mode mode = Mode();

  DCHECK(IsGlobal() || (sd.local_mask & KEYLOCK_ACQUIRED) || (multi_ && multi_->mode == GLOBAL));

  if (txq_ooo) {
    DCHECK(sd.local_mask & OUT_OF_ORDER);
  }

  /*************************************************************************/
  // Actually running the callback.
  // If you change the logic here, also please change the logic
  try {
    OpStatus status = OpStatus::OK;

    // if a transaction is suspended, we still run it because of brpoplpush/blmove case
    // that needs to run lpush on its suspended shard.
    status = (*cb_ptr_)(this, shard);

    if (unique_shard_cnt_ == 1) {
      cb_ptr_ = nullptr;  // We can do it because only a single thread runs the callback.
      local_result_ = status;
    } else {
      if (status == OpStatus::OUT_OF_MEMORY) {
        local_result_ = status;
      } else {
        CHECK_EQ(OpStatus::OK, status);
      }
    }
  } catch (std::bad_alloc&) {
    // TODO: to log at most once per sec.
    LOG_FIRST_N(ERROR, 16) << " out of memory";
    local_result_ = OpStatus::OUT_OF_MEMORY;
  } catch (std::exception& e) {
    LOG(FATAL) << "Unexpected exception " << e.what();
  }

  /*************************************************************************/

  if (is_concluding)  // Check last hop
    LogAutoJournalOnShard(shard);

  // at least the coordinator thread owns the reference.
  DCHECK_GE(GetUseCount(), 1u);

  // If we're the head of tx queue (txq_ooo is false), we remove ourselves upon first invocation
  // and successive hops are run by continuation_trans_ in engine shard.
  // Otherwise we can remove ourselves only when we're concluding (so no more hops will follow).
  // In case of multi transaction is_concluding represents only if the current running op is
  // concluding, therefore we remove from txq in unlock multi function which is when the transaction
  // is concluding.
  bool remove_txq = tx_stop_runnig || !txq_ooo;
  if (remove_txq && sd.pq_pos != TxQueue::kEnd) {
    VLOG(2) << "Remove from txq" << this->DebugId();
    shard->txq()->Remove(sd.pq_pos);
    sd.pq_pos = TxQueue::kEnd;
  }

  // For multi we unlock transaction (i.e. its keys) in UnlockMulti() call.
  // If it's a final hop we should release the locks.
  if (tx_stop_runnig) {
    bool became_suspended = sd.local_mask & SUSPENDED_Q;
    KeyLockArgs largs;

    if (IsGlobal()) {
      DCHECK(!awaked_prerun && !became_suspended);  // Global transactions can not be blocking.
      VLOG(2) << "Releasing shard lock";
      shard->shard_lock()->Release(Mode());
    } else {  // not global.
      largs = GetLockArgs(idx);
      DCHECK(sd.local_mask & KEYLOCK_ACQUIRED);

      // If a transaction has been suspended, we keep the lock so that future transaction
      // touching those keys will be ordered via TxQueue. It's necessary because we preserve
      // the atomicity of awaked transactions by halting the TxQueue.
      if (was_suspended || !became_suspended) {
        shard->db_slice().Release(mode, largs);
        sd.local_mask &= ~KEYLOCK_ACQUIRED;
      }
      sd.local_mask &= ~OUT_OF_ORDER;
    }

    // This is the last hop, so clear cont_trans if its held by the current tx
    shard->RemoveContTx(this);

    // It has 2 responsibilities.
    // 1: to go over potential wakened keys, verify them and activate watch queues.
    // 2: if this transaction was notified and finished running - to remove it from the head
    //    of the queue and notify the next one.
    if (auto* bcontroller = shard->blocking_controller(); bcontroller) {
      if (awaked_prerun || was_suspended) {
        bcontroller->FinalizeWatched(largs, this);
      }

      // Wake only if no tx queue head is currently running
      // Note: RemoveContTx might have no effect above if this tx had no continuations
      if (shard->GetContTx() == nullptr) {
        bcontroller->NotifyPending();
      }
    }
  }

  CHECK_GE(DecreaseRunCnt(), 1u);
  // From this point on we can not access 'this'.

  return !tx_stop_runnig;
}

void Transaction::ScheduleInternal() {
  DCHECK(!shard_data_.empty());
  DCHECK_EQ(0u, txid_);
  DCHECK_EQ(0, coordinator_state_ & (COORD_SCHED | COORD_OOO));

  bool span_all = IsGlobal();

  uint32_t num_shards;
  std::function<bool(uint32_t)> is_active;

  // TODO: For multi-transactions we should be able to deduce mode() at run-time based
  // on the context. For regular multi-transactions we can actually inspect all commands.
  // For eval-like transactions - we can decided based on the command flavor (EVAL/EVALRO) or
  // auto-tune based on the static analysis (by identifying commands with hardcoded command names).
  IntentLock::Mode mode = Mode();

  if (span_all) {
    is_active = [](uint32_t) { return true; };
    num_shards = shard_set->size();

    // Lock shards
    auto cb = [mode](EngineShard* shard) { shard->shard_lock()->Acquire(mode); };
    shard_set->RunBriefInParallel(std::move(cb));
    VLOG(1) << "Global shard lock acquired";
  } else {
    num_shards = unique_shard_cnt_;
    DCHECK_GT(num_shards, 0u);

    is_active = [&](uint32_t i) {
      return num_shards == 1 ? (i == unique_shard_id_) : shard_data_[i].local_mask & ACTIVE;
    };
  }

  // Loop until successfully scheduled in all shards.
  while (true) {
    txid_ = op_seq.fetch_add(1, memory_order_relaxed);
    time_now_ms_ = GetCurrentTimeMs();

    atomic_uint32_t lock_granted_cnt{0};
    atomic_uint32_t success{0};

    auto cb = [&](EngineShard* shard) {
      auto [is_success, is_granted] = ScheduleInShard(shard);
      success.fetch_add(is_success, memory_order_relaxed);
      lock_granted_cnt.fetch_add(is_granted, memory_order_relaxed);
    };
    shard_set->RunBriefInParallel(std::move(cb), is_active);

    bool ooo_disabled = IsGlobal() || (IsAtomicMulti() && multi_->mode != LOCK_AHEAD);

    if (success.load(memory_order_acquire) == num_shards) {
      coordinator_state_ |= COORD_SCHED;
      // If we granted all locks, we can run out of order.
      if (!ooo_disabled && lock_granted_cnt.load(memory_order_relaxed) == num_shards) {
        coordinator_state_ |= COORD_OOO;
      }
      VLOG(2) << "Scheduled " << DebugId()
              << " OutOfOrder: " << bool(coordinator_state_ & COORD_OOO)
              << " num_shards: " << num_shards;

      break;
    }

    VLOG(2) << "Cancelling " << DebugId();

    atomic_bool should_poll_execution{false};
    auto cancel = [&](EngineShard* shard) {
      bool res = CancelShardCb(shard);
      if (res) {
        should_poll_execution.store(true, memory_order_relaxed);
      }
    };
    shard_set->RunBriefInParallel(std::move(cancel), is_active);

    // We must follow up with PollExecution because in rare cases with multi-trans
    // that follows this one, we may find the next transaction in the queue that is never
    // trigerred. Which leads to deadlock. I could solve this by adding PollExecution to
    // CancelShardCb above but then we would need to use the shard_set queue since PollExecution
    // is blocking. I wanted to avoid the additional latency for the general case of running
    // CancelShardCb because of the very rate case below. Therefore, I decided to just fetch the
    // indication that we need to follow up with PollExecution and then send it to shard_set queue.
    // We do not need to wait for this callback to finish - just make sure it will eventually run.
    // See https://github.com/dragonflydb/dragonfly/issues/150 for more info.
    if (should_poll_execution.load(memory_order_relaxed)) {
      for (uint32_t i = 0; i < shard_set->size(); ++i) {
        if (!is_active(i))
          continue;

        shard_set->Add(i, [] { EngineShard::tlocal()->PollExecution("cancel_cleanup", nullptr); });
      }
    }
  }

  if (IsOOO()) {
    for (auto& sd : shard_data_) {
      sd.local_mask |= OUT_OF_ORDER;
    }
  }
}

// Optimized "Schedule and execute" function for the most common use-case of a single hop
// transactions like set/mset/mget etc. Does not apply for more complicated cases like RENAME or
// BLPOP where a data must be read from multiple shards before performing another hop.
OpStatus Transaction::ScheduleSingleHop(RunnableType cb) {
  DCHECK(!cb_ptr_);

  if (multi_ && multi_->role == SQUASHED_STUB) {
    return RunSquashedMultiCb(cb);
  }

  cb_ptr_ = &cb;

  DCHECK(IsAtomicMulti() || (coordinator_state_ & COORD_SCHED) == 0);  // Multi schedule in advance.
  coordinator_state_ |= (COORD_EXEC | COORD_EXEC_CONCLUDING);  // Single hop means we conclude.

  bool was_ooo = false;

  // If we run only on one shard and conclude, we can avoid scheduling at all
  // and directly dispatch the task to its destination shard.
  bool schedule_fast = (unique_shard_cnt_ == 1) && !IsGlobal() && !IsAtomicMulti();
  if (schedule_fast) {
    DCHECK_NE(unique_shard_id_, kInvalidSid);
    DCHECK(shard_data_.size() == 1 || multi_->mode == NON_ATOMIC);

    // IsArmedInShard() first checks run_count_ before shard_data, so use release ordering.
    shard_data_[SidToId(unique_shard_id_)].is_armed.store(true, memory_order_relaxed);
    run_count_.store(1, memory_order_release);

    time_now_ms_ = GetCurrentTimeMs();

    // NOTE: schedule_cb cannot update data on stack when run_fast is false.
    // This is because ScheduleSingleHop can finish before the callback returns.

    // This happens when ScheduleUniqueShard schedules into TxQueue (hence run_fast is false), and
    // then calls PollExecute that in turn runs the callback which calls DecreaseRunCnt. As a result
    // WaitForShardCallbacks below is unblocked before schedule_cb returns. However, if run_fast is
    // true, then we may mutate stack variables, but only before DecreaseRunCnt is called.
    auto schedule_cb = [this, &was_ooo] {
      bool run_fast = ScheduleUniqueShard(EngineShard::tlocal());
      if (run_fast) {
        was_ooo = true;
        // it's important to DecreaseRunCnt only for run_fast and after was_ooo is assigned.
        // If DecreaseRunCnt were called before ScheduleUniqueShard finishes
        // then WaitForShardCallbacks below could exit before schedule_cb assigns return value
        // to was_ooo and cause stack corruption.
        CHECK_GE(DecreaseRunCnt(), 1u);
      }
    };

    if (auto* ss = ServerState::tlocal();
        ss->thread_index() == unique_shard_id_ && ss->AllowInlineScheduling()) {
      DVLOG(2) << "Inline scheduling a transaction";
      schedule_cb();
    } else {
      shard_set->Add(unique_shard_id_, std::move(schedule_cb));  // serves as a barrier.
    }
  } else {
    // This transaction either spans multiple shards and/or is multi.

    if (!IsAtomicMulti())  // Multi schedule in advance.
      ScheduleInternal();

    ExecuteAsync();
  }

  DVLOG(2) << "ScheduleSingleHop before Wait " << DebugId() << " " << run_count_.load();
  WaitForShardCallbacks();
  DVLOG(2) << "ScheduleSingleHop after Wait " << DebugId();

  if (was_ooo) {
    coordinator_state_ |= COORD_OOO;
  }

  if (schedule_fast) {
    CHECK(!cb_ptr_);  // we should have reset it within the callback.
  }
  cb_ptr_ = nullptr;
  return local_result_;
}

OpStatus Transaction::ScheduleRemoteCoordination(RunnableType cb) {
  DCHECK(!cb_ptr_);
  DCHECK_EQ(unique_shard_cnt_, 1UL);
  DCHECK_NE(unique_shard_id_, kInvalidSid);
  // DCHECK_EQ(shard_data_.size(), 1);
  DCHECK(multi_);
  DCHECK_EQ(multi_->mode, LOCK_AHEAD);
  DCHECK(!IsGlobal());

  DCHECK_NE(ServerState::tlocal()->thread_index(), unique_shard_id_);

  BlockingCounter blocker(1);
  auto wrapped_cb = [&]() {
    CHECK_EQ(ServerState::tlocal()->thread_index(), unique_shard_id_);

    auto* shard = EngineShard::tlocal();
    bool run_now = false;
    if (this->IsOOO()) {
      run_now = true;
    } else {
      TxQueue* txq = shard->txq();
      if (txq->Empty()) {
        run_now = true;
      } else {
        Transaction* head = std::get<Transaction*>(txq->Front());
        if (head == this) {
          run_now = true;
        }
      }
    }

    if (run_now) {
      PerShardData& sd = shard_data_[SidToId(unique_shard_id_)];
      sd.is_armed.store(false, memory_order_relaxed);
      run_count_.store(0, memory_order_release);

      local_result_ = cb(this, shard);

      blocker.Dec();
    }
  };

  coordinator_state_ |= COORD_EXEC;

  ExecuteAsyncShahar(wrapped_cb);

  DVLOG(2) << "ScheduleRemoteCoordination before Wait " << DebugId() << " " << run_count_.load();
  blocker.Wait();
  DVLOG(2) << "ScheduleRemoteCoordination after Wait " << DebugId();

  cb_ptr_ = nullptr;
  return local_result_;
}

void Transaction::ReportWritesSquashedMulti(absl::FunctionRef<bool(ShardId)> had_write) {
  DCHECK(multi_);
  for (unsigned i = 0; i < multi_->shard_journal_write.size(); i++)
    multi_->shard_journal_write[i] |= had_write(i);
}

// Runs in the coordinator fiber.
void Transaction::UnlockMulti() {
  VLOG(1) << "UnlockMulti " << DebugId();
  DCHECK(multi_);
  DCHECK_GE(GetUseCount(), 1u);  // Greater-equal because there may be callbacks in progress.

  if (multi_->mode == NON_ATOMIC)
    return;

  auto sharded_keys = make_shared<vector<KeyList>>(shard_set->size());
  while (!multi_->lock_counts.empty()) {
    auto entry = multi_->lock_counts.extract(multi_->lock_counts.begin());
    ShardId sid = Shard(entry.key(), sharded_keys->size());
    (*sharded_keys)[sid].emplace_back(std::move(entry.key()), entry.mapped());
  }

  unsigned shard_journals_cnt =
      ServerState::tlocal()->journal() ? CalcMultiNumOfShardJournals() : 0;

  uint32_t prev = run_count_.fetch_add(shard_data_.size(), memory_order_relaxed);
  DCHECK_EQ(prev, 0u);

  use_count_.fetch_add(shard_data_.size(), std::memory_order_relaxed);
  for (ShardId i = 0; i < shard_data_.size(); ++i) {
    shard_set->Add(i, [this, sharded_keys, shard_journals_cnt]() {
      this->UnlockMultiShardCb(*sharded_keys, EngineShard::tlocal(), shard_journals_cnt);
      intrusive_ptr_release(this);
    });
  }

  VLOG(1) << "UnlockMultiEnd " << DebugId();
}

uint32_t Transaction::CalcMultiNumOfShardJournals() const {
  uint32_t shard_journals_cnt = 0;
  for (bool was_shard_write : multi_->shard_journal_write) {
    if (was_shard_write) {
      ++shard_journals_cnt;
    }
  }
  return shard_journals_cnt;
}

void Transaction::Schedule() {
  if (multi_ && multi_->role == SQUASHED_STUB)
    return;

  if (!IsAtomicMulti())
    ScheduleInternal();
}

// Runs in coordinator thread.
void Transaction::Execute(RunnableType cb, bool conclude) {
  if (multi_ && multi_->role == SQUASHED_STUB) {
    RunSquashedMultiCb(cb);
    return;
  }

  DCHECK(coordinator_state_ & COORD_SCHED);
  DCHECK(!cb_ptr_);

  cb_ptr_ = &cb;
  coordinator_state_ |= COORD_EXEC;

  if (conclude) {
    coordinator_state_ |= COORD_EXEC_CONCLUDING;
  } else {
    coordinator_state_ &= ~COORD_EXEC_CONCLUDING;
  }

  ExecuteAsync();

  DVLOG(1) << "Wait on Exec " << DebugId();
  WaitForShardCallbacks();
  DVLOG(1) << "Wait on Exec " << DebugId() << " completed";

  cb_ptr_ = nullptr;
}

// Runs in coordinator thread.
void Transaction::ExecuteAsync() {
  DVLOG(1) << "ExecuteAsync " << DebugId();

  DCHECK_GT(unique_shard_cnt_, 0u);
  DCHECK_GT(use_count_.load(memory_order_relaxed), 0u);
  DCHECK(!IsAtomicMulti() || multi_->locks_recorded);

  // We do not necessarily Execute this transaction in 'cb' below. It well may be that it will be
  // executed by the engine shard once it has been armed and coordinator thread will finish the
  // transaction before engine shard thread stops accessing it. Therefore, we increase reference
  // by number of callbacks accessing 'this' to allow callbacks to execute shard->Execute(this);
  // safely.
  use_count_.fetch_add(unique_shard_cnt_, memory_order_relaxed);

  // We access sd.is_armed outside of shard-threads but we guard it with run_count_ release.
  IterateActiveShards(
      [](PerShardData& sd, auto i) { sd.is_armed.store(true, memory_order_relaxed); });

  uint32_t seq = seqlock_.load(memory_order_relaxed);

  // this fence prevents that a read or write operation before a release fence will be reordered
  // with a write operation after a release fence. Specifically no writes below will be reordered
  // upwards. Important, because it protects non-threadsafe local_mask from being accessed by
  // IsArmedInShard in other threads.
  run_count_.store(unique_shard_cnt_, memory_order_release);

  auto* ss = ServerState::tlocal();
  if (unique_shard_cnt_ == 1 && ss->thread_index() == unique_shard_id_ &&
      ss->AllowInlineScheduling()) {
    DVLOG(1) << "Short-circuit ExecuteAsync " << DebugId();
    EngineShard::tlocal()->PollExecution("exec_cb", this);
    intrusive_ptr_release(this);  // against use_count_.fetch_add above.
    return;
  }

  // We verify seq lock has the same generation number. See below for more info.
  auto cb = [seq, this] {
    EngineShard* shard = EngineShard::tlocal();

    bool is_armed = IsArmedInShard(shard->shard_id());
    // First we check that this shard should run a callback by checking IsArmedInShard.
    if (is_armed) {
      uint32_t seq_after = seqlock_.load(memory_order_relaxed);

      DVLOG(3) << "PollExecCb " << DebugId() << " sid(" << shard->shard_id() << ") "
               << run_count_.load(memory_order_relaxed);

      // We also make sure that for multi-operation transactions like Multi/Eval
      // this callback runs on a correct operation. We want to avoid a situation
      // where the first operation is executed and the second operation is armed and
      // now this callback from the previous operation finally runs and calls PollExecution.
      // It is usually ok, but for single shard operations we abuse index 0 in shard_data_
      // Therefore we may end up with a situation where this old callback runs on shard 7,
      // accessing shard_data_[0] that now represents shard 5 for the next operation.
      // seqlock provides protection for that so each cb will only run on the operation it has
      // been tasked with.
      // We also must first check is_armed and only then seqlock. The first check ensures that
      // the coordinator thread crossed
      // "run_count_.store(unique_shard_cnt_, memory_order_release);" barrier and our seqlock_
      // is valid.
      if (seq_after == seq) {
        // shard->PollExecution(this) does not necessarily execute this transaction.
        // Therefore, everything that should be handled during the callback execution
        // should go into RunInShard.
        shard->PollExecution("exec_cb", this);
      } else {
        VLOG(1) << "Skipping PollExecution " << DebugId() << " sid(" << shard->shard_id() << ")";
      }
    }

    DVLOG(3) << "ptr_release " << DebugId() << " " << seq;
    intrusive_ptr_release(this);  // against use_count_.fetch_add above.
  };

  // IsArmedInShard is the protector of non-thread safe data.
  IterateActiveShards([&cb](PerShardData& sd, auto i) { shard_set->Add(i, cb); });
}

void Transaction::ExecuteAsyncShahar(absl::FunctionRef<void()> cb) {
  DVLOG(1) << "ExecuteAsyncShahar " << DebugId();

  DCHECK_GT(unique_shard_cnt_, 0u);
  DCHECK_GT(use_count_.load(memory_order_relaxed), 0u);
  DCHECK(!IsAtomicMulti() || multi_->locks_recorded);

  // We do not necessarily Execute this transaction in 'cb' below. It well may be that it will be
  // executed by the engine shard once it has been armed and coordinator thread will finish the
  // transaction before engine shard thread stops accessing it. Therefore, we increase reference
  // by number of callbacks accessing 'this' to allow callbacks to execute shard->Execute(this);
  // safely.
  use_count_.fetch_add(unique_shard_cnt_, memory_order_relaxed);

  // We access sd.is_armed outside of shard-threads but we guard it with run_count_ release.
  IterateActiveShards(
      [](PerShardData& sd, auto i) { sd.is_armed.store(true, memory_order_relaxed); });

  // this fence prevents that a read or write operation before a release fence will be reordered
  // with a write operation after a release fence. Specifically no writes below will be reordered
  // upwards. Important, because it protects non-threadsafe local_mask from being accessed by
  // IsArmedInShard in other threads.
  run_count_.store(unique_shard_cnt_, memory_order_release);

  IterateActiveShards([&cb](PerShardData& sd, auto i) { shard_set->Add(i, cb); });
}

void Transaction::Conclude() {
  auto cb = [](Transaction* t, EngineShard* shard) { return OpStatus::OK; };
  Execute(std::move(cb), true);
}

void Transaction::RunQuickie(EngineShard* shard) {
  DCHECK(!IsAtomicMulti());
  DCHECK(shard_data_.size() == 1u || multi_->mode == NON_ATOMIC);
  DCHECK_NE(unique_shard_id_, kInvalidSid);
  DCHECK_EQ(0u, txid_);

  shard->IncQuickRun();

  auto& sd = shard_data_[SidToId(unique_shard_id_)];
  DCHECK_EQ(0, sd.local_mask & (KEYLOCK_ACQUIRED | OUT_OF_ORDER));

  DVLOG(1) << "RunQuickSingle " << DebugId() << " " << shard->shard_id();
  DCHECK(cb_ptr_) << DebugId() << " " << shard->shard_id();

  // Calling the callback in somewhat safe way
  try {
    local_result_ = (*cb_ptr_)(this, shard);
  } catch (std::bad_alloc&) {
    LOG_FIRST_N(ERROR, 16) << " out of memory";
    local_result_ = OpStatus::OUT_OF_MEMORY;
  } catch (std::exception& e) {
    LOG(FATAL) << "Unexpected exception " << e.what();
  }

  LogAutoJournalOnShard(shard);

  sd.is_armed.store(false, memory_order_relaxed);
  cb_ptr_ = nullptr;  // We can do it because only a single shard runs the callback.
}

// runs in coordinator thread.
// Marks the transaction as expired and removes it from the waiting queue.
void Transaction::ExpireBlocking(WaitKeysProvider wcb) {
  DCHECK(!IsGlobal());
  DVLOG(1) << "ExpireBlocking " << DebugId();

  run_count_.store(unique_shard_cnt_, memory_order_release);

  auto expire_cb = [this, &wcb] {
    EngineShard* es = EngineShard::tlocal();
    ExpireShardCb(wcb(this, es), es);
  };
  IterateActiveShards([&expire_cb](PerShardData& sd, auto i) { shard_set->Add(i, expire_cb); });

  WaitForShardCallbacks();
  DVLOG(1) << "ExpireBlocking finished " << DebugId();
}

string_view Transaction::Name() const {
  return cid_->name();
}

ShardId Transaction::GetUniqueShard() const {
  DCHECK_EQ(GetUniqueShardCnt(), 1U);
  return unique_shard_id_;
}

KeyLockArgs Transaction::GetLockArgs(ShardId sid) const {
  KeyLockArgs res;
  res.db_index = db_index_;
  res.key_step = cid_->key_arg_step();
  res.args = GetShardArgs(sid);
  DCHECK(!res.args.empty() || (cid_->opt_mask() & CO::NO_KEY_JOURNAL));

  return res;
}

// Runs within a engine shard thread.
// Optimized path that schedules and runs transactions out of order if possible.
// Returns true if was eagerly executed, false if it was scheduled into queue.
bool Transaction::ScheduleUniqueShard(EngineShard* shard) {
  DCHECK(!IsAtomicMulti());
  DCHECK_EQ(0u, txid_);
  DCHECK(shard_data_.size() == 1u || multi_->mode == NON_ATOMIC);
  DCHECK_NE(unique_shard_id_, kInvalidSid);

  auto mode = Mode();
  auto lock_args = GetLockArgs(shard->shard_id());

  auto& sd = shard_data_[SidToId(unique_shard_id_)];
  DCHECK_EQ(TxQueue::kEnd, sd.pq_pos);

  // Fast path - for uncontended keys, just run the callback.
  // That applies for single key operations like set, get, lpush etc.
  if (shard->db_slice().CheckLock(mode, lock_args) && shard->shard_lock()->Check(mode)) {
    RunQuickie(shard);
    return true;
  }

  // we can do it because only a single thread writes into txid_ and sd.
  txid_ = op_seq.fetch_add(1, memory_order_relaxed);
  sd.pq_pos = shard->txq()->Insert(this);

  DCHECK_EQ(0, sd.local_mask & KEYLOCK_ACQUIRED);

  shard->db_slice().Acquire(mode, lock_args);
  sd.local_mask |= KEYLOCK_ACQUIRED;

  DVLOG(1) << "Rescheduling into TxQueue " << DebugId();

  shard->PollExecution("schedule_unique", nullptr);

  return false;
}

// This function should not block since it's run via RunBriefInParallel.
pair<bool, bool> Transaction::ScheduleInShard(EngineShard* shard) {
  DCHECK(shard_data_[SidToId(shard->shard_id())].local_mask & ACTIVE);

  // If a more recent transaction already commited, we abort
  if (shard->committed_txid() >= txid_)
    return {false, false};

  TxQueue* txq = shard->txq();
  KeyLockArgs lock_args;
  IntentLock::Mode mode = Mode();
  bool lock_granted = false;

  ShardId sid = SidToId(shard->shard_id());
  auto& sd = shard_data_[sid];

  // Acquire intent locks
  if (!IsGlobal()) {
    lock_args = GetLockArgs(shard->shard_id());

    // Key locks are acquired even if the shard is locked since intent locks are always acquired
    bool shard_unlocked = shard->shard_lock()->Check(mode);
    bool keys_unlocked = shard->db_slice().Acquire(mode, lock_args);

    lock_granted = keys_unlocked && shard_unlocked;
    sd.local_mask |= KEYLOCK_ACQUIRED;
    DVLOG(3) << "Lock granted " << lock_granted << " for trans " << DebugId();
  }

  // If the new transaction requires reordering of the pending queue (i.e. it comes before tail)
  // and some other transaction already locked its keys we can not reorder 'trans' because
  // that other transaction could have deduced that it can run OOO and eagerly execute. Hence, we
  // fail this scheduling attempt for trans.
  if (!txq->Empty() && txid_ < txq->TailScore() && !lock_granted) {
    if (sd.local_mask & KEYLOCK_ACQUIRED) {
      shard->db_slice().Release(mode, lock_args);
      sd.local_mask &= ~KEYLOCK_ACQUIRED;
    }
    return {false, false};
  }

  TxQueue::Iterator it = txq->Insert(this);
  DCHECK_EQ(TxQueue::kEnd, sd.pq_pos);
  sd.pq_pos = it;

  DVLOG(1) << "Insert into tx-queue, sid(" << sid << ") " << DebugId() << ", qlen " << txq->size();

  return {true, lock_granted};
}

bool Transaction::CancelShardCb(EngineShard* shard) {
  ShardId idx = SidToId(shard->shard_id());
  auto& sd = shard_data_[idx];

  auto pos = sd.pq_pos;
  if (pos == TxQueue::kEnd)
    return false;

  sd.pq_pos = TxQueue::kEnd;

  TxQueue* txq = shard->txq();
  TxQueue::Iterator head = txq->Head();
  auto val = txq->At(pos);
  Transaction* trans = absl::get<Transaction*>(val);
  DCHECK(trans == this) << "Pos " << pos << ", txq size " << txq->size() << ", trans " << trans;
  txq->Remove(pos);

  if (sd.local_mask & KEYLOCK_ACQUIRED) {
    auto mode = Mode();
    auto lock_args = GetLockArgs(shard->shard_id());
    DCHECK(lock_args.args.size() > 0);
    shard->db_slice().Release(mode, lock_args);
    sd.local_mask &= ~KEYLOCK_ACQUIRED;
  }

  if (pos == head && !txq->Empty()) {
    return true;
  }

  return false;
}

// runs in engine-shard thread.
ArgSlice Transaction::GetShardArgs(ShardId sid) const {
  // We can read unique_shard_cnt_  only because ShardArgsInShard is called after IsArmedInShard
  // barrier.
  if (unique_shard_cnt_ == 1) {
    return args_;
  }

  const auto& sd = shard_data_[sid];
  return ArgSlice{args_.data() + sd.arg_start, sd.arg_count};
}

// from local index back to original arg index skipping the command.
// i.e. returns (first_key_pos -1) or bigger.
size_t Transaction::ReverseArgIndex(ShardId shard_id, size_t arg_index) const {
  if (unique_shard_cnt_ == 1)
    return reverse_index_[arg_index];

  const auto& sd = shard_data_[shard_id];
  return reverse_index_[sd.arg_start + arg_index];
}

bool Transaction::WaitOnWatch(const time_point& tp, WaitKeysProvider wkeys_provider) {
  DVLOG(2) << "WaitOnWatch " << DebugId();
  using namespace chrono;

  auto cb = [&](Transaction* t, EngineShard* shard) {
    auto keys = wkeys_provider(t, shard);
    return t->WatchInShard(keys, shard);
  };

  Execute(move(cb), true);

  coordinator_state_ |= COORD_BLOCKED;

  auto wake_cb = [this] {
    return (coordinator_state_ & COORD_CANCELLED) ||
           wakeup_requested_.load(memory_order_relaxed) > 0;
  };

  auto* stats = ServerState::tl_connection_stats();
  ++stats->num_blocked_clients;

  cv_status status = cv_status::no_timeout;
  if (tp == time_point::max()) {
    DVLOG(1) << "WaitOnWatch foreva " << DebugId();
    blocking_ec_.await(move(wake_cb));
    DVLOG(1) << "WaitOnWatch AfterWait";
  } else {
    DVLOG(1) << "WaitOnWatch TimeWait for "
             << duration_cast<milliseconds>(tp - time_point::clock::now()).count() << " ms "
             << DebugId();

    status = blocking_ec_.await_until(move(wake_cb), tp);

    DVLOG(1) << "WaitOnWatch await_until " << int(status);
  }

  --stats->num_blocked_clients;

  bool is_expired = (coordinator_state_ & COORD_CANCELLED) || status == cv_status::timeout;
  if (is_expired)
    ExpireBlocking(wkeys_provider);

  coordinator_state_ &= ~COORD_BLOCKED;
  return !is_expired;
}

// Runs only in the shard thread.
OpStatus Transaction::WatchInShard(ArgSlice keys, EngineShard* shard) {
  ShardId idx = SidToId(shard->shard_id());

  auto& sd = shard_data_[idx];
  CHECK_EQ(0, sd.local_mask & SUSPENDED_Q);

  auto* bc = shard->EnsureBlockingController();
  bc->AddWatched(keys, this);

  sd.local_mask |= SUSPENDED_Q;
  sd.local_mask &= ~OUT_OF_ORDER;
  DVLOG(2) << "AddWatched " << DebugId() << " local_mask:" << sd.local_mask
           << ", first_key:" << keys.front();

  return OpStatus::OK;
}

void Transaction::ExpireShardCb(ArgSlice wkeys, EngineShard* shard) {
  auto lock_args = GetLockArgs(shard->shard_id());
  shard->db_slice().Release(Mode(), lock_args);

  unsigned sd_idx = SidToId(shard->shard_id());
  auto& sd = shard_data_[sd_idx];
  sd.local_mask |= EXPIRED_Q;
  sd.local_mask &= ~KEYLOCK_ACQUIRED;

  shard->blocking_controller()->FinalizeWatched(wkeys, this);
  DCHECK(!shard->blocking_controller()->awakened_transactions().contains(this));

  // Resume processing of transaction queue
  shard->PollExecution("unwatchcb", nullptr);

  CHECK_GE(DecreaseRunCnt(), 1u);
}

OpStatus Transaction::RunSquashedMultiCb(RunnableType cb) {
  DCHECK(multi_ && multi_->role == SQUASHED_STUB);
  DCHECK_EQ(unique_shard_cnt_, 1u);
  auto* shard = EngineShard::tlocal();
  auto status = cb(this, shard);
  LogAutoJournalOnShard(shard);
  return status;
}

void Transaction::UnlockMultiShardCb(const std::vector<KeyList>& sharded_keys, EngineShard* shard,
                                     uint32_t shard_journals_cnt) {
  auto journal = shard->journal();

  if (journal != nullptr && multi_->shard_journal_write[shard->shard_id()]) {
    journal->RecordEntry(txid_, journal::Op::EXEC, db_index_, shard_journals_cnt, {}, true);
  }

  if (multi_->mode == GLOBAL) {
    shard->shard_lock()->Release(IntentLock::EXCLUSIVE);
  } else {
    ShardId sid = shard->shard_id();
    for (const auto& k_v : sharded_keys[sid]) {
      auto release = [&](IntentLock::Mode mode) {
        if (k_v.second[mode]) {
          shard->db_slice().Release(mode, db_index_, k_v.first, k_v.second[mode]);
        }
      };

      release(IntentLock::SHARED);
      release(IntentLock::EXCLUSIVE);
    }
  }

  auto& sd = shard_data_[SidToId(shard->shard_id())];
  sd.local_mask |= UNLOCK_MULTI;

  // It does not have to be that all shards in multi transaction execute this tx.
  // Hence it could stay in the tx queue. We perform the necessary cleanup and remove it from
  // there. The transaction is not guaranteed to be at front.
  if (sd.pq_pos != TxQueue::kEnd) {
    DVLOG(1) << "unlockmulti: TxRemove " << DebugId();

    TxQueue* txq = shard->txq();
    DCHECK(!txq->Empty());
    DCHECK_EQ(absl::get<Transaction*>(txq->At(sd.pq_pos)), this);

    txq->Remove(sd.pq_pos);
    sd.pq_pos = TxQueue::kEnd;
  }

  shard->RemoveContTx(this);

  // Wake only if no tx queue head is currently running
  if (shard->blocking_controller() && shard->GetContTx() == nullptr)
    shard->blocking_controller()->NotifyPending();

  shard->PollExecution("unlockmulti", nullptr);

  this->DecreaseRunCnt();
}

inline uint32_t Transaction::DecreaseRunCnt() {
  // to protect against cases where Transaction is destroyed before run_ec_.notify
  // finishes running. We can not put it inside the (res == 1) block because then it's too late.
  ::boost::intrusive_ptr guard(this);

  // We use release so that no stores will be reordered after.
  // It's needed because we need to enforce that all stores executed before this point
  // are visible right after run_count_ is unblocked in the coordinator thread.
  // The fact that run_ec_.notify() does release operation is not enough, because
  // WaitForCallbacks might skip reading run_ec_ if run_count_ is already 0.
  uint32_t res = run_count_.fetch_sub(1, memory_order_release);
  if (res == 1) {
    run_ec_.notify();
  }
  return res;
}

bool Transaction::IsGlobal() const {
  return global_;
}

// Runs only in the shard thread.
// Returns true if the transacton has changed its state from suspended to awakened,
// false, otherwise.
bool Transaction::NotifySuspended(TxId committed_txid, ShardId sid, string_view key) {
  unsigned idx = SidToId(sid);
  auto& sd = shard_data_[idx];
  unsigned local_mask = sd.local_mask;

  if (local_mask & Transaction::EXPIRED_Q) {
    return false;
  }

  // Wake a transaction only once on the first notify.
  // We don't care about preserving the strict order with multiple operations running on blocking
  // keys in parallel, because the internal order is not observable from outside either way.
  if (wakeup_requested_.fetch_add(1, memory_order_relaxed) > 0)
    return false;

  DVLOG(1) << "NotifySuspended " << DebugId() << ", local_mask:" << local_mask << " by commited_id "
           << committed_txid;

  // local_mask could be awaked (i.e. not suspended) if the transaction has been
  // awakened by another key or awakened by the same key multiple times.
  if (local_mask & SUSPENDED_Q) {
    DCHECK_EQ(0u, local_mask & AWAKED_Q);

    sd.local_mask &= ~SUSPENDED_Q;
    sd.local_mask |= AWAKED_Q;

    // Find index of awakened key.
    auto args = GetShardArgs(sid);
    auto it =
        find_if(args.begin(), args.end(), [key](auto arg) { return facade::ToSV(arg) == key; });
    DCHECK(it != args.end());
    sd.wake_key_pos = it - args.begin();
  }

  blocking_ec_.notify();
  return true;
}

optional<string_view> Transaction::GetWakeKey(ShardId sid) const {
  auto& sd = shard_data_[SidToId(sid)];
  if ((sd.local_mask & AWAKED_Q) == 0)
    return nullopt;

  CHECK_NE(sd.wake_key_pos, UINT16_MAX);
  return GetShardArgs(sid).at(sd.wake_key_pos);
}

void Transaction::LogAutoJournalOnShard(EngineShard* shard) {
  // TODO: For now, we ignore non shard coordination.
  if (shard == nullptr)
    return;

  // Ignore technical squasher hops.
  if (multi_ && multi_->role == SQUASHER)
    return;

  bool journal_by_cmd_mask = true;
  if ((cid_->opt_mask() & CO::NO_KEY_JOURNAL) > 0) {
    journal_by_cmd_mask = true;  // Enforce journaling for commands that dont change the db.
  } else if ((cid_->opt_mask() & CO::WRITE) == 0) {
    journal_by_cmd_mask = false;  // Non-write command are not journaled.
  } else if ((cid_->opt_mask() & CO::NO_AUTOJOURNAL) > 0 &&
             !renabled_auto_journal_.load(memory_order_relaxed)) {
    journal_by_cmd_mask = false;  // Command disabled auto journal.
  }
  if (!journal_by_cmd_mask) {
    return;
  }

  auto journal = shard->journal();
  if (journal == nullptr)
    return;

  // TODO: Handle complex commands like LMPOP correctly once they are implemented.
  journal::Entry::Payload entry_payload;

  string_view cmd{cid_->name()};
  if (unique_shard_cnt_ == 1 || args_.empty()) {
    entry_payload = make_pair(cmd, full_args_);
  } else {
    entry_payload = make_pair(cmd, GetShardArgs(shard->shard_id()));
  }
  LogJournalOnShard(shard, std::move(entry_payload), unique_shard_cnt_, false, true);
}

void Transaction::LogJournalOnShard(EngineShard* shard, journal::Entry::Payload&& payload,
                                    uint32_t shard_cnt, bool multi_commands,
                                    bool allow_await) const {
  auto journal = shard->journal();
  CHECK(journal);
  if (multi_ && multi_->role != SQUASHED_STUB)
    multi_->shard_journal_write[shard->shard_id()] = true;

  bool is_multi = multi_commands || IsAtomicMulti();

  auto opcode = is_multi ? journal::Op::MULTI_COMMAND : journal::Op::COMMAND;
  journal->RecordEntry(txid_, opcode, db_index_, shard_cnt, std::move(payload), allow_await);
}

void Transaction::FinishLogJournalOnShard(EngineShard* shard, uint32_t shard_cnt) const {
  if (multi_) {
    return;
  }
  auto journal = shard->journal();
  CHECK(journal);
  journal->RecordEntry(txid_, journal::Op::EXEC, db_index_, shard_cnt, {}, false);
}

void Transaction::CancelBlocking() {
  if (coordinator_state_ & COORD_BLOCKED) {
    coordinator_state_ |= COORD_CANCELLED;
    blocking_ec_.notify();
  }
}

OpResult<KeyIndex> DetermineKeys(const CommandId* cid, CmdArgList args) {
  if (cid->opt_mask() & CO::GLOBAL_TRANS)
    return KeyIndex::Empty();

  KeyIndex key_index;

  int num_custom_keys = -1;

  if (cid->opt_mask() & CO::VARIADIC_KEYS) {
    // ZUNION/INTER <num_keys> <key1> [<key2> ...]
    // EVAL <script> <num_keys>
    // XREAD ... STREAMS ...
    if (args.size() < 2) {
      return OpStatus::SYNTAX_ERR;
    }

    string_view name{cid->name()};

    if (name == "XREAD" || name == "XREADGROUP") {
      for (size_t i = 0; i < args.size(); ++i) {
        string_view arg = ArgS(args, i);
        if (absl::EqualsIgnoreCase(arg, "STREAMS")) {
          size_t left = args.size() - i - 1;
          if (left < 2 || left % 2 != 0)
            return OpStatus::SYNTAX_ERR;

          key_index.start = i + 1;
          key_index.end = key_index.start + (left / 2);
          key_index.step = 1;

          return key_index;
        }
      }
      return OpStatus::SYNTAX_ERR;
    }

    if (absl::EndsWith(name, "STORE"))
      key_index.bonus = 0;  // Z<xxx>STORE <key> commands

    unsigned num_keys_index;
    if (absl::StartsWith(name, "EVAL"))
      num_keys_index = 1;
    else
      num_keys_index = key_index.bonus ? *key_index.bonus + 1 : 0;

    string_view num = ArgS(args, num_keys_index);
    if (!absl::SimpleAtoi(num, &num_custom_keys) || num_custom_keys < 0)
      return OpStatus::INVALID_INT;

    if (name == "ZDIFF" && num_custom_keys == 0) {
      return OpStatus::INVALID_INT;
    }

    if (name == "ZUNION" && num_custom_keys == 0) {
      return OpStatus::SYNTAX_ERR;
    }

    if (args.size() < size_t(num_custom_keys) + num_keys_index + 1)
      return OpStatus::SYNTAX_ERR;
  }

  if (cid->first_key_pos() > 0) {
    key_index.start = cid->first_key_pos() - 1;
    int last = cid->last_key_pos();
    if (num_custom_keys >= 0) {
      key_index.end = key_index.start + num_custom_keys;
    } else {
      key_index.end = last > 0 ? last : (int(args.size()) + last + 1);
    }
    key_index.step = cid->key_arg_step();

    return key_index;
  }

  LOG(FATAL) << "TBD: Not supported " << cid->name();

  return key_index;
}

std::vector<Transaction::PerShardCache>& Transaction::TLTmpSpace::GetShardIndex(unsigned size) {
  shard_cache.resize(size);
  for (auto& v : shard_cache)
    v.Clear();
  return shard_cache;
}

}  // namespace dfly
