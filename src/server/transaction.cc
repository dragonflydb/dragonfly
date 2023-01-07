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

[[maybe_unused]] constexpr size_t kTransSize = sizeof(Transaction);

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
Transaction::Transaction(const CommandId* cid) : cid_(cid) {
  string_view cmd_name(cid_->name());
  if (cmd_name == "EXEC" || cmd_name == "EVAL" || cmd_name == "EVALSHA") {
    multi_.reset(new Multi);
    multi_->multi_opts = cid->opt_mask();

    if (cmd_name == "EVAL" || cmd_name == "EVALSHA") {
      multi_->is_expanding = false;  // we lock all the keys at once.
    }
  }
}

Transaction::~Transaction() {
  DVLOG(2) << "Transaction " << StrCat(Name(), "@", txid_, "/", unique_shard_cnt_, ")")
           << " destroyed";
}

/**
 *
 * There are 4 options that we consider here:
 * a. T spans a single shard and its not multi.
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

OpStatus Transaction::InitByArgs(DbIndex index, CmdArgList args) {
  db_index_ = index;
  cmd_with_full_args_ = args;

  if (IsGlobal()) {
    unique_shard_cnt_ = shard_set->size();
    shard_data_.resize(unique_shard_cnt_);
    return OpStatus::OK;
  }

  CHECK_GT(args.size(), 1U);  // first entry is the command name.
  DCHECK_EQ(unique_shard_cnt_, 0u);
  DCHECK(args_.empty());

  OpResult<KeyIndex> key_index_res = DetermineKeys(cid_, args);
  if (!key_index_res)
    return key_index_res.status();

  const auto& key_index = *key_index_res;

  if (key_index.start == args.size()) {  // eval with 0 keys.
    CHECK(absl::StartsWith(cid_->name(), "EVAL"));
    return OpStatus::OK;
  }

  DCHECK_LT(key_index.start, args.size());
  DCHECK_GT(key_index.start, 0u);

  bool incremental_locking = multi_ && multi_->is_expanding;
  bool single_key = !multi_ && key_index.HasSingleKey();
  bool needs_reverse_mapping = cid_->opt_mask() & CO::REVERSE_MAPPING;

  if (single_key) {
    DCHECK_GT(key_index.step, 0u);

    shard_data_.resize(1);  // Single key optimization

    // even for a single key we may have multiple arguments per key (MSET).
    for (unsigned j = key_index.start; j < key_index.start + key_index.step; ++j) {
      args_.push_back(ArgS(args, j));
    }
    string_view key = args_.front();

    unique_shard_cnt_ = 1;
    unique_shard_id_ = Shard(key, shard_set->size());

    if (needs_reverse_mapping) {
      reverse_index_.resize(args_.size());
      for (unsigned j = 0; j < reverse_index_.size(); ++j) {
        reverse_index_[j] = j + key_index.start - 1;
      }
    }
    return OpStatus::OK;
  }

  // Our shard_data is not sparse, so we must allocate for all threads :(
  shard_data_.resize(shard_set->size());
  CHECK(key_index.step == 1 || key_index.step == 2);
  DCHECK(key_index.step == 1 || (args.size() % 2) == 1);

  // Reuse thread-local temporary storage. Since this code is atomic we can use it here.
  auto& shard_index = tmp_space.shard_cache;
  shard_index.resize(shard_data_.size());
  for (auto& v : shard_index) {
    v.Clear();
  }

  // TODO: to determine correctly locking mode for transactions, scripts
  // and regular commands.
  IntentLock::Mode mode = IntentLock::EXCLUSIVE;
  bool should_record_locks = false;

  if (multi_) {
    mode = Mode();
    multi_->keys.clear();
    tmp_space.uniq_keys.clear();
    DCHECK_LT(int(mode), 2);

    // With EVAL, we call this function for EVAL itself as well as for each command
    // for eval. currently, we lock everything only during the eval call.
    should_record_locks = incremental_locking || !multi_->locks_recorded;
  }

  if (key_index.bonus) {  // additional one-of key.
    DCHECK(key_index.step == 1);

    string_view key = ArgS(args, key_index.bonus);
    uint32_t sid = Shard(key, shard_data_.size());
    shard_index[sid].args.push_back(key);
    if (needs_reverse_mapping)
      shard_index[sid].original_index.push_back(key_index.bonus - 1);
  }

  for (unsigned i = key_index.start; i < key_index.end; ++i) {
    string_view key = ArgS(args, i);
    uint32_t sid = Shard(key, shard_data_.size());

    shard_index[sid].args.push_back(key);
    if (needs_reverse_mapping)
      shard_index[sid].original_index.push_back(i - 1);

    if (should_record_locks && tmp_space.uniq_keys.insert(key).second) {
      if (multi_->is_expanding) {
        multi_->keys.push_back(key);
      } else {
        multi_->locks[key].cnt[int(mode)]++;
      }
    };

    if (key_index.step == 2) {  // value
      ++i;

      string_view val = ArgS(args, i);
      shard_index[sid].args.push_back(val);
      if (needs_reverse_mapping)
        shard_index[sid].original_index.push_back(i - 1);
    }
  }

  if (multi_) {
    multi_->locks_recorded = true;
  }

  args_.resize(key_index.num_args());

  // we need reverse index only for some commands (MSET etc).
  if (needs_reverse_mapping)
    reverse_index_.resize(args_.size());

  auto next_arg = args_.begin();
  auto rev_indx_it = reverse_index_.begin();

  // slice.arg_start/arg_count point to args_ array which is sorted according to shard of each key.
  // reverse_index_[i] says what's the original position of args_[i] in args.
  for (size_t i = 0; i < shard_data_.size(); ++i) {
    auto& sd = shard_data_[i];
    auto& si = shard_index[i];

    CHECK_LT(si.args.size(), 1u << 15);

    sd.arg_count = si.args.size();
    sd.arg_start = next_arg - args_.begin();

    // We reset the local_mask for incremental locking to allow locking of arguments
    // for each operation within the same transaction. For instant locking we lock at
    // the beginning all the keys so we must preserve the mask to avoid double locking.
    if (incremental_locking) {
      sd.local_mask = 0;
    }

    if (!sd.arg_count)
      continue;

    ++unique_shard_cnt_;
    unique_shard_id_ = i;
    for (size_t j = 0; j < si.args.size(); ++j) {
      *next_arg = si.args[j];
      if (needs_reverse_mapping) {
        *rev_indx_it++ = si.original_index[j];
      }
      ++next_arg;
    }
  }

  CHECK(next_arg == args_.end());
  DVLOG(1) << "InitByArgs " << DebugId() << " " << args_.front();

  // validation
  if (needs_reverse_mapping) {
    for (size_t i = 0; i < args_.size(); ++i) {
      DCHECK_EQ(args_[i], ArgS(args, 1 + reverse_index_[i]));  // 1 for the commandname.
    }
  }

  if (unique_shard_cnt_ == 1) {
    PerShardData* sd;
    if (multi_) {
      sd = &shard_data_[unique_shard_id_];
    } else {
      shard_data_.resize(1);
      sd = &shard_data_.front();
    }
    sd->arg_count = -1;
    sd->arg_start = -1;
  }

  // Validation.
  for (const auto& sd : shard_data_) {
    // sd.local_mask may be non-zero for multi transactions with instant locking.
    // Specifically EVALs may maintain state between calls.
    DCHECK_EQ(0, sd.local_mask & ARMED);
    if (!multi_) {
      DCHECK_EQ(TxQueue::kEnd, sd.pq_pos);
    }
  }

  return OpStatus::OK;
}

void Transaction::SetExecCmd(const CommandId* cid) {
  DCHECK(multi_);
  DCHECK(!cb_);

  // The order is important, we call Schedule for multi transaction before overriding cid_.
  // TODO: The flow is ugly. Consider introducing a proper interface for Multi transactions
  // like SetupMulti/TurndownMulti. We already have UnlockMulti that should be part of
  // TurndownMulti.
  if (txid_ == 0) {
    ScheduleInternal();
  }

  unique_shard_cnt_ = 0;
  args_.clear();
  cid_ = cid;
  cb_ = nullptr;
}

string Transaction::DebugId() const {
  DCHECK_GT(use_count_.load(memory_order_relaxed), 0u);

  return StrCat(Name(), "@", txid_, "/", unique_shard_cnt_, " (", trans_id(this), ")");
}

// Runs in the dbslice thread. Returns true if transaction needs to be kept in the queue.
bool Transaction::RunInShard(EngineShard* shard) {
  DCHECK_GT(run_count_.load(memory_order_relaxed), 0u);
  CHECK(cb_) << DebugId();
  DCHECK_GT(txid_, 0u);

  // Unlike with regular transactions we do not acquire locks upon scheduling
  // because Scheduling is done before multi-exec batch is executed. Therefore we
  // lock keys right before the execution of each statement.

  VLOG(2) << "RunInShard: " << DebugId() << " sid:" << shard->shard_id();

  unsigned idx = SidToId(shard->shard_id());
  auto& sd = shard_data_[idx];

  DCHECK(sd.local_mask & ARMED);
  sd.local_mask &= ~ARMED;

  bool was_suspended = sd.local_mask & SUSPENDED_Q;
  bool awaked_prerun = (sd.local_mask & AWAKED_Q) != 0;
  bool incremental_lock = multi_ && multi_->is_expanding;

  // For multi we unlock transaction (i.e. its keys) in UnlockMulti() call.
  // Therefore we differentiate between concluding, which says that this specific
  // runnable concludes current operation, and should_release which tells
  // whether we should unlock the keys. should_release is false for multi and
  // equal to concluding otherwise.
  bool should_release = (coordinator_state_ & COORD_EXEC_CONCLUDING) && !multi_;
  IntentLock::Mode mode = Mode();

  // We make sure that we lock exactly once for each (multi-hop) transaction inside
  // transactions that lock incrementally.
  if (incremental_lock && ((sd.local_mask & KEYLOCK_ACQUIRED) == 0)) {
    DCHECK(!awaked_prerun);  // we should not have blocking transaction inside multi block.

    sd.local_mask |= KEYLOCK_ACQUIRED;
    shard->db_slice().Acquire(mode, GetLockArgs(idx));
  }

  DCHECK(IsGlobal() || (sd.local_mask & KEYLOCK_ACQUIRED));

  /*************************************************************************/
  // Actually running the callback.
  // If you change the logic here, also please change the logic
  try {
    // if transaction is suspended (blocked in watched queue), then it's a noop.
    OpStatus status = OpStatus::OK;

    if (!was_suspended) {
      status = cb_(this, shard);
    }

    if (unique_shard_cnt_ == 1) {
      cb_ = nullptr;  // We can do it because only a single thread runs the callback.
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

  if (!was_suspended && should_release)  // Check last hop & non suspended.
    LogJournalOnShard(shard);

  // at least the coordinator thread owns the reference.
  DCHECK_GE(use_count(), 1u);

  // we remove tx from tx-queue upon first invocation.
  // if it needs to run again it runs via a dedicated continuation_trans_ state in EngineShard.
  if (sd.pq_pos != TxQueue::kEnd) {
    shard->txq()->Remove(sd.pq_pos);
    sd.pq_pos = TxQueue::kEnd;
  }

  // If it's a final hop we should release the locks.
  if (should_release) {
    bool become_suspended = sd.local_mask & SUSPENDED_Q;

    if (IsGlobal()) {
      DCHECK(!awaked_prerun && !become_suspended);  // Global transactions can not be blocking.
      shard->shard_lock()->Release(Mode());
    } else {  // not global.
      KeyLockArgs largs = GetLockArgs(idx);
      DCHECK(sd.local_mask & KEYLOCK_ACQUIRED);

      // If a transaction has been suspended, we keep the lock so that future transaction
      // touching those keys will be ordered via TxQueue. It's necessary because we preserve
      // the atomicity of awaked transactions by halting the TxQueue.
      if (was_suspended || !become_suspended) {
        shard->db_slice().Release(mode, largs);
        sd.local_mask &= ~KEYLOCK_ACQUIRED;

        if (was_suspended || (sd.local_mask & AWAKED_Q)) {
          shard->blocking_controller()->RemoveWatched(this);
        }
      }
      sd.local_mask &= ~OUT_OF_ORDER;
    }
    // It has 2 responsibilities.
    // 1: to go over potential wakened keys, verify them and activate watch queues.
    // 2: if this transaction was notified and finished running - to remove it from the head
    //    of the queue and notify the next one.
    // RunStep is also called for global transactions because of commands like MOVE.
    if (shard->blocking_controller()) {
      shard->blocking_controller()->RunStep(awaked_prerun ? this : nullptr);
    }
  }

  CHECK_GE(DecreaseRunCnt(), 1u);
  // From this point on we can not access 'this'.

  return !should_release;  // keep
}

void Transaction::ScheduleInternal() {
  DCHECK(!shard_data_.empty());
  DCHECK_EQ(0u, txid_);
  DCHECK_EQ(0, coordinator_state_ & (COORD_SCHED | COORD_OOO));

  bool span_all = IsGlobal();
  bool single_hop = (coordinator_state_ & COORD_EXEC_CONCLUDING);

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
  } else {
    num_shards = unique_shard_cnt_;
    DCHECK_GT(num_shards, 0u);

    is_active = [&](uint32_t i) {
      return num_shards == 1 ? (i == unique_shard_id_) : shard_data_[i].arg_count > 0;
    };
  }

  while (true) {
    txid_ = op_seq.fetch_add(1, memory_order_relaxed);

    atomic_uint32_t lock_granted_cnt{0};
    atomic_uint32_t success{0};

    time_now_ms_ = GetCurrentTimeMs();

    auto cb = [&](EngineShard* shard) {
      pair<bool, bool> res = ScheduleInShard(shard);
      success.fetch_add(res.first, memory_order_relaxed);
      lock_granted_cnt.fetch_add(res.second, memory_order_relaxed);
    };

    shard_set->RunBriefInParallel(std::move(cb), is_active);

    if (success.load(memory_order_acquire) == num_shards) {
      // We allow out of order execution only for single hop transactions.
      // It might be possible to do it for multi-hop transactions as well but currently is
      // too complicated to reason about.
      if (single_hop && lock_granted_cnt.load(memory_order_relaxed) == num_shards) {
        // OOO can not happen with span-all transactions. We ensure it in ScheduleInShard when we
        // refuse to acquire locks for these transactions..
        DCHECK(!span_all);
        coordinator_state_ |= COORD_OOO;
      }
      VLOG(2) << "Scheduled " << DebugId()
              << " OutOfOrder: " << bool(coordinator_state_ & COORD_OOO)
              << " num_shards: " << num_shards;

      if (mode == IntentLock::EXCLUSIVE) {
        journal::Journal* j = ServerState::tlocal()->journal();
        // TODO: we may want to pass custom command name into journal.
        if (j && j->SchedStartTx(txid_, 0, num_shards)) {
        }
      }
      coordinator_state_ |= COORD_SCHED;
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

void Transaction::LockMulti() {
  DCHECK(multi_ && multi_->is_expanding);

  IntentLock::Mode mode = Mode();
  for (auto key : multi_->keys) {
    multi_->locks[key].cnt[int(mode)]++;
  }
  multi_->keys.clear();
}

// Optimized "Schedule and execute" function for the most common use-case of a single hop
// transactions like set/mset/mget etc. Does not apply for more complicated cases like RENAME or
// BLPOP where a data must be read from multiple shards before performing another hop.
OpStatus Transaction::ScheduleSingleHop(RunnableType cb) {
  DCHECK(!cb_);

  cb_ = std::move(cb);

  // single hop -> concluding.
  coordinator_state_ |= (COORD_EXEC | COORD_EXEC_CONCLUDING);

  if (!multi_) {  // for non-multi transactions we schedule exactly once.
    DCHECK_EQ(0, coordinator_state_ & COORD_SCHED);
  }

  bool schedule_fast = (unique_shard_cnt_ == 1) && !IsGlobal() && !multi_;
  if (schedule_fast) {  // Single shard (local) optimization.
    // We never resize shard_data because that would affect MULTI transaction correctness.
    DCHECK_EQ(1u, shard_data_.size());

    shard_data_[0].local_mask |= ARMED;

    // memory_order_release because we do not want it to be reordered with shard_data writes
    // above.
    // IsArmedInShard() first checks run_count_ before accessing shard_data.
    run_count_.fetch_add(1, memory_order_release);
    time_now_ms_ = GetCurrentTimeMs();

    // Please note that schedule_cb can not update any data on ScheduleSingleHop stack
    // since the latter can exit before ScheduleUniqueShard returns.
    // The problematic flow is as follows: ScheduleUniqueShard schedules into TxQueue and then
    // call PollExecute that runs the callback which calls DecreaseRunCnt.
    // As a result WaitForShardCallbacks below is unblocked.
    auto schedule_cb = [this] {
      bool run_eager = ScheduleUniqueShard(EngineShard::tlocal());
      if (run_eager) {
        // it's important to DecreaseRunCnt only for run_eager and after run_eager was assigned.
        // If DecreaseRunCnt were called before ScheduleUniqueShard finishes
        // then WaitForShardCallbacks below could exit before schedule_cb assigns return value
        // to run_eager and cause stack corruption.
        CHECK_GE(DecreaseRunCnt(), 1u);
      }
    };

    shard_set->Add(unique_shard_id_, std::move(schedule_cb));  // serves as a barrier.
  } else {
    // Transaction spans multiple shards or it's global (like flushdb) or multi.
    // Note that the logic here is a bit different from the public Schedule() function.
    if (multi_) {
      if (multi_->is_expanding)
        LockMulti();
    } else {
      ScheduleInternal();
    }

    ExecuteAsync();
  }

  DVLOG(1) << "ScheduleSingleHop before Wait " << DebugId() << " " << run_count_.load();
  WaitForShardCallbacks();
  DVLOG(1) << "ScheduleSingleHop after Wait " << DebugId();

  cb_ = nullptr;

  return local_result_;
}

// Runs in the coordinator fiber.
void Transaction::UnlockMulti() {
  VLOG(1) << "UnlockMulti " << DebugId();

  DCHECK(multi_);
  using KeyList = vector<pair<std::string_view, LockCnt>>;
  vector<KeyList> sharded_keys(shard_set->size());

  // It's LE and not EQ because there may be callbacks in progress that increase use_count_.
  DCHECK_LE(1u, use_count());

  for (const auto& k_v : multi_->locks) {
    ShardId sid = Shard(k_v.first, sharded_keys.size());
    sharded_keys[sid].push_back(k_v);
  }

  uint32_t prev = run_count_.fetch_add(shard_data_.size(), memory_order_relaxed);
  DCHECK_EQ(prev, 0u);

  for (ShardId i = 0; i < shard_data_.size(); ++i) {
    shard_set->Add(i, [&] { UnlockMultiShardCb(sharded_keys, EngineShard::tlocal()); });
  }
  WaitForShardCallbacks();
  DCHECK_GE(use_count(), 1u);

  VLOG(1) << "UnlockMultiEnd " << DebugId();
}

void Transaction::Schedule() {
  if (multi_ && multi_->is_expanding) {
    LockMulti();
  } else {
    ScheduleInternal();
  }
}

// Runs in coordinator thread.
void Transaction::Execute(RunnableType cb, bool conclude) {
  DCHECK(coordinator_state_ & COORD_SCHED);

  cb_ = std::move(cb);
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

  cb_ = nullptr;
}

// Runs in coordinator thread.
void Transaction::ExecuteAsync() {
  DVLOG(1) << "ExecuteAsync " << DebugId();

  DCHECK_GT(unique_shard_cnt_, 0u);
  DCHECK_GT(use_count_.load(memory_order_relaxed), 0u);

  // We do not necessarily Execute this transaction in 'cb' below. It well may be that it will be
  // executed by the engine shard once it has been armed and coordinator thread will finish the
  // transaction before engine shard thread stops accessing it. Therefore, we increase reference
  // by number of callbacks accessesing 'this' to allow callbacks to execute shard->Execute(this);
  // safely.
  use_count_.fetch_add(unique_shard_cnt_, memory_order_relaxed);

  bool is_global = IsGlobal();

  if (unique_shard_cnt_ == 1) {
    shard_data_[SidToId(unique_shard_id_)].local_mask |= ARMED;
  } else {
    for (ShardId i = 0; i < shard_data_.size(); ++i) {
      auto& sd = shard_data_[i];
      if (!is_global && sd.arg_count == 0)
        continue;
      DCHECK_LT(sd.arg_count, 1u << 15);
      sd.local_mask |= ARMED;
    }
  }

  uint32_t seq = seqlock_.load(memory_order_relaxed);

  // this fence prevents that a read or write operation before a release fence will be reordered
  // with a write operation after a release fence. Specifically no writes below will be reordered
  // upwards. Important, because it protects non-threadsafe local_mask from being accessed by
  // IsArmedInShard in other threads.
  run_count_.store(unique_shard_cnt_, memory_order_release);

  // We verify seq lock has the same generation number. See below for more info.
  auto cb = [seq, this] {
    EngineShard* shard = EngineShard::tlocal();

    uint16_t local_mask = GetLocalMask(shard->shard_id());

    // we use fetch_add with release trick to make sure that local_mask is loaded before
    // we load seq_after. We could gain similar result with "atomic_thread_fence(acquire)"
    uint32_t seq_after = seqlock_.fetch_add(0, memory_order_release);
    bool should_poll = (seq_after == seq) && (local_mask & ARMED);

    DVLOG(2) << "PollExecCb " << DebugId() << " sid(" << shard->shard_id() << ") "
             << run_count_.load(memory_order_relaxed) << ", should_poll: " << should_poll;

    // We verify that this callback is still relevant.
    // If we still have the same sequence number and local_mask is ARMED it means
    // the coordinator thread has not crossed WaitForShardCallbacks barrier.
    // Otherwise, this callback is redundant. We may still call PollExecution but
    // we should not pass this to it since it can be in undefined state for this callback.
    if (should_poll) {
      // shard->PollExecution(this) does not necessarily execute this transaction.
      // Therefore, everything that should be handled during the callback execution
      // should go into RunInShard.
      shard->PollExecution("exec_cb", this);
    }

    DVLOG(2) << "ptr_release " << DebugId() << " " << seq;
    intrusive_ptr_release(this);  // against use_count_.fetch_add above.
  };

  // IsArmedInShard is the protector of non-thread safe data.
  if (!is_global && unique_shard_cnt_ == 1) {
    shard_set->Add(unique_shard_id_, std::move(cb));  // serves as a barrier.
  } else {
    for (ShardId i = 0; i < shard_data_.size(); ++i) {
      auto& sd = shard_data_[i];
      if (!is_global && sd.arg_count == 0)
        continue;
      shard_set->Add(i, cb);  // serves as a barrier.
    }
  }
}

void Transaction::RunQuickie(EngineShard* shard) {
  DCHECK(!multi_);
  DCHECK_EQ(1u, shard_data_.size());
  DCHECK_EQ(0u, txid_);

  shard->IncQuickRun();

  auto& sd = shard_data_[0];
  DCHECK_EQ(0, sd.local_mask & (KEYLOCK_ACQUIRED | OUT_OF_ORDER));

  DVLOG(1) << "RunQuickSingle " << DebugId() << " " << shard->shard_id() << " " << args_[0];
  CHECK(cb_) << DebugId() << " " << shard->shard_id() << " " << args_[0];

  // Calling the callback in somewhat safe way
  try {
    local_result_ = cb_(this, shard);
  } catch (std::bad_alloc&) {
    LOG_FIRST_N(ERROR, 16) << " out of memory";
    local_result_ = OpStatus::OUT_OF_MEMORY;
  } catch (std::exception& e) {
    LOG(FATAL) << "Unexpected exception " << e.what();
  }

  LogJournalOnShard(shard);

  sd.local_mask &= ~ARMED;
  cb_ = nullptr;  // We can do it because only a single shard runs the callback.
}

// runs in coordinator thread.
// Marks the transaction as expired and removes it from the waiting queue.
void Transaction::ExpireBlocking() {
  DVLOG(1) << "ExpireBlocking " << DebugId();
  DCHECK(!IsGlobal());

  run_count_.store(unique_shard_cnt_, memory_order_release);

  auto expire_cb = [this] { ExpireShardCb(EngineShard::tlocal()); };

  if (unique_shard_cnt_ == 1) {
    DCHECK_LT(unique_shard_id_, shard_set->size());
    shard_set->Add(unique_shard_id_, move(expire_cb));
  } else {
    for (ShardId i = 0; i < shard_data_.size(); ++i) {
      auto& sd = shard_data_[i];
      DCHECK_EQ(0, sd.local_mask & ARMED);
      if (sd.arg_count == 0)
        continue;

      shard_set->Add(i, expire_cb);
    }
  }

  // Wait for all callbacks to conclude.
  WaitForShardCallbacks();
  DVLOG(1) << "ExpireBlocking finished " << DebugId();
}

const char* Transaction::Name() const {
  return cid_->name();
}

KeyLockArgs Transaction::GetLockArgs(ShardId sid) const {
  KeyLockArgs res;
  res.db_index = db_index_;
  res.key_step = cid_->key_arg_step();
  res.args = ShardArgsInShard(sid);

  return res;
}

// Runs within a engine shard thread.
// Optimized path that schedules and runs transactions out of order if possible.
// Returns true if was eagerly executed, false if it was scheduled into queue.
bool Transaction::ScheduleUniqueShard(EngineShard* shard) {
  DCHECK(!multi_);
  DCHECK_EQ(0u, txid_);
  DCHECK_EQ(1u, shard_data_.size());

  auto mode = Mode();
  auto lock_args = GetLockArgs(shard->shard_id());

  auto& sd = shard_data_.front();
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
  DCHECK(!shard_data_.empty());

  // schedule_success, lock_granted.
  pair<bool, bool> result{false, false};

  if (shard->committed_txid() >= txid_) {
    return result;
  }

  TxQueue* txq = shard->txq();
  KeyLockArgs lock_args;
  IntentLock::Mode mode = Mode();

  bool spans_all = IsGlobal();
  bool lock_granted = false;
  ShardId sid = SidToId(shard->shard_id());

  auto& sd = shard_data_[sid];

  if (!spans_all) {
    bool shard_unlocked = shard->shard_lock()->Check(mode);
    lock_args = GetLockArgs(shard->shard_id());

    // we need to acquire the lock unrelated to shard_unlocked since we register into Tx queue.
    // All transactions in the queue must acquire the intent lock.
    lock_granted = shard->db_slice().Acquire(mode, lock_args) && shard_unlocked;
    sd.local_mask |= KEYLOCK_ACQUIRED;
    DVLOG(1) << "Lock granted " << lock_granted << " for trans " << DebugId();
  }

  if (!txq->Empty()) {
    // If the new transaction requires reordering of the pending queue (i.e. it comes before tail)
    // and some other transaction already locked its keys we can not reorder 'trans' because
    // that other transaction could have deduced that it can run OOO and eagerly execute. Hence, we
    // fail this scheduling attempt for trans.
    // However, when we schedule span-all transactions we can still reorder them. The reason is
    // before we start scheduling them we lock the shards and disable OOO.
    // We may record when they disable OOO via barrier_ts so if the queue contains transactions
    // that were only scheduled afterwards we know they are not free so we can still
    // reorder the queue. Currently, this optimization is disabled: barrier_ts < pq->HeadScore().
    bool to_proceed = lock_granted || txq->TailScore() < txid_;
    if (!to_proceed) {
      if (sd.local_mask & KEYLOCK_ACQUIRED) {  // rollback the lock.
        shard->db_slice().Release(mode, lock_args);
        sd.local_mask &= ~KEYLOCK_ACQUIRED;
      }

      return result;  // false, false
    }
  }

  result.second = lock_granted;
  result.first = true;

  TxQueue::Iterator it = txq->Insert(this);
  DCHECK_EQ(TxQueue::kEnd, sd.pq_pos);
  sd.pq_pos = it;

  DVLOG(1) << "Insert into tx-queue, sid(" << sid << ") " << DebugId() << ", qlen " << txq->size();

  return result;
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
    shard->db_slice().Release(mode, lock_args);
    sd.local_mask &= ~KEYLOCK_ACQUIRED;
  }

  if (pos == head && !txq->Empty()) {
    return true;
  }

  return false;
}

// runs in engine-shard thread.
ArgSlice Transaction::ShardArgsInShard(ShardId sid) const {
  DCHECK(!args_.empty());

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
  if (unique_shard_cnt_ == 1)  // mget: 0->0, 1->1. zunionstore has 0->2
    return reverse_index_[arg_index];

  const auto& sd = shard_data_[shard_id];
  return reverse_index_[sd.arg_start + arg_index];
}

bool Transaction::WaitOnWatch(const time_point& tp) {
  // Assumes that transaction is pending and scheduled. TODO: To verify it with state machine.
  VLOG(2) << "WaitOnWatch Start use_count(" << use_count() << ")";
  using namespace chrono;

  Execute([](Transaction* t, EngineShard* shard) { return t->AddToWatchedShardCb(shard); }, true);

  coordinator_state_ |= COORD_BLOCKED;

  auto wake_cb = [this] {
    return (coordinator_state_ & COORD_CANCELLED) ||
           notify_txid_.load(memory_order_relaxed) != kuint64max;
  };

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

  if ((coordinator_state_ & COORD_CANCELLED) || status == cv_status::timeout) {
    ExpireBlocking();
    coordinator_state_ &= ~COORD_BLOCKED;
    return false;
  }

#if 0
  // We were notified by a shard, so lets make sure that our notifications converged to a stable
  // form.
  if (unique_shard_cnt_ > 1) {
    run_count_.store(unique_shard_cnt_, memory_order_release);

    auto converge_cb = [this] {
      this->CheckForConvergence(EngineShard::tlocal());
    };

    for (ShardId i = 0; i < shard_data_.size(); ++i) {
      auto& sd = shard_data_[i];
      DCHECK_EQ(0, sd.local_mask & ARMED);
      if (sd.arg_count == 0)
        continue;
      shard_set->Add(i, converge_cb);
    }

    // Wait for all callbacks to conclude.
    WaitForShardCallbacks();
    DVLOG(1) << "Convergence finished " << DebugId();
  }
#endif

  // Lift blocking mask.
  coordinator_state_ &= ~COORD_BLOCKED;

  return true;
}

// Runs only in the shard thread.
OpStatus Transaction::AddToWatchedShardCb(EngineShard* shard) {
  ShardId idx = SidToId(shard->shard_id());

  auto& sd = shard_data_[idx];
  CHECK_EQ(0, sd.local_mask & SUSPENDED_Q);
  DCHECK_EQ(0, sd.local_mask & ARMED);

  shard->AddBlocked(this);
  sd.local_mask |= SUSPENDED_Q;
  DVLOG(1) << "AddWatched " << DebugId() << " local_mask:" << sd.local_mask;

  return OpStatus::OK;
}

void Transaction::ExpireShardCb(EngineShard* shard) {
  auto lock_args = GetLockArgs(shard->shard_id());
  shard->db_slice().Release(Mode(), lock_args);

  unsigned sd_idx = SidToId(shard->shard_id());
  auto& sd = shard_data_[sd_idx];
  sd.local_mask |= EXPIRED_Q;
  sd.local_mask &= ~KEYLOCK_ACQUIRED;

  shard->blocking_controller()->RemoveWatched(this);

  // Need to see why I decided to call this.
  // My guess - probably to trigger the run of stalled transactions in case
  // this shard concurrently awoke this transaction and stalled the processing
  // of TxQueue.
  shard->PollExecution("expirecb", nullptr);

  CHECK_GE(DecreaseRunCnt(), 1u);
}

void Transaction::UnlockMultiShardCb(const std::vector<KeyList>& sharded_keys, EngineShard* shard) {
  if (multi_->multi_opts & CO::GLOBAL_TRANS) {
    shard->shard_lock()->Release(IntentLock::EXCLUSIVE);
  }

  ShardId sid = shard->shard_id();
  for (const auto& k_v : sharded_keys[sid]) {
    auto release = [&](IntentLock::Mode mode) {
      if (k_v.second.cnt[mode]) {
        shard->db_slice().Release(mode, db_index_, k_v.first, k_v.second.cnt[mode]);
      }
    };

    release(IntentLock::SHARED);
    release(IntentLock::EXCLUSIVE);
  }

  auto& sd = shard_data_[SidToId(shard->shard_id())];

  // It does not have to be that all shards in multi transaction execute this tx.
  // Hence it could stay in the tx queue. We perform the necessary cleanup and remove it from
  // there.
  if (sd.pq_pos != TxQueue::kEnd) {
    DVLOG(1) << "unlockmulti: TxPopFront " << DebugId();

    TxQueue* txq = shard->txq();
    DCHECK(!txq->Empty());
    Transaction* trans = absl::get<Transaction*>(txq->Front());
    DCHECK(trans == this);
    txq->PopFront();
    sd.pq_pos = TxQueue::kEnd;
  }

  shard->ShutdownMulti(this);

  // notify awakened transactions.
  if (shard->blocking_controller())
    shard->blocking_controller()->RunStep(nullptr);
  shard->PollExecution("unlockmulti", nullptr);

  this->DecreaseRunCnt();
}

#if 0
// HasResultConverged has detailed documentation on how convergence is determined.
void Transaction::CheckForConvergence(EngineShard* shard) {
  unsigned idx = SidToId(shard->shard_id());
  auto& sd = shard_data_[idx];

  TxId notify = notify_txid();

  if ((sd.local_mask & AWAKED_Q) || shard->HasResultConverged(notify)) {
    CHECK_GE(DecreaseRunCnt(), 1u);
    return;
  }

  LOG(DFATAL) << "TBD";

  BlockingController* bc = shard->blocking_controller();
  CHECK(bc);  // must be present because we have watched this shard before.

  bc->RegisterAwaitForConverge(this);
}
#endif

inline uint32_t Transaction::DecreaseRunCnt() {
  // to protect against cases where Transaction is destroyed before run_ec_.notify
  // finishes running. We can not put it inside the (res == 1) block because then it's too late.
  ::boost::intrusive_ptr guard(this);

  // We use release so that no stores will be reordered after.
  uint32_t res = run_count_.fetch_sub(1, memory_order_release);
  if (res == 1) {
    run_ec_.notify();
  }
  return res;
}

bool Transaction::IsGlobal() const {
  return (cid_->opt_mask() & CO::GLOBAL_TRANS) != 0;
}

// Runs only in the shard thread.
// Returns true if the transacton has changed its state from suspended to awakened,
// false, otherwise.
bool Transaction::NotifySuspended(TxId committed_txid, ShardId sid) {
  unsigned idx = SidToId(sid);
  auto& sd = shard_data_[idx];
  unsigned local_mask = sd.local_mask;

  if (local_mask & Transaction::EXPIRED_Q) {
    return false;
  }

  DVLOG(1) << "NotifySuspended " << DebugId() << ", local_mask:" << local_mask << " by "
           << committed_txid;

  // local_mask could be awaked (i.e. not suspended) if the transaction has been
  // awakened by another key or awakened by the same key multiple times.
  if (local_mask & SUSPENDED_Q) {
    DCHECK_EQ(0u, local_mask & AWAKED_Q);

    sd.local_mask &= ~SUSPENDED_Q;
    sd.local_mask |= AWAKED_Q;

    TxId notify_id = notify_txid_.load(memory_order_relaxed);

    while (committed_txid < notify_id) {
      if (notify_txid_.compare_exchange_weak(notify_id, committed_txid, memory_order_relaxed)) {
        // if we improved notify_txid_ - break.
        blocking_ec_.notify();  // release barrier.
        break;
      }
    }
    return true;
  }

  CHECK(sd.local_mask & AWAKED_Q);
  return false;
}

void Transaction::LogJournalOnShard(EngineShard* shard) {
  // TODO: For now, we ignore non shard coordination.
  if (shard == nullptr)
    return;

  // Ignore custom journal or non-write commands.
  if ((cid_->opt_mask() & CO::NO_AUTOJOURNAL) > 0 || (cid_->opt_mask() & CO::WRITE) == 0)
    return;

  auto journal = shard->journal();
  if (journal == nullptr)
    return;

  // TODO: Handle complex commands like LMPOP correctly once they are implemented.
  journal::Entry::Payload entry_payload;
  if (unique_shard_cnt_ == 1 || args_.empty()) {
    CHECK(!cmd_with_full_args_.empty());
    entry_payload = cmd_with_full_args_;
  } else {
    auto cmd = facade::ToSV(cmd_with_full_args_.front());
    entry_payload = make_pair(cmd, ShardArgsInShard(shard->shard_id()));
  }
  journal->RecordEntry(txid_, db_index_, std::move(entry_payload), unique_shard_cnt_);
}

void Transaction::BreakOnShutdown() {
  if (coordinator_state_ & COORD_BLOCKED) {
    coordinator_state_ |= COORD_CANCELLED;
    blocking_ec_.notify();
  }
}

OpResult<KeyIndex> DetermineKeys(const CommandId* cid, CmdArgList args) {
  DCHECK_EQ(0u, cid->opt_mask() & CO::GLOBAL_TRANS);

  KeyIndex key_index;

  int num_custom_keys = -1;

  if (cid->opt_mask() & CO::VARIADIC_KEYS) {
    if (args.size() < 3) {
      return OpStatus::SYNTAX_ERR;
    }

    string_view name{cid->name()};

    if (!absl::StartsWith(name, "EVAL")) {
      key_index.bonus = 1;  // Z<xxx>STORE commands
    }
    string_view num(ArgS(args, 2));
    if (!absl::SimpleAtoi(num, &num_custom_keys) || num_custom_keys < 0)
      return OpStatus::INVALID_INT;

    if (size_t(num_custom_keys) + 3 > args.size())
      return OpStatus::SYNTAX_ERR;
  }

  if (cid->first_key_pos() > 0) {
    key_index.start = cid->first_key_pos();
    int last = cid->last_key_pos();
    if (num_custom_keys >= 0) {
      key_index.end = key_index.start + num_custom_keys;
    } else {
      key_index.end = last > 0 ? last + 1 : (int(args.size()) + 1 + last);
    }
    key_index.step = cid->key_arg_step();

    return key_index;
  }

  LOG(FATAL) << "TBD: Not supported " << cid->name();

  return key_index;
}

}  // namespace dfly
