// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/transaction.h"

#include "base/logging.h"
#include "server/command_registry.h"
#include "server/db_slice.h"
#include "server/engine_shard_set.h"

namespace dfly {

using namespace std;
using namespace util;

thread_local Transaction::TLTmpSpace Transaction::tmp_space;

namespace {

std::atomic_uint64_t op_seq{1};

constexpr size_t kTransSize = sizeof(Transaction);


}  // namespace


IntentLock::Mode Transaction::Mode() const {
  return (trans_options_ & CO::READONLY) ? IntentLock::SHARED : IntentLock::EXCLUSIVE;
}

Transaction::~Transaction() {
  DVLOG(2) << "Transaction " << DebugId() << " destroyed";
}

/**
 * @brief Construct a new Transaction:: Transaction object
 *
 * @param cid
 * @param ess
 * @param cs
 */
Transaction::Transaction(const CommandId* cid, EngineShardSet* ess) : cid_(cid), ess_(ess) {
  trans_options_ = cid_->opt_mask();

  bool single_key = cid_->first_key_pos() > 0 && !cid_->is_multi_key();
  if (single_key) {
    dist_.shard_data.resize(1);  // Single key optimization
  } else {
    // Our shard_data is not sparse, so we must allocate for all threads :(
    dist_.shard_data.resize(ess_->size());
  }
}

/**
 *
 * There are 4 options that we consider here:
 * a. T spans a single shard and its not multi.
 *    unique_shard_id_ is predefined before the schedule() is called.
 *    In that case only a single thread will be scheduled and it will use shard_data[0] just becase
 *    shard_data.size() = 1. Engine thread can access any data because there is schedule barrier
 *    between InitByArgs and RunInShard/IsArmedInShard functions.
 * b. T  spans multiple shards and its not multi
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

void Transaction::InitByArgs(CmdArgList args) {
  CHECK_GT(args.size(), 1U);
  CHECK_LT(size_t(cid_->first_key_pos()), args.size());
  DCHECK_EQ(unique_shard_cnt_, 0u);

  if (!cid_->is_multi_key()) {  // Single key optimization.
    auto key = ArgS(args, cid_->first_key_pos());
    args_.push_back(key);

    unique_shard_cnt_ = 1;
    unique_shard_id_ = Shard(key, ess_->size());
    num_keys_ = 1;
    return;
  }

  CHECK(cid_->key_arg_step() == 1 || cid_->key_arg_step() == 2);
  DCHECK(cid_->key_arg_step() == 1 || (args.size() % 2) == 1);

  // Reuse thread-local temporary storage. Since this code is non-preemptive we can use it here.
  auto& shard_index = tmp_space.shard_cache;
  shard_index.resize(dist_.shard_data.size());
  for (auto& v : shard_index) {
    v.Clear();
  }

  size_t key_end = cid_->last_key_pos() > 0 ? cid_->last_key_pos() + 1
                                            : (args.size() + 1 + cid_->last_key_pos());
  for (size_t i = 1; i < key_end; ++i) {
    std::string_view key = ArgS(args, i);
    uint32_t sid = Shard(key, dist_.shard_data.size());
    shard_index[sid].args.push_back(key);
    shard_index[sid].original_index.push_back(i - 1);
    ++num_keys_;

    if (cid_->key_arg_step() == 2) {  // value
      ++i;
      auto val = ArgS(args, i);
      shard_index[sid].args.push_back(val);
      shard_index[sid].original_index.push_back(i - 1);
    }
  }

  args_.resize(key_end - 1);
  dist_.reverse_index.resize(args_.size());

  auto next_arg = args_.begin();
  auto rev_indx_it = dist_.reverse_index.begin();

  // slice.arg_start/arg_count point to args_ array which is sorted according to shard of each key.
  // reverse_index_[i] says what's the original position of args_[i] in args.
  for (size_t i = 0; i < dist_.shard_data.size(); ++i) {
    auto& sd = dist_.shard_data[i];
    auto& si = shard_index[i];
    CHECK_LT(si.args.size(), 1u << 15);
    sd.arg_count = si.args.size();
    sd.arg_start = next_arg - args_.begin();
    sd.local_mask = 0;
    if (!sd.arg_count)
      continue;

    ++unique_shard_cnt_;
    unique_shard_id_ = i;
    uint32_t orig_indx = 0;
    for (size_t j = 0; j < si.args.size(); ++j) {
      *next_arg = si.args[j];
      *rev_indx_it = si.original_index[orig_indx];

      ++next_arg;
      ++orig_indx;
      ++rev_indx_it;
    }
  }

  CHECK(next_arg == args_.end());
  DVLOG(1) << "InitByArgs " << DebugId();

  if (unique_shard_cnt_ == 1) {
    PerShardData* sd;

    dist_.shard_data.resize(1);
    sd = &dist_.shard_data.front();
    sd->arg_count = -1;
    sd->arg_start = -1;
  }

  // Validation.
  for (const auto& sd : dist_.shard_data) {
    DCHECK_EQ(sd.local_mask, 0u);
    DCHECK_EQ(0, sd.local_mask & ARMED);
    DCHECK_EQ(TxQueue::kEnd, sd.pq_pos);
  }
}

string Transaction::DebugId() const {
  return absl::StrCat(Name(), "@", txid_, "/", unique_shard_cnt_, " (", trans_id(this), ")");
}

// Runs in the dbslice thread. Returns true if transaction needs to be kept in the queue.
bool Transaction::RunInShard(ShardId sid) {
  CHECK(cb_);
  DCHECK_GT(txid_, 0u);

  EngineShard* shard = EngineShard::tlocal();

  // Unlike with regular transactions we do not acquire locks upon scheduling
  // because Scheduling is done before multi-exec batch is executed. Therefore we
  // lock keys right before the execution of each statement.

  DVLOG(1) << "RunInShard: " << DebugId() << " sid:" << sid;

  sid = TranslateSidInShard(sid);
  auto& sd = dist_.shard_data[sid];
  DCHECK(sd.local_mask & ARMED);
  sd.local_mask &= ~ARMED;

  bool concluding = dist_.is_concluding_cb;

  DCHECK(sd.local_mask & KEYS_ACQUIRED);

  // Actually running the callback.
  OpStatus status = cb_(this, shard);

  // If it's a final hop we should release the locks.
  if (concluding) {

    auto largs = GetLockArgs(sid);
    shard->db_slice().Release(Mode(), largs);
    sd.local_mask &= ~KEYS_ACQUIRED;
  }

  if (unique_shard_cnt_ == 1) {
    cb_ = nullptr;  // We can do it because only a single thread runs the callback.
    local_result_ = status;
  } else {
    CHECK_EQ(OpStatus::OK, status);
  }

  // This shard should own a reference for transaction as well as coordinator thread.
  DCHECK_GT(use_count(), 1u);
  CHECK_GE(Disarm(), 1u);

  // must be computed before intrusive_ptr_release call.
  if (concluding) {
    sd.pq_pos = TxQueue::kEnd;
    // For multi-transaction we need to clear this flag to allow locking of the next set of keys
    // during the next child transaction.
    sd.local_mask &= ~KEYS_ACQUIRED;
    DVLOG(2) << "ptr_release " << DebugId() << " " << this->use_count();

    intrusive_ptr_release(this);  // Against ScheduleInternal.
  }

  return !concluding;  // keep
}

void Transaction::ScheduleInternal(bool single_hop) {
  DCHECK_EQ(0, state_mask_.load(memory_order_acquire) & SCHEDULED);
  DCHECK_EQ(0u, txid_);

  uint32_t num_shards;
  std::function<bool(uint32_t)> is_active;

  num_shards = unique_shard_cnt_;
  DCHECK_GT(num_shards, 0u);

  is_active = [&](uint32_t i) {
    return num_shards == 1 ? (i == unique_shard_id_) : dist_.shard_data[i].arg_count > 0;
  };

  // intrusive_ptr_add num_shards times.
  use_count_.fetch_add(num_shards, memory_order_relaxed);

  while (true) {
    txid_ = op_seq.fetch_add(1, std::memory_order_relaxed);

    std::atomic_uint32_t lock_acquire_cnt{0};
    std::atomic_uint32_t success{0};

    auto cb = [&](EngineShard* shard) {
      pair<bool, bool> res = ScheduleInShard(shard);
      success.fetch_add(res.first, memory_order_relaxed);
      lock_acquire_cnt.fetch_add(res.second, memory_order_relaxed);
    };

    ess_->RunBriefInParallel(std::move(cb), is_active);

    if (success.load(memory_order_acquire) == num_shards) {
      // We allow out of order execution only for single hop transactions.
      // It might be possible to do it for multi-hop transactions as well but currently is
      // too complicated to reason about.
      if (single_hop && lock_acquire_cnt.load(memory_order_relaxed) == num_shards) {
        dist_.out_of_order.store(true, memory_order_relaxed);
      }
      DVLOG(1) << "Scheduled " << DebugId() << " OutOfOrder: " << dist_.out_of_order;

      state_mask_.fetch_or(SCHEDULED, memory_order_release);
      break;
    }

    DVLOG(1) << "Cancelling " << DebugId();

    auto cancel = [&](EngineShard* shard) {
      success.fetch_sub(CancelInShard(shard), memory_order_relaxed);
    };

    ess_->RunBriefInParallel(std::move(cancel), is_active);
    CHECK_EQ(0u, success.load(memory_order_relaxed));
  }
}

// Optimized "Schedule and execute" function for the most common use-case of a single hop
// transactions like set/mset/mget etc. Does not apply for more complicated cases like RENAME or
// BLPOP where a data must be read from multiple shards before performing another hop.
OpStatus Transaction::ScheduleSingleHop(RunnableType cb) {
  DCHECK(!cb_);

  cb_ = std::move(cb);

  bool run_eager = false;
  bool schedule_fast = (unique_shard_cnt_ == 1);
  if (schedule_fast) {  // Single shard (local) optimization.
    // We never resize shard_data because that would affect MULTI transaction correctness.
    DCHECK_EQ(1u, dist_.shard_data.size());

    dist_.shard_data[0].local_mask |= ARMED;
    arm_count_.fetch_add(1, memory_order_release);  // Decreases in RunLocal.
    auto schedule_cb = [&] { return ScheduleUniqueShard(EngineShard::tlocal()); };
    run_eager = ess_->Await(unique_shard_id_, std::move(schedule_cb));  // serves as a barrier.
    (void)run_eager;
  } else {  // Transaction spans multiple shards or it's global (like flushdb)
    ScheduleInternal(true);
    ExecuteAsync(true);
  }

  DVLOG(1) << "Before DoneWait " << DebugId() << " " << args_.front();
  WaitArm();
  DVLOG(1) << "After DoneWait";

  cb_ = nullptr;
  state_mask_.fetch_or(AFTERRUN, memory_order_release);

  return local_result_;
}

// Runs in coordinator thread.
void Transaction::Execute(RunnableType cb, bool conclude) {
  cb_ = std::move(cb);

  ExecuteAsync(conclude);

  DVLOG(1) << "Wait on " << DebugId();
  WaitArm();
  DVLOG(1) << "Wait on " << DebugId() << " completed";
  cb_ = nullptr;
  dist_.out_of_order.store(false, memory_order_relaxed);

  uint32_t mask = conclude ? AFTERRUN : RUNNING;
  state_mask_.fetch_or(mask, memory_order_release);
}

// Runs in coordinator thread.
void Transaction::ExecuteAsync(bool concluding_cb) {
  DVLOG(1) << "ExecuteAsync " << DebugId() << " concluding " << concluding_cb;

  dist_.is_concluding_cb = concluding_cb;

  DCHECK_GT(unique_shard_cnt_, 0u);

  // We do not necessarily Execute this transaction in 'cb' below. It well may be that it will be
  // executed by the engine shard once it has been armed and coordinator thread will finish the
  // transaction before engine shard thread stops accessing it. Therefore, we increase reference
  // by number of callbacks accessesing 'this' to allow callbacks to execute shard->Execute(this);
  // safely.
  use_count_.fetch_add(unique_shard_cnt_, memory_order_relaxed);

  if (unique_shard_cnt_ == 1) {
    dist_.shard_data[TranslateSidInShard(unique_shard_id_)].local_mask |= ARMED;
  } else {
    for (ShardId i = 0; i < dist_.shard_data.size(); ++i) {
      auto& sd = dist_.shard_data[i];
      if (sd.arg_count == 0)
        continue;
      DCHECK_LT(sd.arg_count, 1u << 15);
      sd.local_mask |= ARMED;
    }
  }

  // this fence prevents that a read or write operation before a release fence will be reordered
  // with a write operation after a release fence. Specifically no writes below will be reordered
  // upwards. Important, because it protects non-threadsafe local_mask from being accessed by
  // IsArmedInShard in other threads.
  arm_count_.fetch_add(unique_shard_cnt_, memory_order_acq_rel);

  auto cb = [this] {
    EngineShard* shard = EngineShard::tlocal();
    DVLOG(2) << "TriggerExec " << DebugId() << " sid:" << shard->shard_id();

    // Everything that should be handled during the callback execution should go into RunInShard.
    shard->Execute(this);

    DVLOG(2) << "ptr_release " << DebugId() << " " << use_count();
    intrusive_ptr_release(this);  // against use_count_.fetch_add above.
  };

  // IsArmedInShard is the protector of non-thread safe data.
  if (unique_shard_cnt_ == 1) {
    ess_->Add(unique_shard_id_, std::move(cb));  // serves as a barrier.
  } else {
    for (ShardId i = 0; i < dist_.shard_data.size(); ++i) {
      auto& sd = dist_.shard_data[i];
      if (sd.arg_count == 0)
        continue;
      ess_->Add(i, cb);  // serves as a barrier.
    }
  }
}

void Transaction::RunQuickSingle() {
  DCHECK_EQ(1u, dist_.shard_data.size());
  DCHECK_EQ(0u, txid_);

  EngineShard* shard = EngineShard::tlocal();
  auto& sd = dist_.shard_data[0];
  DCHECK_EQ(0, sd.local_mask & KEYS_ACQUIRED);

  DVLOG(1) << "RunQuickSingle " << DebugId() << " " << shard->shard_id() << " " << args_[0];
  CHECK(cb_) << DebugId() << " " << shard->shard_id() << " " << args_[0];

  local_result_ = cb_(this, shard);

  sd.local_mask &= ~ARMED;
  cb_ = nullptr;  // We can do it because only a single shard runs the callback.
  CHECK_GE(Disarm(), 1u);
}

const char* Transaction::Name() const {
  return cid_->name();
}

KeyLockArgs Transaction::GetLockArgs(ShardId sid) const {
  KeyLockArgs res;
  res.db_index = 0;  // TODO
  res.key_step = cid_->key_arg_step();
  res.args = ShardArgsInShard(sid);

  return res;
}

// Runs within a engine shard thread.
// Optimized path that schedules and runs transactions out of order if possible.
// Returns true if was eagerly executed, false if it was scheduled into queue.
bool Transaction::ScheduleUniqueShard(EngineShard* shard) {
  DCHECK_EQ(0u, txid_);
  DCHECK_EQ(1u, dist_.shard_data.size());

  auto mode = Mode();
  auto lock_args = GetLockArgs(shard->shard_id());

  auto& sd = dist_.shard_data.front();
  DCHECK_EQ(TxQueue::kEnd, sd.pq_pos);

  // Fast path - for uncontended keys, just run the callback.
  // That applies for single key operations like set, get, lpush etc.
  if (shard->db_slice().CheckLock(mode, lock_args)) {
    RunQuickSingle();  // TODO: for journal - this can become multi-shard
                       // transaction on replica.
    return true;
  }

  intrusive_ptr_add_ref(this);

  // we can do it because only a single thread writes into txid_ and sd.
  txid_ = op_seq.fetch_add(1, std::memory_order_relaxed);
  TxQueue::Iterator it = shard->InsertTxQ(this);
  sd.pq_pos = it;

  DCHECK_EQ(0, sd.local_mask & KEYS_ACQUIRED);
  bool lock_acquired = shard->db_slice().Acquire(mode, lock_args);
  sd.local_mask |= KEYS_ACQUIRED;
  DCHECK(!lock_acquired);  // Because CheckLock above failed.

  state_mask_.fetch_or(SCHEDULED, memory_order_release);

  return false;
}

// This function should not block since it's run via RunBriefInParallel.
pair<bool, bool> Transaction::ScheduleInShard(EngineShard* shard) {
  // schedule_success, lock_granted.
  pair<bool, bool> result{false, false};

  if (shard->committed_txid() >= txid_) {
    return result;
  }

  TxQueue* pq = shard->txq();
  KeyLockArgs lock_args;
  IntentLock::Mode mode = Mode();

  bool lock_granted = false;
  ShardId sid = TranslateSidInShard(shard->shard_id());

  auto& sd = dist_.shard_data[sid];

  bool shard_unlocked = true;
  lock_args = GetLockArgs(shard->shard_id());

  // we need to acquire the lock unrelated to shard_unlocked since we register into Tx queue.
  // All transactions in the queue must acquire the intent lock.
  lock_granted = shard->db_slice().Acquire(mode, lock_args) && shard_unlocked;
  sd.local_mask |= KEYS_ACQUIRED;
  DVLOG(1) << "Lock granted " << lock_granted << " for trans " << DebugId();

  if (!pq->Empty()) {
    // If the new transaction requires reordering of the pending queue (i.e. it comes before tail)
    // and some other transaction already locked its keys we can not reorder 'trans' because
    // that other transaction could have deduced that it can run OOO and eagerly execute. Hence, we
    // fail this scheduling attempt for trans.
    // However, when we schedule span-all transactions we can still reorder them. The reason is
    // before we start scheduling them we lock the shards and disable OOO.
    // We may record when they disable OOO via barrier_ts so if the queue contains transactions
    // that were only scheduled afterwards we know they are not free so we can still
    // reorder the queue. Currently, this optimization is disabled: barrier_ts < pq->HeadRank().
    bool to_proceed = lock_granted || pq->TailScore() < txid_;
    if (!to_proceed) {
      if (sd.local_mask & KEYS_ACQUIRED) {  // rollback the lock.
        shard->db_slice().Release(mode, lock_args);
        sd.local_mask &= ~KEYS_ACQUIRED;
      }

      return result;  // false, false
    }
  }

  result.second = lock_granted;
  result.first = true;

  TxQueue::Iterator it = pq->Insert(this);
  DCHECK_EQ(TxQueue::kEnd, sd.pq_pos);
  sd.pq_pos = it;

  DVLOG(1) << "Insert into tx-queue, sid(" << sid << ") " << DebugId() << ", qlen " << pq->size();

  return result;
}

bool Transaction::CancelInShard(EngineShard* shard) {
  ShardId sid = TranslateSidInShard(shard->shard_id());
  auto& sd = dist_.shard_data[sid];

  auto pos = sd.pq_pos;
  if (pos == TxQueue::kEnd)
    return false;

  sd.pq_pos = TxQueue::kEnd;

  TxQueue* pq = shard->txq();
  auto val = pq->At(pos);
  Transaction* trans = absl::get<Transaction*>(val);
  DCHECK(trans == this) << "Pos " << pos << ", pq size " << pq->size() << ", trans " << trans;
  pq->Remove(pos);

  if (sd.local_mask & KEYS_ACQUIRED) {
    auto mode = Mode();
    auto lock_args = GetLockArgs(shard->shard_id());
    shard->db_slice().Release(mode, lock_args);
    sd.local_mask &= ~KEYS_ACQUIRED;
  }
  return true;
}

// runs in engine-shard thread.
ArgSlice Transaction::ShardArgsInShard(ShardId sid) const {
  DCHECK(!args_.empty());
  DCHECK_NOTNULL(EngineShard::tlocal());

  // We can read unique_shard_cnt_  only because ShardArgsInShard is called after IsArmedInShard
  // barrier.
  if (unique_shard_cnt_ == 1) {
    return args_;
  }

  const auto& sd = dist_.shard_data[sid];
  return ArgSlice{args_.data() + sd.arg_start, sd.arg_count};
}

size_t Transaction::ReverseArgIndex(ShardId shard_id, size_t arg_index) const {
  if (unique_shard_cnt_ == 1)
    return arg_index;

  return dist_.reverse_index[dist_.shard_data[shard_id].arg_start + arg_index];
}

}  // namespace dfly
