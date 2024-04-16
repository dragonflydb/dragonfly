// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/transaction.h"

#include <absl/strings/match.h>

#include "base/logging.h"
#include "facade/op_status.h"
#include "redis/redis_aux.h"
#include "server/blocking_controller.h"
#include "server/command_registry.h"
#include "server/common.h"
#include "server/db_slice.h"
#include "server/engine_shard_set.h"
#include "server/journal/journal.h"
#include "server/server_state.h"

ABSL_FLAG(uint32_t, tx_queue_warning_len, 96,
          "Length threshold for warning about long transaction queue");

namespace dfly {

using namespace std;
using namespace util;
using absl::StrCat;

thread_local Transaction::TLTmpSpace Transaction::tmp_space;

namespace {

// Global txid sequence
atomic_uint64_t op_seq{1};

constexpr size_t kTransSize [[maybe_unused]] = sizeof(Transaction);

void AnalyzeTxQueue(const EngineShard* shard, const TxQueue* txq) {
  unsigned q_limit = absl::GetFlag(FLAGS_tx_queue_warning_len);
  if (txq->size() > q_limit) {
    static thread_local time_t last_log_time = 0;
    // TODO: glog provides LOG_EVERY_T, which uses precise clock.
    // We should introduce inside helio LOG_PERIOD_ATLEAST macro that takes seconds and
    // uses low precision clock.
    time_t now = time(nullptr);
    if (now >= last_log_time + 10) {
      last_log_time = now;
      EngineShard::TxQueueInfo info = shard->AnalyzeTxQueue();
      string msg =
          StrCat("TxQueue is too long. Tx count:", info.tx_total, ", armed:", info.tx_armed,
                 ", runnable:", info.tx_runnable, ", total locks: ", info.total_locks,
                 ", contended locks: ", info.contended_locks, "\n");
      absl::StrAppend(&msg, "max contention score: ", info.max_contention_score,
                      ", lock: ", info.max_contention_lock,
                      ", poll_executions:", shard->stats().poll_execution_total);
      const Transaction* cont_tx = shard->GetContTx();
      if (cont_tx) {
        absl::StrAppend(&msg, " continuation_tx: ", cont_tx->DebugId(), " ",
                        cont_tx->DEBUG_IsArmedInShard(shard->shard_id()) ? " armed" : "");
      }

      LOG(WARNING) << msg;
    }
  }
}

void RecordTxScheduleStats(const Transaction* tx) {
  auto* ss = ServerState::tlocal();
  ++(tx->IsGlobal() ? ss->stats.tx_global_cnt : ss->stats.tx_normal_cnt);
  ++ss->stats.tx_width_freq_arr[tx->GetUniqueShardCnt() - 1];
}

std::ostream& operator<<(std::ostream& os, Transaction::time_point tp) {
  using namespace chrono;
  if (tp == Transaction::time_point::max())
    return os << "inf";
  size_t ms = duration_cast<milliseconds>(tp - Transaction::time_point::clock::now()).count();
  return os << ms << "ms";
}

uint16_t trans_id(const Transaction* ptr) {
  return (intptr_t(ptr) >> 8) & 0xFFFF;
}

bool CheckLocks(const DbSlice& db_slice, IntentLock::Mode mode, const KeyLockArgs& lock_args) {
  for (size_t i = 0; i < lock_args.args.size(); i += lock_args.key_step) {
    string_view s = lock_args.args[i];
    if (!db_slice.CheckLock(mode, lock_args.db_index, s))
      return false;
  }
  return true;
}

}  // namespace

bool Transaction::BatonBarrier::IsClaimed() const {
  return claimed_.load(memory_order_relaxed);
}

bool Transaction::BatonBarrier::TryClaim() {
  return !claimed_.exchange(true, memory_order_relaxed);  // false means first means success
}

void Transaction::BatonBarrier::Close() {
  DCHECK(claimed_.load(memory_order_relaxed));
  closed_.store(true, memory_order_relaxed);
  ec_.notify();  // release
}

cv_status Transaction::BatonBarrier::Wait(time_point tp) {
  auto cb = [this] { return closed_.load(memory_order_acquire); };

  if (tp != time_point::max()) {
    // Wait until timepoint and return immediately if we finished without a timeout
    if (ec_.await_until(cb, tp) == cv_status::no_timeout)
      return cv_status::no_timeout;

    // We timed out and claimed the barrier, so no one will be able to claim it anymore
    if (TryClaim()) {
      closed_.store(true, memory_order_relaxed);  // Purely formal
      return cv_status::timeout;
    }

    // fallthrough: otherwise a modification is in progress, wait for it below
  }

  ec_.await(cb);
  return cv_status::no_timeout;
}

Transaction::Transaction(const CommandId* cid) : cid_{cid} {
  InitTxTime();
  string_view cmd_name(cid_->name());
  if (cmd_name == "EXEC" || cmd_name == "EVAL" || cmd_name == "EVALSHA") {
    multi_.reset(new MultiData);
    multi_->shard_journal_write.resize(shard_set->size(), false);

    multi_->mode = NOT_DETERMINED;
    multi_->role = DEFAULT;
  }
}

Transaction::Transaction(const Transaction* parent, ShardId shard_id, std::optional<SlotId> slot_id)
    : multi_{make_unique<MultiData>()},
      txid_{parent->txid()},
      unique_shard_cnt_{1},
      unique_shard_id_{shard_id} {
  if (parent->multi_) {
    multi_->mode = parent->multi_->mode;
  } else {
    // Use squashing mechanism for inline execution of single-shard EVAL
    multi_->mode = LOCK_AHEAD;
  }

  multi_->role = SQUASHED_STUB;
  multi_->shard_journal_write.resize(1);

  MultiUpdateWithParent(parent);
  if (slot_id.has_value()) {
    unique_slot_checker_.Add(*slot_id);
  }
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
  stats_.coordinator_index = ProactorBase::me() ? ProactorBase::me()->GetPoolIndex() : kInvalidSid;
}

void Transaction::InitGlobal() {
  DCHECK(!multi_ || (multi_->mode == GLOBAL || multi_->mode == NON_ATOMIC));

  global_ = true;
  EnableAllShards();
}

void Transaction::BuildShardIndex(const KeyIndex& key_index, std::vector<PerShardCache>* out) {
  auto args = full_args_;

  auto& shard_index = *out;

  auto add = [this, rev_mapping = key_index.has_reverse_mapping, &shard_index](uint32_t sid,
                                                                               uint32_t i) {
    string_view val = ArgS(full_args_, i);
    shard_index[sid].args.push_back(val);
    if (rev_mapping)
      shard_index[sid].original_index.push_back(i);
  };

  if (key_index.bonus) {
    DCHECK(key_index.step == 1);
    string_view key = ArgS(args, *key_index.bonus);
    unique_slot_checker_.Add(key);
    uint32_t sid = Shard(key, shard_data_.size());
    add(sid, *key_index.bonus);
  }

  for (unsigned i = key_index.start; i < key_index.end; ++i) {
    string_view key = ArgS(args, i);
    unique_slot_checker_.Add(key);
    uint32_t sid = Shard(key, shard_data_.size());
    add(sid, i);

    DCHECK_LE(key_index.step, 2u);
    if (key_index.step == 2) {  // Handle value associated with preceding key.
      add(sid, ++i);
    }
  }
}

void Transaction::InitShardData(absl::Span<const PerShardCache> shard_index, size_t num_args,
                                bool rev_mapping) {
  kv_args_.reserve(num_args);
  if (rev_mapping)
    reverse_index_.reserve(num_args);

  // Store the concatenated per-shard arguments from the shard index inside kv_args_
  // and make each shard data point to its own sub-span inside kv_args_.
  for (size_t i = 0; i < shard_data_.size(); ++i) {
    auto& sd = shard_data_[i];
    const auto& si = shard_index[i];

    sd.arg_count = si.args.size();
    sd.arg_start = kv_args_.size();

    // Multi transactions can re-initialize on different shards, so clear ACTIVE flag.
    DCHECK_EQ(sd.local_mask & ACTIVE, 0);

    if (sd.arg_count == 0)
      continue;

    sd.local_mask |= ACTIVE;

    unique_shard_cnt_++;
    unique_shard_id_ = i;

    for (size_t j = 0; j < si.args.size(); ++j) {
      kv_args_.push_back(si.args[j]);
      if (rev_mapping)
        reverse_index_.push_back(si.original_index[j]);
    }
  }

  DCHECK_EQ(kv_args_.size(), num_args);
}

void Transaction::LaunderKeyStorage(CmdArgVec* keys) {
  DCHECK_EQ(multi_->mode, LOCK_AHEAD);
  DCHECK_GT(keys->size(), 0u);

  auto& m_keys = multi_->frozen_keys;
  auto& m_keys_set = multi_->frozen_keys_set;

  // Reserve enough space, so pointers from frozen_keys_set are not invalidated
  m_keys.reserve(keys->size());

  for (MutableSlice key : *keys) {
    string_view key_s = string_view(LockTag{facade::ToSV(key)});
    // Insert copied string view, not original. This is why "try insert" is not allowed
    if (!m_keys_set.contains(key_s))
      m_keys_set.insert(m_keys.emplace_back(key_s));
  }

  // Copy mutable pointers into keys
  keys->clear();
  for (string& key : m_keys)
    keys->emplace_back(key.data(), key.size());
}

void Transaction::StoreKeysInArgs(const KeyIndex& key_index) {
  DCHECK(!key_index.bonus);
  DCHECK(key_index.step == 1u || key_index.step == 2u);

  // even for a single key we may have multiple arguments per key (MSET).
  for (unsigned j = key_index.start; j < key_index.end; j++) {
    kv_args_.push_back(ArgS(full_args_, j));
    if (key_index.step == 2)
      kv_args_.push_back(ArgS(full_args_, ++j));
  }

  if (key_index.has_reverse_mapping) {
    reverse_index_.resize(kv_args_.size());
    for (unsigned j = 0; j < reverse_index_.size(); ++j) {
      reverse_index_[j] = j + key_index.start;
    }
  }
}

void Transaction::InitByKeys(const KeyIndex& key_index) {
  if (key_index.start == full_args_.size()) {  // eval with 0 keys.
    CHECK(absl::StartsWith(cid_->name(), "EVAL")) << cid_->name();
    return;
  }

  DCHECK_LT(key_index.start, full_args_.size());

  // Stub transactions always operate only on single shard.
  bool is_stub = multi_ && multi_->role == SQUASHED_STUB;

  if ((key_index.HasSingleKey() && !IsAtomicMulti()) || is_stub) {
    DCHECK(!IsActiveMulti() || multi_->mode == NON_ATOMIC);

    // We don't have to split the arguments by shards, so we can copy them directly.
    StoreKeysInArgs(key_index);

    unique_shard_cnt_ = 1;
    if (is_stub)  // stub transactions don't migrate
      DCHECK_EQ(unique_shard_id_, Shard(kv_args_.front(), shard_set->size()));
    else {
      unique_slot_checker_.Add(kv_args_.front());
      unique_shard_id_ = Shard(kv_args_.front(), shard_set->size());
    }

    // Multi transactions that execute commands on their own (not stubs) can't shrink the backing
    // array, as it still might be read by leftover callbacks.
    shard_data_.resize(IsActiveMulti() ? shard_set->size() : 1);
    shard_data_[SidToId(unique_shard_id_)].local_mask |= ACTIVE;

    return;
  }

  shard_data_.resize(shard_set->size());  // shard_data isn't sparse, so we must allocate for all :(
  DCHECK(key_index.step == 1 || key_index.step == 2);
  DCHECK(key_index.step != 2 || (full_args_.size() % 2) == 0);

  // Safe, because flow below is not preemptive.
  auto& shard_index = tmp_space.GetShardIndex(shard_data_.size());

  // Distribute all the arguments by shards.
  BuildShardIndex(key_index, &shard_index);

  // Initialize shard data based on distributed arguments.
  InitShardData(shard_index, key_index.num_args(), key_index.has_reverse_mapping);

  DCHECK(!multi_ || multi_->mode != LOCK_AHEAD || !multi_->frozen_keys.empty());

  DVLOG(1) << "InitByArgs " << DebugId() << " " << kv_args_.front();

  // Compress shard data, if we occupy only one shard.
  if (unique_shard_cnt_ == 1) {
    PerShardData* sd;
    if (IsActiveMulti()) {
      sd = &shard_data_[SidToId(unique_shard_id_)];
      DCHECK(sd->local_mask & ACTIVE);
    } else {
      shard_data_.resize(1);
      sd = &shard_data_.front();
      sd->local_mask |= ACTIVE;
    }
    sd->arg_count = -1;
    sd->arg_start = -1;
  }

  // Validation. Check reverse mapping was built correctly.
  if (key_index.has_reverse_mapping) {
    for (size_t i = 0; i < kv_args_.size(); ++i) {
      DCHECK_EQ(kv_args_[i], ArgS(full_args_, reverse_index_[i])) << full_args_;
    }
  }

  // Validation.
  for (const auto& sd : shard_data_) {
    // sd.local_mask may be non-zero for multi transactions with instant locking.
    // Specifically EVALs may maintain state between calls.
    DCHECK(!sd.is_armed.load(memory_order_relaxed));
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

  if ((cid_->opt_mask() & CO::NO_KEY_TRANSACTIONAL) > 0) {
    if ((cid_->opt_mask() & CO::NO_KEY_TX_SPAN_ALL) > 0)
      EnableAllShards();
    else
      EnableShard(0);
    return OpStatus::OK;
  }

  DCHECK_EQ(unique_shard_cnt_, 0u);
  DCHECK(kv_args_.empty());

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

  InitBase(db_index_, {});

  // Because squashing already determines active shards by partitioning commands,
  // we don't have to work with keys manually and can just mark active shards.
  // The partitioned commands know it's keys and assume they have correct access.
  DCHECK_EQ(shard_data_.size(), shard_set->size());
  for (unsigned i = 0; i < shard_data_.size(); i++) {
    if (enabled(i)) {
      shard_data_[i].local_mask |= ACTIVE;
      unique_shard_cnt_++;
      unique_shard_id_ = i;
    } else {
      shard_data_[i].local_mask &= ~ACTIVE;
    }
    shard_data_[i].arg_start = 0;
    shard_data_[i].arg_count = 0;
  }

  MultiBecomeSquasher();
}

void Transaction::StartMultiGlobal(DbIndex dbid) {
  CHECK(multi_);
  CHECK(shard_data_.empty());  // Make sure default InitByArgs didn't run.

  multi_->mode = GLOBAL;
  InitBase(dbid, {});
  InitGlobal();
  multi_->lock_mode = IntentLock::EXCLUSIVE;

  ScheduleInternal();
}

void Transaction::StartMultiLockedAhead(DbIndex dbid, CmdArgVec keys, bool skip_scheduling) {
  DVLOG(1) << "StartMultiLockedAhead on " << keys.size() << " keys";

  DCHECK(multi_);
  DCHECK(shard_data_.empty());  // Make sure default InitByArgs didn't run.

  multi_->mode = LOCK_AHEAD;
  multi_->lock_mode = LockMode();

  LaunderKeyStorage(&keys);  // Filter uniques and normalize

  InitBase(dbid, absl::MakeSpan(keys));
  InitByKeys(KeyIndex::Range(0, keys.size()));

  if (!skip_scheduling)
    ScheduleInternal();

  full_args_ = {nullptr, 0};  // InitBase set it to temporary keys, now we reset it.
}

void Transaction::StartMultiNonAtomic() {
  DCHECK(multi_);
  multi_->mode = NON_ATOMIC;
}

void Transaction::InitTxTime() {
  time_now_ms_ = GetCurrentTimeMs();
}

void Transaction::MultiSwitchCmd(const CommandId* cid) {
  DCHECK(multi_);
  DCHECK(!cb_ptr_);

  multi_->cmd_seq_num++;

  if (multi_->role != SQUASHED_STUB)  // stub transactions don't migrate between threads
    unique_shard_id_ = 0;
  unique_shard_cnt_ = 0;

  kv_args_.clear();
  reverse_index_.clear();

  cid_ = cid;
  cb_ptr_ = nullptr;

  for (auto& sd : shard_data_) {
    sd.arg_count = sd.arg_start = 0;

    if (multi_->mode == NON_ATOMIC) {
      sd.local_mask = 0;  // Non atomic transactions schedule each time, so remove all flags
      DCHECK_EQ(sd.pq_pos, TxQueue::kEnd);
    } else {
      DCHECK(IsAtomicMulti());   // Every command determines it's own active shards
      sd.local_mask &= ~ACTIVE;  // so remove ACTIVE flags, but keep KEYLOCK_ACQUIRED
    }
    DCHECK(!sd.is_armed.load(memory_order_relaxed));
  }

  if (multi_->mode == NON_ATOMIC) {
    coordinator_state_ = 0;
    txid_ = 0;
  } else if (multi_->role == SQUASHED_STUB) {
    DCHECK_EQ(coordinator_state_, 0u);
  }

  // Each hop needs to be prepared, reset role
  if (multi_->role == SQUASHER)
    multi_->role = DEFAULT;
}

void Transaction::MultiUpdateWithParent(const Transaction* parent) {
  // Disabled because of single shard lua optimization
  // DCHECK(multi_);
  // DCHECK(parent->multi_);  // it might not be a squasher yet, but certainly is multi
  DCHECK_EQ(multi_->role, SQUASHED_STUB);
  txid_ = parent->txid_;
  time_now_ms_ = parent->time_now_ms_;
  unique_slot_checker_ = parent->unique_slot_checker_;
}

void Transaction::MultiBecomeSquasher() {
  DCHECK(multi_->mode == GLOBAL || multi_->mode == LOCK_AHEAD);
  DCHECK_GT(GetUniqueShardCnt(), 0u);    // initialized and determined active shards
  DCHECK(cid_->IsMultiTransactional());  // proper base command set
  multi_->role = SQUASHER;
}

string Transaction::DebugId(std::optional<ShardId> sid) const {
  DCHECK_GT(use_count_.load(memory_order_relaxed), 0u);
  string res = StrCat(Name(), "@", txid_, "/", unique_shard_cnt_);
  if (multi_) {
    absl::StrAppend(&res, ":", multi_->cmd_seq_num);
  }
  absl::StrAppend(&res, " {id=", trans_id(this));
  if (sid) {
    absl::StrAppend(&res, ",mask[", *sid, "]=", int(shard_data_[SidToId(*sid)].local_mask),
                    ",txqpos[]=", shard_data_[SidToId(*sid)].pq_pos);
  }
  absl::StrAppend(&res, "}");
  return res;
}

void Transaction::PrepareMultiForScheduleSingleHop(ShardId sid, DbIndex db, CmdArgList args) {
  multi_.reset();
  InitBase(db, args);
  EnableShard(sid);
  OpResult<KeyIndex> key_index = DetermineKeys(cid_, args);
  CHECK(key_index);
  DCHECK(!key_index->has_reverse_mapping);
  StoreKeysInArgs(*key_index);
}

// Runs in the dbslice thread. Returns true if the transaction continues running in the thread.
bool Transaction::RunInShard(EngineShard* shard, bool txq_ooo) {
  DCHECK_GT(txid_, 0u);
  CHECK(cb_ptr_) << DebugId();

  unsigned idx = SidToId(shard->shard_id());
  auto& sd = shard_data_[idx];

  sd.stats.total_runs++;

  DCHECK_GT(run_barrier_.DEBUG_Count(), 0u);
  VLOG(2) << "RunInShard: " << DebugId() << " sid:" << shard->shard_id() << " " << sd.local_mask;

  bool was_suspended = sd.local_mask & SUSPENDED_Q;
  bool awaked_prerun = sd.local_mask & AWAKED_Q;

  IntentLock::Mode mode = LockMode();

  DCHECK(IsGlobal() || (sd.local_mask & KEYLOCK_ACQUIRED) || (multi_ && multi_->mode == GLOBAL));
  DCHECK(!txq_ooo || (sd.local_mask & OUT_OF_ORDER));

  /*************************************************************************/

  RunCallback(shard);

  /*************************************************************************/
  // at least the coordinator thread owns the reference.
  DCHECK_GE(GetUseCount(), 1u);

  bool is_concluding = coordinator_state_ & COORD_CONCLUDING;

  // If we're the head of tx queue (txq_ooo is false), we remove ourselves upon first invocation
  // and successive hops are run by continuation_trans_ in engine shard.
  // Otherwise we can remove ourselves only when we're concluding (so no more hops will follow).
  if ((is_concluding || !txq_ooo) && sd.pq_pos != TxQueue::kEnd) {
    VLOG(2) << "Remove from txq " << this->DebugId();
    shard->txq()->Remove(sd.pq_pos);
    sd.pq_pos = TxQueue::kEnd;
  }

  // For multi we unlock transaction (i.e. its keys) in UnlockMulti() call.
  // If it's a final hop we should release the locks.
  if (is_concluding) {
    bool became_suspended = sd.local_mask & SUSPENDED_Q;
    KeyLockArgs largs;

    if (IsGlobal()) {
      DCHECK(!awaked_prerun && !became_suspended);  // Global transactions can not be blocking.
      VLOG(2) << "Releasing shard lock";
      shard->shard_lock()->Release(LockMode());
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

    if (IsAtomicMulti()) {  // Can only be true if run through ScheduleSingleHop
      DCHECK(cid_->IsMultiTransactional());
      MultiReportJournalOnShard(shard);
    }

    // It has 2 responsibilities.
    // 1: to go over potential wakened keys, verify them and activate watch queues.
    // 2: if this transaction was notified and finished running - to remove it from the head
    //    of the queue and notify the next one.
    if (auto* bcontroller = shard->blocking_controller(); bcontroller) {
      if (awaked_prerun || was_suspended) {
        CHECK_EQ(largs.key_step, 1u);
        bcontroller->FinalizeWatched(GetShardArgs(idx), this);
      }

      // Wake only if no tx queue head is currently running
      // Note: RemoveContTx might have no effect above if this tx had no continuations
      if (shard->GetContTx() == nullptr) {
        bcontroller->NotifyPending();
      }
    }
  }

  FinishHop();  // From this point on we can not access 'this'.
  return !is_concluding;
}

void Transaction::RunCallback(EngineShard* shard) {
  DCHECK_EQ(shard, EngineShard::tlocal());

  RunnableResult result;
  try {
    result = (*cb_ptr_)(this, shard);

    if (unique_shard_cnt_ == 1) {
      cb_ptr_ = nullptr;  // We can do it because only a single thread runs the callback.
      local_result_ = result;
    } else {
      if (result == OpStatus::OUT_OF_MEMORY) {
        absl::base_internal::SpinLockHolder lk{&local_result_mu_};
        CHECK(local_result_ == OpStatus::OK || local_result_ == OpStatus::OUT_OF_MEMORY);
        local_result_ = result;
      } else {
        CHECK_EQ(OpStatus::OK, result);
      }
    }
  } catch (std::bad_alloc&) {
    LOG_FIRST_N(ERROR, 16) << " out of memory";  // TODO: to log at most once per sec.
    absl::base_internal::SpinLockHolder lk{&local_result_mu_};
    local_result_ = OpStatus::OUT_OF_MEMORY;
  } catch (std::exception& e) {
    LOG(FATAL) << "Unexpected exception " << e.what();
  }

  shard->db_slice().OnCbFinish();

  // Handle result flags to alter behaviour.
  if (result.flags & RunnableResult::AVOID_CONCLUDING) {
    // Multi shard callbacks should either all or none choose to conclude. They can't communicate,
    // so they must know their decision ahead, consequently there is no point in using this flag.
    CHECK_EQ(unique_shard_cnt_, 1u);
    DCHECK((coordinator_state_ & COORD_CONCLUDING) || multi_->concluding);
    coordinator_state_ &= ~COORD_CONCLUDING;
  }

  // Log to journal only once the command finished running
  if ((coordinator_state_ & COORD_CONCLUDING) || (multi_ && multi_->concluding))
    LogAutoJournalOnShard(shard, result);
}

// TODO: For multi-transactions we should be able to deduce mode() at run-time based
// on the context. For regular multi-transactions we can actually inspect all commands.
// For eval-like transactions - we can decide based on the command flavor (EVAL/EVALRO) or
// auto-tune based on the static analysis (by identifying commands with hardcoded command names).
void Transaction::ScheduleInternal() {
  DCHECK_EQ(txid_, 0u);
  DCHECK_EQ(coordinator_state_ & COORD_SCHED, 0);
  DCHECK_GT(unique_shard_cnt_, 0u);
  DCHECK(!IsAtomicMulti() || cid_->IsMultiTransactional());

  // Try running immediately (during scheduling) if we're concluding and either:
  // - have a single shard, and thus never have to cancel scheduling due to reordering
  // - run as an idempotent command, meaning we can safely repeat the operation if scheduling fails
  bool can_run_immediately = !IsGlobal() && (coordinator_state_ & COORD_CONCLUDING) &&
                             (unique_shard_cnt_ == 1 || (cid_->opt_mask() & CO::IDEMPOTENT));

  DVLOG(1) << "ScheduleInternal " << cid_->name() << " on " << unique_shard_cnt_ << " shards "
           << " immediate run: " << can_run_immediately;

  auto is_active = [this](uint32_t i) { return IsActive(i); };

  // Loop until successfully scheduled in all shards.
  while (true) {
    stats_.schedule_attempts++;

    // This is a contention point for all threads - avoid using it unless necessary.
    // Single shard operations can assign txid later if the immediate run failed.
    if (unique_shard_cnt_ > 1)
      txid_ = op_seq.fetch_add(1, memory_order_relaxed);

    InitTxTime();

    atomic_uint32_t schedule_fails = 0;
    auto cb = [this, &schedule_fails, can_run_immediately]() {
      if (!ScheduleInShard(EngineShard::tlocal(), can_run_immediately)) {
        schedule_fails.fetch_add(1, memory_order_relaxed);
      }
      run_barrier_.Dec();
    };

    run_barrier_.Start(unique_shard_cnt_);
    if (CanRunInlined()) {
      // single shard schedule operation can't fail
      CHECK(ScheduleInShard(EngineShard::tlocal(), can_run_immediately));
      run_barrier_.Dec();
    } else {
      IterateActiveShards([cb](const auto& sd, ShardId i) { shard_set->Add(i, cb); });
      run_barrier_.Wait();
    }

    if (schedule_fails.load(memory_order_relaxed) == 0) {
      coordinator_state_ |= COORD_SCHED;

      RecordTxScheduleStats(this);
      break;
    }

    VLOG(2) << "Cancelling " << DebugId();
    ServerState::tlocal()->stats.tx_schedule_cancel_cnt += 1;

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
      IterateActiveShards([](const auto& sd, auto i) {
        shard_set->Add(i, [] { EngineShard::tlocal()->PollExecution("cancel_cleanup", nullptr); });
      });
    }
  }
}

void Transaction::ReportWritesSquashedMulti(absl::FunctionRef<bool(ShardId)> had_write) {
  DCHECK(multi_);
  for (unsigned i = 0; i < multi_->shard_journal_write.size(); i++)
    multi_->shard_journal_write[i] |= had_write(i);

  // Update imemdiately if we decide to conclude after one hop without UnlockMulti
  multi_->shard_journal_cnt = CalcMultiNumOfShardJournals();
}

// Runs in the coordinator fiber.
void Transaction::UnlockMulti() {
  VLOG(1) << "UnlockMulti " << DebugId();
  DCHECK(multi_);
  DCHECK_GE(GetUseCount(), 1u);  // Greater-equal because there may be callbacks in progress.

  // Return if we either didn't schedule at all (and thus run) or already did conclude
  if ((coordinator_state_ & COORD_SCHED) == 0 || (coordinator_state_ & COORD_CONCLUDING) > 0)
    return;

  multi_->frozen_keys_set.clear();

  auto sharded_keys = make_shared<vector<vector<string_view>>>(shard_set->size());
  for (string& key : multi_->frozen_keys) {
    ShardId sid = Shard(key, sharded_keys->size());
    (*sharded_keys)[sid].emplace_back(key);
  }

  multi_->shard_journal_cnt = ServerState::tlocal()->journal() ? CalcMultiNumOfShardJournals() : 0;

  use_count_.fetch_add(shard_data_.size(), std::memory_order_relaxed);

  DCHECK_EQ(shard_data_.size(), shard_set->size());
  for (ShardId i = 0; i < shard_data_.size(); ++i) {
    shard_set->Add(i, [this, sharded_keys, i]() {
      this->UnlockMultiShardCb((*sharded_keys)[i], EngineShard::tlocal());
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

OpStatus Transaction::ScheduleSingleHop(RunnableType cb) {
  Execute(cb, true);
  return local_result_;
}

void Transaction::Schedule() {
  // no-op
}

// Runs in coordinator thread.
void Transaction::Execute(RunnableType cb, bool conclude) {
  if (multi_ && multi_->role == SQUASHED_STUB) {
    local_result_ = RunSquashedMultiCb(cb);
    return;
  }

  local_result_ = OpStatus::OK;
  cb_ptr_ = &cb;

  if (IsAtomicMulti()) {
    multi_->concluding = conclude;
  } else {
    coordinator_state_ = conclude ? (coordinator_state_ | COORD_CONCLUDING)
                                  : (coordinator_state_ & ~COORD_CONCLUDING);
  }

  if ((coordinator_state_ & COORD_SCHED) == 0) {
    ScheduleInternal();
  }

  DispatchHop();
  run_barrier_.Wait();
  cb_ptr_ = nullptr;

  if (coordinator_state_ & COORD_CONCLUDING)
    coordinator_state_ &= ~COORD_SCHED;
}

// Runs in coordinator thread.
void Transaction::DispatchHop() {
  DVLOG(1) << "DispatchHop " << DebugId();
  DCHECK_GT(unique_shard_cnt_, 0u);
  DCHECK_GT(use_count_.load(memory_order_relaxed), 0u);
  DCHECK(!IsAtomicMulti() || multi_->lock_mode.has_value());
  DCHECK_LE(shard_data_.size(), 1024u);

  // Hops can start executing immediately after being armed, so we
  // initialize the run barrier before arming, as well as copy indices
  // of active shards to avoid reading concurrently accessed shard data.
  std::bitset<1024> poll_flags(0);
  unsigned run_cnt = 0;
  IterateActiveShards([&poll_flags, &run_cnt](auto& sd, auto i) {
    if ((sd.local_mask & RAN_IMMEDIATELY) == 0) {
      run_cnt++;
      poll_flags.set(i, true);
    }
    sd.local_mask &= ~RAN_IMMEDIATELY;  // we'll run it next time if it avoided concluding
  });

  DCHECK_EQ(run_cnt, poll_flags.count());
  if (run_cnt == 0)  // all callbacks were run immediately
    return;

  run_barrier_.Start(run_cnt);

  // Set armed flags on all active shards.
  std::atomic_thread_fence(memory_order_release);  // once fence to avoid flushing writes in loop
  IterateActiveShards([&poll_flags](auto& sd, auto i) {
    if (poll_flags.test(i))
      sd.is_armed.store(true, memory_order_relaxed);
  });

  if (CanRunInlined()) {
    DCHECK_EQ(run_cnt, 1u);
    DVLOG(1) << "Short-circuit ExecuteAsync " << DebugId();
    EngineShard::tlocal()->PollExecution("exec_cb", this);
    return;
  }

  use_count_.fetch_add(run_cnt, memory_order_relaxed);  // for each pointer from poll_cb

  auto poll_cb = [this] {
    EngineShard::tlocal()->PollExecution("exec_cb", this);
    DVLOG(3) << "ptr_release " << DebugId();
    intrusive_ptr_release(this);  // against use_count_.fetch_add above.
  };
  IterateShards([&poll_cb, &poll_flags](PerShardData& sd, auto i) {
    if (poll_flags.test(i))
      shard_set->Add(i, poll_cb);
  });
}

void Transaction::FinishHop() {
  boost::intrusive_ptr<Transaction> guard(this);  // Keep alive until Dec() fully finishes
  run_barrier_.Dec();
}

void Transaction::Conclude() {
  if (!IsScheduled())
    return;
  auto cb = [](Transaction* t, EngineShard* shard) { return OpStatus::OK; };
  Execute(std::move(cb), true);
}

void Transaction::Refurbish() {
  txid_ = 0;
  coordinator_state_ = 0;
  cb_ptr_ = nullptr;
}

const absl::flat_hash_set<std::string_view>& Transaction::GetMultiKeys() const {
  DCHECK(multi_);
  return multi_->frozen_keys_set;
}

void Transaction::FIX_ConcludeJournalExec() {
  if (!multi_->shard_journal_write.front())
    return;

  multi_->shard_journal_cnt = 1;
  MultiReportJournalOnShard(EngineShard::tlocal());
}

string Transaction::DEBUG_PrintFailState(ShardId sid) const {
  auto res = StrCat(
      "usc: ", unique_shard_cnt_, ", name:", GetCId()->name(),
      ", usecnt:", use_count_.load(memory_order_relaxed), ", runcnt: ", run_barrier_.DEBUG_Count(),
      ", coordstate: ", coordinator_state_, ", coord native thread: ", stats_.coordinator_index,
      ", schedule attempts: ", stats_.schedule_attempts, ", report from sid: ", sid, "\n");
  std::atomic_thread_fence(memory_order_acquire);
  for (unsigned i = 0; i < shard_data_.size(); ++i) {
    const auto& sd = shard_data_[i];
    absl::StrAppend(&res, "- shard: ", i, " local_mask:", sd.local_mask,
                    " total_runs: ", sd.stats.total_runs, "\n");
  }
  return res;
}

void Transaction::EnableShard(ShardId sid) {
  unique_shard_cnt_ = 1;
  unique_shard_id_ = sid;
  shard_data_.resize(1);
  shard_data_.front().local_mask |= ACTIVE;
}

void Transaction::EnableAllShards() {
  unique_shard_cnt_ = shard_set->size();
  unique_shard_id_ = unique_shard_cnt_ == 1 ? 0 : kInvalidSid;
  shard_data_.resize(shard_set->size());
  for (auto& sd : shard_data_)
    sd.local_mask |= ACTIVE;
}

// runs in coordinator thread.
// Marks the transaction as expired and removes it from the waiting queue.
void Transaction::ExpireBlocking(WaitKeysProvider wcb) {
  DCHECK(!IsGlobal());
  DVLOG(1) << "ExpireBlocking " << DebugId();
  run_barrier_.Start(unique_shard_cnt_);

  auto expire_cb = [this, &wcb] {
    EngineShard* es = EngineShard::tlocal();
    ExpireShardCb(wcb(this, es), es);
  };
  IterateActiveShards([&expire_cb](PerShardData& sd, auto i) { shard_set->Add(i, expire_cb); });

  run_barrier_.Wait();
  DVLOG(1) << "ExpireBlocking finished " << DebugId();
}

string_view Transaction::Name() const {
  return cid_ ? cid_->name() : "null-command";
}

ShardId Transaction::GetUniqueShard() const {
  DCHECK_EQ(GetUniqueShardCnt(), 1U);
  return unique_shard_id_;
}

optional<SlotId> Transaction::GetUniqueSlotId() const {
  return unique_slot_checker_.GetUniqueSlotId();
}

KeyLockArgs Transaction::GetLockArgs(ShardId sid) const {
  KeyLockArgs res;
  res.db_index = db_index_;
  res.key_step = cid_->opt_mask() & CO::INTERLEAVED_KEYS ? 2 : 1;
  res.args = GetShardArgs(sid);
  DCHECK(!res.args.empty() || (cid_->opt_mask() & CO::NO_KEY_TRANSACTIONAL));

  return res;
}

uint16_t Transaction::DisarmInShard(ShardId sid) {
  auto& sd = shard_data_[SidToId(sid)];
  // NOTE: Maybe compare_exchange is worth it to avoid redundant writes
  return sd.is_armed.exchange(false, memory_order_acquire) ? sd.local_mask : 0;
}

pair<uint16_t, bool> Transaction::DisarmInShardWhen(ShardId sid, uint16_t relevant_flags) {
  auto& sd = shard_data_[SidToId(sid)];
  if (sd.is_armed.load(memory_order_acquire)) {
    bool relevant = sd.local_mask & relevant_flags;
    if (relevant)
      CHECK(sd.is_armed.exchange(false, memory_order_release));
    return {sd.local_mask, relevant};
  }
  return {0, false};
}

bool Transaction::IsActive(ShardId sid) const {
  // If we have only one shard, we often don't store infromation about all shards, so determine it
  // solely by id
  if (unique_shard_cnt_ == 1) {
    // However the active flag is still supposed to be set for our unique shard
    DCHECK((shard_data_[SidToId(unique_shard_id_)].local_mask & ACTIVE));
    return sid == unique_shard_id_;
  }

  return shard_data_[SidToId(sid)].local_mask & ACTIVE;
}

IntentLock::Mode Transaction::LockMode() const {
  return cid_->IsReadOnly() ? IntentLock::SHARED : IntentLock::EXCLUSIVE;
}

OpArgs Transaction::GetOpArgs(EngineShard* shard) const {
  DCHECK(IsActive(shard->shard_id()));
  DCHECK((multi_ && multi_->role == SQUASHED_STUB) || (run_barrier_.DEBUG_Count() > 0));
  return OpArgs{shard, this, GetDbContext()};
}

// This function should not block since it's run via RunBriefInParallel.
bool Transaction::ScheduleInShard(EngineShard* shard, bool can_run_immediately) {
  ShardId sid = SidToId(shard->shard_id());
  auto& sd = shard_data_[sid];

  DCHECK(sd.local_mask & ACTIVE);
  DCHECK_EQ(sd.local_mask & KEYLOCK_ACQUIRED, 0);
  sd.local_mask &= ~(OUT_OF_ORDER | RAN_IMMEDIATELY);

  TxQueue* txq = shard->txq();
  KeyLockArgs lock_args;
  IntentLock::Mode mode = LockMode();
  bool lock_granted = false;

  // If a more recent transaction already commited, we abort
  if (txid_ > 0 && shard->committed_txid() >= txid_)
    return false;

  // Acquire intent locks. Intent locks are always acquired, even if already locked by others.
  if (!IsGlobal()) {
    lock_args = GetLockArgs(shard->shard_id());
    bool shard_unlocked = shard->shard_lock()->Check(mode);

    // Check if we can run immediately
    if (shard_unlocked && can_run_immediately && CheckLocks(shard->db_slice(), mode, lock_args)) {
      sd.local_mask |= RAN_IMMEDIATELY;
      shard->stats().tx_immediate_total++;

      RunCallback(shard);
      // Check state again, it could've been updated if the callback returned AVOID_CONCLUDING flag.
      // Only possible for single shard.
      if (coordinator_state_ & COORD_CONCLUDING)
        return true;
    }

    bool keys_unlocked = shard->db_slice().Acquire(mode, lock_args);
    lock_granted = shard_unlocked && keys_unlocked;

    sd.local_mask |= KEYLOCK_ACQUIRED;
    if (lock_granted) {
      sd.local_mask |= OUT_OF_ORDER;
    }

    DVLOG(3) << "Lock granted " << lock_granted << " for trans " << DebugId();
  }

  // Single shard operations might have delayed acquiring txid unless neccessary.
  if (txid_ == 0) {
    DCHECK_EQ(unique_shard_cnt_, 1u);
    txid_ = op_seq.fetch_add(1, memory_order_relaxed);
    DCHECK_GT(txid_, shard->committed_txid());
  }

  // If the new transaction requires reordering of the pending queue (i.e. it comes before tail)
  // and some other transaction already locked its keys we can not reorder 'trans' because
  // the transaction could have deduced that it can run OOO and eagerly execute. Hence, we
  // fail this scheduling attempt for trans.
  if (!txq->Empty() && txid_ < txq->TailScore() && !lock_granted) {
    if (sd.local_mask & KEYLOCK_ACQUIRED) {
      shard->db_slice().Release(mode, lock_args);
      sd.local_mask &= ~KEYLOCK_ACQUIRED;
    }
    return false;
  }

  if (IsGlobal()) {
    shard->shard_lock()->Acquire(mode);
    VLOG(1) << "Global shard lock acquired";
  }

  TxQueue::Iterator it = txq->Insert(this);
  DCHECK_EQ(TxQueue::kEnd, sd.pq_pos);
  sd.pq_pos = it;

  AnalyzeTxQueue(shard, txq);
  DVLOG(1) << "Insert into tx-queue, sid(" << sid << ") " << DebugId() << ", qlen " << txq->size();

  return true;
}

bool Transaction::CancelShardCb(EngineShard* shard) {
  ShardId idx = SidToId(shard->shard_id());
  auto& sd = shard_data_[idx];

  TxQueue::Iterator q_pos = exchange(sd.pq_pos, TxQueue::kEnd);
  if (q_pos == TxQueue::kEnd) {
    DCHECK_EQ(sd.local_mask & KEYLOCK_ACQUIRED, 0);
    return false;
  }

  TxQueue* txq = shard->txq();
  bool was_head = txq->Head() == q_pos;

  Transaction* trans = absl::get<Transaction*>(txq->At(q_pos));
  DCHECK(trans == this) << txq->size() << ' ' << sd.pq_pos << ' ' << trans->DebugId();
  txq->Remove(q_pos);

  if (IsGlobal()) {
    shard->shard_lock()->Release(LockMode());
  } else {
    auto lock_args = GetLockArgs(shard->shard_id());
    DCHECK(sd.local_mask & KEYLOCK_ACQUIRED);
    DCHECK(!lock_args.args.empty());
    shard->db_slice().Release(LockMode(), lock_args);
    sd.local_mask &= ~KEYLOCK_ACQUIRED;
  }

  // Check if we need to poll the next head
  return was_head && !txq->Empty();
}

// runs in engine-shard thread.
ArgSlice Transaction::GetShardArgs(ShardId sid) const {
  DCHECK(!multi_ || multi_->role != SQUASHER);

  // We can read unique_shard_cnt_  only because ShardArgsInShard is called after IsArmedInShard
  // barrier.
  if (unique_shard_cnt_ == 1) {
    return kv_args_;
  }

  const auto& sd = shard_data_[sid];
  return ArgSlice{kv_args_.data() + sd.arg_start, sd.arg_count};
}

// from local index back to original arg index skipping the command.
// i.e. returns (first_key_pos -1) or bigger.
size_t Transaction::ReverseArgIndex(ShardId shard_id, size_t arg_index) const {
  DCHECK_LT(arg_index, reverse_index_.size());

  if (unique_shard_cnt_ == 1)
    return reverse_index_[arg_index];

  const auto& sd = shard_data_[shard_id];
  return reverse_index_[sd.arg_start + arg_index];
}

OpStatus Transaction::WaitOnWatch(const time_point& tp, WaitKeysProvider wkeys_provider,
                                  KeyReadyChecker krc, bool* block_flag, bool* pause_flag) {
  if (blocking_barrier_.IsClaimed()) {  // Might have been cancelled ahead by a dropping connection
    Conclude();
    return OpStatus::CANCELLED;
  }

  DCHECK(!IsAtomicMulti());  // blocking inside MULTI is not allowed

  // Register keys on active shards blocking controllers and mark shard state as suspended.
  auto cb = [&](Transaction* t, EngineShard* shard) {
    auto keys = wkeys_provider(t, shard);
    return t->WatchInShard(keys, shard, krc);
  };
  Execute(std::move(cb), true);

  // Don't reset the scheduled flag because we didn't release the locks
  coordinator_state_ |= COORD_SCHED;

  auto* stats = ServerState::tl_connection_stats();
  ++stats->num_blocked_clients;
  DVLOG(1) << "WaitOnWatch wait for " << tp << " " << DebugId();

  // Wait for the blocking barrier to be closed.
  // Note: It might return immediately if another thread already notified us.
  *block_flag = true;
  cv_status status = blocking_barrier_.Wait(tp);
  *block_flag = false;

  DVLOG(1) << "WaitOnWatch done " << int(status) << " " << DebugId();
  --stats->num_blocked_clients;

  *pause_flag = true;
  ServerState::tlocal()->AwaitPauseState(true);  // blocking are always write commands
  *pause_flag = false;

  OpStatus result = OpStatus::OK;
  if (status == cv_status::timeout) {
    result = OpStatus::TIMED_OUT;
  } else if (coordinator_state_ & COORD_CANCELLED) {
    result = local_result_;
  }

  // If we don't follow up with an "action" hop, we must clean up manually on all shards.
  if (result != OpStatus::OK)
    ExpireBlocking(wkeys_provider);

  return result;
}

OpStatus Transaction::WatchInShard(ArgSlice keys, EngineShard* shard, KeyReadyChecker krc) {
  auto& sd = shard_data_[SidToId(shard->shard_id())];

  CHECK_EQ(0, sd.local_mask & SUSPENDED_Q);
  sd.local_mask |= SUSPENDED_Q;
  sd.local_mask &= ~OUT_OF_ORDER;

  shard->EnsureBlockingController()->AddWatched(keys, std::move(krc), this);
  DVLOG(2) << "WatchInShard " << DebugId() << ", first_key:" << keys.front();

  return OpStatus::OK;
}

void Transaction::ExpireShardCb(ArgSlice wkeys, EngineShard* shard) {
  // Blocking transactions don't release keys when suspending, release them now.
  auto lock_args = GetLockArgs(shard->shard_id());
  shard->db_slice().Release(LockMode(), lock_args);

  auto& sd = shard_data_[SidToId(shard->shard_id())];
  sd.local_mask &= ~KEYLOCK_ACQUIRED;

  shard->blocking_controller()->FinalizeWatched(wkeys, this);
  DCHECK(!shard->blocking_controller()->awakened_transactions().contains(this));

  // Resume processing of transaction queue
  shard->PollExecution("unwatchcb", nullptr);
  FinishHop();
}

OpStatus Transaction::RunSquashedMultiCb(RunnableType cb) {
  DCHECK(multi_ && multi_->role == SQUASHED_STUB);
  DCHECK_EQ(unique_shard_cnt_, 1u);

  auto* shard = EngineShard::tlocal();
  auto result = cb(this, shard);
  shard->db_slice().OnCbFinish();
  LogAutoJournalOnShard(shard, result);

  DCHECK_EQ(result.flags, 0);  // if it's sophisticated, we shouldn't squash it
  return result;
}

void Transaction::MultiReportJournalOnShard(EngineShard* shard) const {
  DCHECK_EQ(EngineShard::tlocal(), shard);
  auto* journal = shard->journal();
  size_t write_idx = multi_->role == SQUASHED_STUB ? 0 : shard->shard_id();
  if (journal != nullptr && multi_->shard_journal_write[write_idx]) {
    journal->RecordEntry(txid_, journal::Op::EXEC, db_index_, multi_->shard_journal_cnt,
                         unique_slot_checker_.GetUniqueSlotId(), {}, true);
  }
}

void Transaction::UnlockMultiShardCb(absl::Span<const std::string_view> sharded_keys,
                                     EngineShard* shard) {
  DCHECK(multi_ && multi_->lock_mode);

  MultiReportJournalOnShard(shard);

  if (multi_->mode == GLOBAL) {
    shard->shard_lock()->Release(IntentLock::EXCLUSIVE);
  } else {
    for (const auto& key : sharded_keys) {
      shard->db_slice().ReleaseNormalized(*multi_->lock_mode, db_index_, LockTag{key});
    }
  }

  ShardId sid = shard->shard_id();
  auto& sd = shard_data_[SidToId(sid)];
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
}

bool Transaction::IsGlobal() const {
  // Please note that a transaction can be non-global even if multi_->mode == GLOBAL.
  // It happens when a transaction is squashed and switches to execute differrent commands.
  return global_;
}

// Runs only in the shard thread.
// Returns true if the transacton has changed its state from suspended to awakened,
// false, otherwise.
bool Transaction::NotifySuspended(TxId committed_txid, ShardId sid, string_view key) {
  // Wake a transaction only once on the first notify.
  // We don't care about preserving the strict order with multiple operations running on blocking
  // keys in parallel, because the internal order is not observable from outside either way.
  if (!blocking_barrier_.TryClaim())
    return false;

  auto& sd = shard_data_[SidToId(sid)];

  DVLOG(1) << "NotifySuspended " << DebugId() << ", local_mask:" << sd.local_mask
           << " by commited_id " << committed_txid;

  // We're the first and only to wake this transaction, expect the shard to be suspended
  CHECK(sd.local_mask & SUSPENDED_Q);
  CHECK_EQ(sd.local_mask & AWAKED_Q, 0);

  // Find index of awakened key
  auto args = GetShardArgs(sid);
  auto it = find_if(args.begin(), args.end(), [key](auto arg) { return facade::ToSV(arg) == key; });
  CHECK(it != args.end());

  // Change state to awaked and store index of awakened key
  sd.local_mask &= ~SUSPENDED_Q;
  sd.local_mask |= AWAKED_Q;
  sd.wake_key_pos = it - args.begin();

  blocking_barrier_.Close();
  return true;
}

optional<string_view> Transaction::GetWakeKey(ShardId sid) const {
  auto& sd = shard_data_[SidToId(sid)];
  if ((sd.local_mask & AWAKED_Q) == 0)
    return nullopt;

  CHECK_NE(sd.wake_key_pos, UINT16_MAX);
  return GetShardArgs(sid).at(sd.wake_key_pos);
}

void Transaction::LogAutoJournalOnShard(EngineShard* shard, RunnableResult result) {
  // TODO: For now, we ignore non shard coordination.
  if (shard == nullptr)
    return;

  // Ignore technical squasher hops.
  if (multi_ && multi_->role == SQUASHER)
    return;

  // Only write commands and/or no-key-transactional commands are logged
  if (cid_->IsWriteOnly() == 0 && (cid_->opt_mask() & CO::NO_KEY_TRANSACTIONAL) == 0)
    return;

  auto journal = shard->journal();
  if (journal == nullptr)
    return;

  if (result.status != OpStatus::OK) {
    // We log NOOP even for NO_AUTOJOURNAL commands because the non-success status could have been
    // due to OOM in a single shard, while other shards succeeded
    journal->RecordEntry(txid_, journal::Op::NOOP, db_index_, unique_shard_cnt_,
                         unique_slot_checker_.GetUniqueSlotId(), journal::Entry::Payload{}, true);
    return;
  }

  // If autojournaling was disabled and not re-enabled, skip it
  if ((cid_->opt_mask() & CO::NO_AUTOJOURNAL) && !re_enabled_auto_journal_)
    return;

  // TODO: Handle complex commands like LMPOP correctly once they are implemented.
  journal::Entry::Payload entry_payload;

  string_view cmd{cid_->name()};
  if (unique_shard_cnt_ == 1 || kv_args_.empty()) {
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

  if (multi_) {
    if (multi_->role != SQUASHED_STUB)
      multi_->shard_journal_write[shard->shard_id()] = true;
    else
      multi_->shard_journal_write[0] = true;
  }

  bool is_multi = multi_commands || IsAtomicMulti();

  auto opcode = is_multi ? journal::Op::MULTI_COMMAND : journal::Op::COMMAND;
  journal->RecordEntry(txid_, opcode, db_index_, shard_cnt, unique_slot_checker_.GetUniqueSlotId(),
                       std::move(payload), allow_await);
}

void Transaction::FinishLogJournalOnShard(EngineShard* shard, uint32_t shard_cnt) const {
  if (multi_) {
    return;
  }
  auto journal = shard->journal();
  CHECK(journal);
  journal->RecordEntry(txid_, journal::Op::EXEC, db_index_, shard_cnt,
                       unique_slot_checker_.GetUniqueSlotId(), {}, false);
}

void Transaction::ReviveAutoJournal() {
  DCHECK(cid_->opt_mask() & CO::NO_AUTOJOURNAL);
  DCHECK_EQ(run_barrier_.DEBUG_Count(), 0u);  // Can't be changed while dispatching
  re_enabled_auto_journal_ = true;
}

void Transaction::CancelBlocking(std::function<OpStatus(ArgSlice)> status_cb) {
  // We're on the owning thread of this transaction, so we can safely access it's data below.
  // First, check if it makes sense to proceed.
  if (blocking_barrier_.IsClaimed() || cid_ == nullptr || (cid_->opt_mask() & CO::BLOCKING) == 0)
    return;

  OpStatus status = OpStatus::CANCELLED;
  if (status_cb) {
    vector<string_view> all_keys;
    IterateActiveShards([this, &all_keys](PerShardData&, auto i) {
      auto shard_keys = GetShardArgs(i);
      all_keys.insert(all_keys.end(), shard_keys.begin(), shard_keys.end());
    });
    status = status_cb(absl::MakeSpan(all_keys));
  }

  if (status == OpStatus::OK)
    return;

  // Check if someone else is about to wake us up
  if (!blocking_barrier_.TryClaim())
    return;

  coordinator_state_ |= COORD_CANCELLED;
  local_result_ = status;
  blocking_barrier_.Close();
}

bool Transaction::CanRunInlined() const {
  auto* ss = ServerState::tlocal();
  if (unique_shard_cnt_ == 1 && unique_shard_id_ == ss->thread_index() &&
      ss->AllowInlineScheduling()) {
    ss->stats.tx_inline_runs++;
    return true;
  }
  return false;
}

OpResult<KeyIndex> DetermineKeys(const CommandId* cid, CmdArgList args) {
  KeyIndex key_index;

  if (cid->opt_mask() & (CO::GLOBAL_TRANS | CO::NO_KEY_TRANSACTIONAL))
    return key_index;

  int num_custom_keys = -1;

  if (cid->opt_mask() & CO::REVERSE_MAPPING) {
    key_index.has_reverse_mapping = true;
  }

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

    if (num_custom_keys == 0 &&
        (absl::StartsWith(name, "ZDIFF") || absl::StartsWith(name, "ZUNION") ||
         absl::StartsWith(name, "ZINTER"))) {
      return OpStatus::AT_LEAST_ONE_KEY;
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
    key_index.step = cid->opt_mask() & CO::INTERLEAVED_KEYS ? 2 : 1;

    if (cid->opt_mask() & CO::STORE_LAST_KEY) {
      string_view name{cid->name()};

      if (name == "GEORADIUSBYMEMBER" && args.size() >= 5) {
        // key member radius .. STORE destkey
        string_view opt = ArgS(args, args.size() - 2);
        if (absl::EqualsIgnoreCase(opt, "STORE") || absl::EqualsIgnoreCase(opt, "STOREDIST")) {
          key_index.bonus = args.size() - 1;
        }
      }
    }

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
