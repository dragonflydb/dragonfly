// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <absl/container/inlined_vector.h>

#include <string_view>
#include <variant>
#include <vector>

#include "core/intent_lock.h"
#include "core/tx_queue.h"
#include "facade/op_status.h"
#include "server/common.h"
#include "server/table.h"
#include "util/fibers/fibers_ext.h"

namespace dfly {

class DbSlice;
class EngineShardSet;
class EngineShard;

using facade::OpStatus;
using facade::OpResult;

class Transaction {
  Transaction(const Transaction&);
  void operator=(const Transaction&) = delete;

  ~Transaction();

  // Transactions are reference counted.
  friend void intrusive_ptr_add_ref(Transaction* trans) noexcept {
    trans->use_count_.fetch_add(1, std::memory_order_relaxed);
  }

  friend void intrusive_ptr_release(Transaction* trans) noexcept {
    if (1 == trans->use_count_.fetch_sub(1, std::memory_order_release)) {
      std::atomic_thread_fence(std::memory_order_acquire);
      delete trans;
    }
  }

 public:
  using RunnableType = std::function<OpStatus(Transaction* t, EngineShard*)>;
  using time_point = ::std::chrono::steady_clock::time_point;

  enum LocalMask : uint16_t {
    ARMED = 1,  // Transaction was armed with the callback
    OUT_OF_ORDER = 2,
    KEYLOCK_ACQUIRED = 4,
    SUSPENDED_Q = 0x10,  // added by the coordination flow (via WaitBlocked()).
    AWAKED_Q = 0x20,     // awaked by condition (lpush etc)
    EXPIRED_Q = 0x40,    // timed-out and should be garbage collected from the blocking queue.
  };

  Transaction(const CommandId* cid, EngineShardSet* ess);

  void InitByArgs(DbIndex index, CmdArgList args);

  void SetExecCmd(const CommandId* cid);

  std::string DebugId() const;

  // Runs in engine thread
  ArgSlice ShardArgsInShard(ShardId sid) const;

  // Maps the index in ShardKeys(shard_id) slice back to the index in the original array passed to
  // InitByArgs.
  // Runs in the coordinator thread.
  size_t ReverseArgIndex(ShardId shard_id, size_t arg_index) const;

  //! Returns true if the transaction spans this shard_id.
  //! Runs from the coordinator thread.
  bool IsActive(ShardId shard_id) const {
    return unique_shard_cnt_ == 1 ? unique_shard_id_ == shard_id
                                  : shard_data_[shard_id].arg_count > 0;
  }

  //! Returns true if the transaction is armed for execution on this sid (used to avoid
  //! duplicate runs). Supports local transactions under multi as well.
  //! Can be used in contexts that wait for an event to happen.
  bool IsArmedInShard(ShardId sid) const {
    // For multi transactions shard_data_ spans all shards.
    if (sid >= shard_data_.size())
      sid = 0;

    // We use acquire so that no reordering will move before this load.
    return run_count_.load(std::memory_order_acquire) > 0 && shard_data_[sid].local_mask & ARMED;
  }

  // Called from engine set shard threads.
  uint16_t GetLocalMask(ShardId sid) const {
    return shard_data_[SidToId(sid)].local_mask;
  }

  // Schedules a transaction. Usually used for multi-hop transactions like Rename or BLPOP.
  // For single hop, use ScheduleSingleHop instead.
  void Schedule() {
    ScheduleInternal();
  }

  // if conclude is true, removes the transaction from the pending queue.
  void Execute(RunnableType cb, bool conclude);

  // for multi-key scenarios cb should return Status::Ok since otherwise the return value
  // will be ill-defined.
  OpStatus ScheduleSingleHop(RunnableType cb);

  // Fits only for single key scenarios because it writes into shared variable res from
  // potentially multiple threads.
  template <typename F> auto ScheduleSingleHopT(F&& f) -> decltype(f(this, nullptr)) {
    decltype(f(this, nullptr)) res;

    ScheduleSingleHop([&res, f = std::forward<F>(f)](Transaction* t, EngineShard* shard) {
      res = f(t, shard);
      return res.status();
    });
    return res;
  }

  void UnlockMulti();

  TxId txid() const {
    return txid_;
  }

  // based on cid_->opt_mask.
  IntentLock::Mode Mode() const;

  const char* Name() const;

  DbIndex db_index() const {
    return db_index_;  // TODO: support multiple db indexes.
  }

  uint32_t unique_shard_cnt() const {
    return unique_shard_cnt_;
  }

  TxId notify_txid() const {
    return notify_txid_.load(std::memory_order_relaxed);
  }

  bool IsMulti() const {
    return bool(multi_);
  }

  bool IsGlobal() const;

  bool IsOOO() const {
    return coordinator_state_ & COORD_OOO;
  }

  EngineShardSet* shard_set() {
    return ess_;
  }

  // Registers transaction into watched queue and blocks until a) either notification is received.
  // or b) tp is reached. If tp is time_point::max() then waits indefinitely.
  // Expects that the transaction had been scheduled before, and uses Execute(.., true) to register.
  // Returns false if timeout ocurred, true if was notified by one of the keys.
  bool WaitOnWatch(const time_point& tp);
  void UnregisterWatch();

  // Returns true if transaction is awaked, false if it's timed-out and can be removed from the
  // blocking queue. NotifySuspended may be called from (multiple) shard threads and
  // with each call potentially improving the minimal wake_txid at which
  // this transaction has been awaked.
  bool NotifySuspended(TxId committed_ts, ShardId sid);

  void BreakOnClose();

  // Called by EngineShard when performing Execute over the tx queue.
  // Returns true if transaction should be kept in the queue.
  bool RunInShard(EngineShard* shard);

  void RunNoop(EngineShard* shard);

  //! Returns locking arguments needed for DbSlice to Acquire/Release transactional locks.
  //! Runs in the shard thread.
  KeyLockArgs GetLockArgs(ShardId sid) const;

 private:
  unsigned SidToId(ShardId sid) const {
    return sid < shard_data_.size() ? sid : 0;
  }

  void ScheduleInternal();

  void ExpireBlocking();
  void ExecuteAsync();

  // Optimized version of RunInShard for single shard uncontended cases.
  void RunQuickie(EngineShard* shard);

  //! Returns true if transaction run out-of-order during the scheduling phase.
  bool ScheduleUniqueShard(EngineShard* shard);

  /// Returns pair(schedule_success, lock_granted)
  /// schedule_success is true if transaction was scheduled on db_slice.
  /// lock_granted is true if lock was granted for all the keys on this shard.
  /// Runs in the shard thread.
  std::pair<bool, bool> ScheduleInShard(EngineShard* shard);

  // Returns true if operation was cancelled for this shard. Runs in the shard thread.
  bool CancelShardCb(EngineShard* shard);

  // Shard callbacks used within Execute calls
  OpStatus AddToWatchedShardCb(EngineShard* shard);
  bool RemoveFromWatchedShardCb(EngineShard* shard);
  void ExpireShardCb(EngineShard* shard);
  void CheckForConvergence(EngineShard* shard);

  void WaitForShardCallbacks() {
    run_ec_.await([this] { return 0 == run_count_.load(std::memory_order_relaxed); });

    // store operations below can not be ordered above the fence
    std::atomic_thread_fence(std::memory_order_release);
    seqlock_.fetch_add(1, std::memory_order_relaxed);
  }

  // Returns the previous value of run count.
  uint32_t DecreaseRunCnt();

  uint32_t use_count() const {
    return use_count_.load(std::memory_order_relaxed);
  }

  struct PerShardData {
    uint32_t arg_start = 0;  // Indices into args_ array.
    uint16_t arg_count = 0;

    // Accessed only within the engine-shard thread.
    // Bitmask of LocalState enums.
    uint16_t local_mask{0};

    // Needed to rollback inconsistent schedulings or remove OOO transactions from
    // tx queue.
    uint32_t pq_pos = TxQueue::kEnd;

    PerShardData(PerShardData&&) noexcept {
    }

    PerShardData() = default;
  };
  enum { kPerShardSize = sizeof(PerShardData) };

  struct LockCnt {
    unsigned cnt[2] = {0, 0};
  };

  struct Multi {
    absl::flat_hash_map<std::string_view, LockCnt> locks;
    uint32_t multi_opts = 0;  // options of the parent transaction.

    bool incremental = true;
    bool locks_recorded = false;
  };

  util::fibers_ext::EventCount blocking_ec_;  // used to wake blocking transactions.
  util::fibers_ext::EventCount run_ec_;

  // shard_data spans all the shards in ess_.
  // I wish we could use a dense array of size [0..uniq_shards] but since
  // multiple threads access this array to synchronize between themselves using
  // PerShardData.state, it can be tricky. The complication comes from multi_ transactions where
  // scheduled transaction is accessed between operations as well.
  absl::InlinedVector<PerShardData, 4> shard_data_;  // length = shard_count

  //! Stores arguments of the transaction (i.e. keys + values) partitioned by shards.
  absl::InlinedVector<std::string_view, 4> args_;

  // Reverse argument mapping. Allows to reconstruct responses according to the original order of
  // keys.
  std::vector<uint32_t> reverse_index_;

  RunnableType cb_;
  std::unique_ptr<Multi> multi_;  // Initialized when the transaction is multi/exec.

  const CommandId* cid_;
  EngineShardSet* ess_;
  TxId txid_{0};
  std::atomic<TxId> notify_txid_{kuint64max};

  std::atomic_uint32_t use_count_{0}, run_count_{0}, seqlock_{0};

  // unique_shard_cnt_ and unique_shard_id_ is accessed only by coordinator thread.
  uint32_t unique_shard_cnt_{0};  // number of unique shards span by args_

  ShardId unique_shard_id_{kInvalidSid};
  DbIndex db_index_ = 0;

  // Used for single-hop transactions with unique_shards_ == 1, hence no data-race.
  OpStatus local_result_ = OpStatus::OK;

  enum CoordinatorState : uint8_t {
    COORD_SCHED = 1,
    COORD_EXEC = 2,

    // We are running the last execution step in multi-hop operation.
    COORD_EXEC_CONCLUDING = 4,
    COORD_BLOCKED = 8,
    COORD_CANCELLED = 0x10,
    COORD_OOO = 0x20,
  };

  // Transaction coordinator state, written and read by coordinator thread.
  // Can be read by shard threads as long as we respect ordering rules, i.e. when
  // they read this variable the coordinator thread is stalled and can not cause data races.
  // If COORDINATOR_XXX has been set, it means we passed or crossed stage XXX.
  uint8_t coordinator_state_ = 0;

  struct PerShardCache {
    std::vector<std::string_view> args;
    std::vector<uint32_t> original_index;

    void Clear() {
      args.clear();
      original_index.clear();
    }
  };

  struct TLTmpSpace {
    std::vector<PerShardCache> shard_cache;
    absl::flat_hash_set<std::string_view> uniq_keys;
  };

  static thread_local TLTmpSpace tmp_space;
};

inline uint16_t trans_id(const Transaction* ptr) {
  return intptr_t(ptr) & 0xFFFF;
}

}  // namespace dfly
