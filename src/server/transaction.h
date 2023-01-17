// Copyright 2022, DragonflyDB authors.  All rights reserved.
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
#include "server/journal/types.h"
#include "server/table.h"
#include "util/fibers/fibers_ext.h"

namespace dfly {

class EngineShard;
class BlockingController;

using facade::OpResult;
using facade::OpStatus;

class Transaction {
  friend class BlockingController;

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
  using time_point = ::std::chrono::steady_clock::time_point;
  using RunnableType = std::function<OpStatus(Transaction* t, EngineShard*)>;
  using WaitKeysPovider = std::function<ArgSlice(Transaction*, EngineShard* shard)>;

  enum LocalMask : uint16_t {
    ARMED = 1,  // Transaction was armed with the callback
    OUT_OF_ORDER = 2,
    KEYLOCK_ACQUIRED = 4,
    SUSPENDED_Q = 0x10,  // added by the coordination flow (via WaitBlocked()).
    AWAKED_Q = 0x20,     // awaked by condition (lpush etc)
    EXPIRED_Q = 0x40,    // timed-out and should be garbage collected from the blocking queue.
  };

 public:
  explicit Transaction(const CommandId* cid);

  OpStatus InitByArgs(DbIndex index, CmdArgList args);

  void SetExecCmd(const CommandId* cid);

  std::string DebugId() const;

  // Runs in engine thread
  ArgSlice GetShardArgs(ShardId sid) const;

  // Maps the index in GetShardArgs(shard_id) slice back to the index
  // in the original array passed to InitByArgs.
  size_t ReverseArgIndex(ShardId shard_id, size_t arg_index) const;

  // Schedules a transaction. Usually used for multi-hop transactions like Rename or BLPOP.
  // For single hop, use ScheduleSingleHop instead.
  void Schedule();

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

  // Called by EngineShard when performing Execute over the tx queue.
  // Returns true if transaction should be kept in the queue.
  bool RunInShard(EngineShard* shard);

  // Registers transaction into watched queue and blocks until a) either notification is received.
  // or b) tp is reached. If tp is time_point::max() then waits indefinitely.
  // Expects that the transaction had been scheduled before, and uses Execute(.., true) to register.
  // Returns false if timeout occurred, true if was notified by one of the keys.
  bool WaitOnWatch(const time_point& tp, WaitKeysPovider cb);

  // Returns true if transaction is awaked, false if it's timed-out and can be removed from the
  // blocking queue. NotifySuspended may be called from (multiple) shard threads and
  // with each call potentially improving the minimal wake_txid at which
  // this transaction has been awaked.
  bool NotifySuspended(TxId committed_ts, ShardId sid);

  void BreakOnShutdown();

  void UnlockMulti();

  // In multi transaciton command we calculate the unique shard count of the trasaction
  // after all transaciton commands where executed, by checking the last txid writen to
  // all journals.
  // This value is writen to journal so that replica we be able to apply the multi command
  // atomicaly.
  void SetMultiUniqueShardCount();

  // Log a journal entry on shard with payload.
  void LogJournalOnShard(EngineShard* shard, journal::Entry::Payload&& payload) const;

 public:
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

  TxId txid() const {
    return txid_;
  }

  // based on cid_->opt_mask.
  IntentLock::Mode Mode() const;

  const char* Name() const;

  uint32_t GetUniqueShardCnt() const {
    return unique_shard_cnt_;
  }

  TxId GetNotifyTxid() const {
    return notify_txid_.load(std::memory_order_relaxed);
  }

  bool IsMulti() const {
    return bool(multi_);
  }

  bool IsGlobal() const;

  bool IsOOO() const {
    return coordinator_state_ & COORD_OOO;
  }

  //! Returns locking arguments needed for DbSlice to Acquire/Release transactional locks.
  //! Runs in the shard thread.
  KeyLockArgs GetLockArgs(ShardId sid) const;

  OpArgs GetOpArgs(EngineShard* shard) const {
    return OpArgs{shard, this, GetDbContext()};
  }

  DbContext GetDbContext() const {
    return DbContext{.db_index = db_index_, .time_now_ms = time_now_ms_};
  }

  DbIndex GetDbIndex() const {
    return db_index_;
  }

 private:
  struct LockCnt {
    unsigned cnt[2] = {0, 0};
  };

  using KeyList = std::vector<std::pair<std::string_view, LockCnt>>;

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

  struct MultiData {
    absl::flat_hash_map<std::string_view, LockCnt> locks;
    std::vector<std::string_view> keys;

    uint32_t multi_opts = 0;  // options of the parent transaction.

    // Whether this transaction can lock more keys during its progress.
    bool is_expanding = true;
    bool locks_recorded = false;
  };

  enum CoordinatorState : uint8_t {
    COORD_SCHED = 1,
    COORD_EXEC = 2,

    // We are running the last execution step in multi-hop operation.
    COORD_EXEC_CONCLUDING = 4,
    COORD_BLOCKED = 8,
    COORD_CANCELLED = 0x10,
    COORD_OOO = 0x20,
  };

 private:
  void ScheduleInternal();
  void LockMulti();

  void UnwatchBlocking(bool should_expire, WaitKeysPovider wcb);
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

  // Returns true if we need to follow up with PollExecution on this shard.
  bool CancelShardCb(EngineShard* shard);

  void UnwatchShardCb(ArgSlice wkeys, bool should_expire, EngineShard* shard);
  void UnlockMultiShardCb(const std::vector<KeyList>& sharded_keys, EngineShard* shard);

  // Adds itself to watched queue in the shard. Must run in that shard thread.
  OpStatus WatchInShard(ArgSlice keys, EngineShard* shard);

  void WaitForShardCallbacks() {
    run_ec_.await([this] { return 0 == run_count_.load(std::memory_order_relaxed); });

    // store operations below can not be ordered above the fence
    std::atomic_thread_fence(std::memory_order_release);
    seqlock_.fetch_add(1, std::memory_order_relaxed);
  }

  // Returns the previous value of run count.
  uint32_t DecreaseRunCnt();

  uint32_t GetUseCount() const {
    return use_count_.load(std::memory_order_relaxed);
  }

  // Log command in the journal of a shard for write commands with auto-journaling enabled.
  // Should be called immediately after the last phase (hop).
  void LogAutoJournalOnShard(EngineShard* shard);

  unsigned SidToId(ShardId sid) const {
    return sid < shard_data_.size() ? sid : 0;
  }

 private:
  // shard_data spans all the shards in ess_.
  // I wish we could use a dense array of size [0..uniq_shards] but since
  // multiple threads access this array to synchronize between themselves using
  // PerShardData.state, it can be tricky. The complication comes from multi_ transactions where
  // scheduled transaction is accessed between operations as well.
  absl::InlinedVector<PerShardData, 4> shard_data_;  // length = shard_count

  // Stores arguments of the transaction (i.e. keys + values) partitioned by shards.
  absl::InlinedVector<std::string_view, 4> args_;

  // Stores the full undivided command.
  CmdArgList cmd_with_full_args_;

  // Reverse argument mapping. Allows to reconstruct responses according to the original order of
  // keys.
  std::vector<uint32_t> reverse_index_;

  RunnableType cb_;
  const CommandId* cid_;
  std::unique_ptr<MultiData> multi_;  // Initialized when the transaction is multi/exec.

  TxId txid_{0};
  DbIndex db_index_{0};
  uint64_t time_now_ms_{0};
  std::atomic<TxId> notify_txid_{kuint64max};
  std::atomic_uint32_t use_count_{0}, run_count_{0}, seqlock_{0};

  // unique_shard_cnt_ and unique_shard_id_ is accessed only by coordinator thread.
  uint32_t unique_shard_cnt_{0};  // number of unique shards span by args_

  ShardId unique_shard_id_{kInvalidSid};

  util::fibers_ext::EventCount blocking_ec_;  // used to wake blocking transactions.
  util::fibers_ext::EventCount run_ec_;

  // Transaction coordinator state, written and read by coordinator thread.
  // Can be read by shard threads as long as we respect ordering rules, i.e. when
  // they read this variable the coordinator thread is stalled and can not cause data races.
  // If COORDINATOR_XXX has been set, it means we passed or crossed stage XXX.
  uint8_t coordinator_state_ = 0;

  // Used for single-hop transactions with unique_shards_ == 1, hence no data-race.
  OpStatus local_result_ = OpStatus::OK;

 private:
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
  return (intptr_t(ptr) >> 8) & 0xFFFF;
}

OpResult<KeyIndex> DetermineKeys(const CommandId* cid, CmdArgList args);

}  // namespace dfly
