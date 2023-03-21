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

// Central building block of the transactional framework.
//
// Use it to run callbacks on the shard threads - such dispatches are called hops.
// The shards to run on are determined by the keys of the underlying command.
// Global transactions run on all shards.
//
// Use ScheduleSingleHop() if only a single hop is needed.
// Otherwise, schedule the transaction with Schedule() and run successive hops
// with Execute().
//
// Multi transactions are handled by a single transaction, which internally avoids
// rescheduling. The flow of EXEC and EVAL is as follows:
//
// trans->StartMulti_MultiMode_()
// for ([cmd, args]) {
//   trans->MultiSwitchCmd(cmd)  // 1. Set new command
//   trans->InitByArgs(args)     // 2. Re-initialize with arguments
//   cmd->Invoke(trans)          // 3. Run
// }
// trans->UnlockMulti()
//
class Transaction {
  friend class BlockingController;

  Transaction(const Transaction&);
  void operator=(const Transaction&) = delete;

  ~Transaction();  // Transactions are reference counted with intrusive_ptr.

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
  // Runnable that is run on shards during hop executions (often named callback).
  using RunnableType = std::function<OpStatus(Transaction* t, EngineShard*)>;
  // Provides keys to block on for specific shard.
  using WaitKeysProvider = std::function<ArgSlice(Transaction*, EngineShard* shard)>;

  // Modes in which a multi transaction can run.
  enum MultiMode {
    // Invalid state.
    NOT_DETERMINED = 0,
    // Global transaction.
    GLOBAL = 1,
    // Keys are locked ahead during Schedule.
    LOCK_AHEAD = 2,
    // Keys are locked incrementally during each new command.
    // The shards to schedule on are detemined ahead and remain fixed.
    LOCK_INCREMENTAL = 3,
    // Each command is executed separately. Equivalent to a pipeline.
    NON_ATOMIC = 4,
  };

  // State on specific shard.
  enum LocalMask : uint16_t {
    ACTIVE = 1,  // Set on all active shards.
    // UNUSED = 1 << 1,
    OUT_OF_ORDER = 1 << 2,      // Whether its running out of order
    KEYLOCK_ACQUIRED = 1 << 3,  // Whether its key locks are acquired
    SUSPENDED_Q = 1 << 4,       // Whether is suspened (by WatchInShard())
    AWAKED_Q = 1 << 5,          // Whether it was awakened (by NotifySuspended())
    EXPIRED_Q = 1 << 6,         // Whether it timed out and should be dropped
  };

  enum StubMode { NONE, INLINE, DELAYED };

 public:
  explicit Transaction(const CommandId* cid, uint32_t thread_index);

  explicit Transaction(Transaction* parent, StubMode sm);

  // Initialize from command (args) on specific db.
  OpStatus InitByArgs(DbIndex index, CmdArgList args);

  // Get command arguments for specific shard. Called from shard thread.
  ArgSlice GetShardArgs(ShardId sid) const;

  // Map arg_index from GetShardArgs slice to index in original command slice from InitByArgs.
  size_t ReverseArgIndex(ShardId shard_id, size_t arg_index) const;

  // Schedule transaction.
  // Usually used for multi hop transactions like RENAME or BLPOP.
  // For single hop transactions use ScheduleSingleHop instead.
  void Schedule();

  // Execute transaction hop. If conclude is true, it is removed from the pending queue.
  void Execute(RunnableType cb, bool conclude);

  // Execute single hop and conclude.
  // Callback should return OK for multi key invocations, otherwise return value is ill-defined.
  OpStatus ScheduleSingleHop(RunnableType cb);

  // Execute single hop with return value and conclude.
  // Can be used only for single key invocations, because it writes a into shared variable.
  template <typename F> auto ScheduleSingleHopT(F&& f) -> decltype(f(this, nullptr));

  // Called by EngineShard when performing Execute over the tx queue.
  // Returns true if transaction should be kept in the queue.
  bool RunInShard(EngineShard* shard);

  // Registers transaction into watched queue and blocks until a) either notification is received.
  // or b) tp is reached. If tp is time_point::max() then waits indefinitely.
  // Expects that the transaction had been scheduled before, and uses Execute(.., true) to register.
  // Returns false if timeout occurred, true if was notified by one of the keys.
  bool WaitOnWatch(const time_point& tp, WaitKeysProvider cb);

  // Returns true if transaction is awaked, false if it's timed-out and can be removed from the
  // blocking queue. NotifySuspended may be called from (multiple) shard threads and
  // with each call potentially improving the minimal wake_txid at which
  // this transaction has been awaked.
  bool NotifySuspended(TxId committed_ts, ShardId sid);

  // Cancel all blocking watches on shutdown. Set COORD_CANCELLED.
  void BreakOnShutdown();

  // In some cases for non auto-journaling commands we want to enable the auto journal flow.
  void RenableAutoJournal() {
    renabled_auto_journal_.store(true, std::memory_order_relaxed);
  }

  // Start multi in GLOBAL mode.
  void StartMultiGlobal(DbIndex dbid);

  // Start multi in LOCK_AHEAD mode with given keys.
  void StartMultiLockedAhead(DbIndex dbid, CmdArgList keys);

  // Start multi in LOCK_INCREMENTAL mode on given shards.
  void StartMultiLockedIncr(DbIndex dbid, const std::vector<bool>& shards);

  // Start multi in NON_ATOMIC mode.
  void StartMultiNonAtomic();

  // Unlock key locks of a multi transaction.
  void UnlockMulti();

  // Set new command for multi transaction.
  void MultiSwitchCmd(const CommandId* cid);

  // Returns locking arguments needed for DbSlice to Acquire/Release transactional locks.
  // Runs in the shard thread.
  KeyLockArgs GetLockArgs(ShardId sid) const;

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
    return run_count_.load(std::memory_order_acquire) > 0 &&
           shard_data_[sid].is_armed.load(std::memory_order_relaxed);
  }

  // MVP
  void Wait() {
    WaitForShardCallbacks();
  }

  void RunStub();

  void NonBlock() {
    non_blocking_ = true;
  }

  // Called from engine set shard threads.
  uint16_t GetLocalMask(ShardId sid) const {
    return shard_data_[SidToId(sid)].local_mask;
  }

  TxId txid() const {
    return txid_;
  }

  IntentLock::Mode Mode() const;  // Based on command mask

  const char* Name() const;  // Based on command name

  uint32_t GetUniqueShardCnt() const {
    return unique_shard_cnt_;
  }

  TxId GetNotifyTxid() const {
    return notify_txid_.load(std::memory_order_relaxed);
  }

  bool IsMulti() const {
    return bool(multi_);
  }

  MultiMode GetMultiMode() const {
    return multi_->mode;
  }

  bool IsGlobal() const;

  bool IsOOO() const {
    return coordinator_state_ & COORD_OOO;
  }

  OpArgs GetOpArgs(EngineShard* shard) const {
    return OpArgs{shard, this, GetDbContext()};
  }

  DbContext GetDbContext() const {
    return DbContext{.db_index = db_index_, .time_now_ms = time_now_ms_};
  }

  DbIndex GetDbIndex() const {
    return db_index_;
  }

  const CommandId* GetCId() const {
    return cid_;
  }

  std::string DebugId() const;

  // Write a journal entry to a shard journal with the given payload. When logging a non-automatic
  // journal command, multiple journal entries may be necessary. In this case, call with set
  // multi_commands to true and  call the FinishLogJournalOnShard function after logging the final
  // entry.
  void LogJournalOnShard(EngineShard* shard, journal::Entry::Payload&& payload, uint32_t shard_cnt,
                         bool multi_commands, bool allow_await) const;
  void FinishLogJournalOnShard(EngineShard* shard, uint32_t shard_cnt) const;

 private:
  // Holds number of locks for each IntentLock::Mode: shared and exlusive.
  struct LockCnt {
    unsigned& operator[](IntentLock::Mode mode) {
      return cnt[int(mode)];
    }

    unsigned operator[](IntentLock::Mode mode) const {
      return cnt[int(mode)];
    }

   private:
    unsigned cnt[2] = {0, 0};
  };

  // owned std::string because callbacks its used in run fully async and can outlive the entries.
  using KeyList = std::vector<std::pair<std::string, LockCnt>>;

  struct PerShardData {
    PerShardData(PerShardData&&) noexcept {
    }

    PerShardData() = default;

    // this is the only variable that is accessed by both shard and coordinator threads.
    std::atomic_bool is_armed{false};

    // We pad with some memory so that atomic loads won't cause false sharing betweem threads.
    char pad[48];  // to make sure PerShardData is 64 bytes and takes full cacheline.

    uint32_t arg_start = 0;  // Indices into args_ array.
    uint16_t arg_count = 0;

    // Accessed within shard thread.
    // Bitmask of LocalState enums.
    uint16_t local_mask{0};

    // Needed to rollback inconsistent schedulings or remove OOO transactions from
    // tx queue.
    uint32_t pq_pos = TxQueue::kEnd;
  };

  static_assert(sizeof(PerShardData) == 64);  // cacheline

  // State of a multi transaction.
  struct MultiData {
    // Increase lock counts for all current keys for mode. Clear keys.
    void AddLocks(IntentLock::Mode mode);

    // Whether it locks incrementally.
    bool IsIncrLocks() const;

    MultiMode mode;

    absl::flat_hash_map<std::string, LockCnt> lock_counts;
    std::vector<std::string> keys;

    // The shard_journal_write vector variable is used to determine the number of shards
    // involved in a multi-command transaction. This information is utilized by replicas when
    // executing multi-command. For every write to a shard journal, the corresponding index in the
    // vector is marked as true.
    absl::InlinedVector<bool, 4> shard_journal_write;

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

  struct PerShardCache {
    bool requested_active = false;  // Activate on shard regardless of presence of keys.
    std::vector<std::string_view> args;
    std::vector<uint32_t> original_index;

    void Clear() {
      requested_active = false;
      args.clear();
      original_index.clear();
    }
  };

 private:
  // Init basic fields and reset re-usable.
  void InitBase(DbIndex dbid, CmdArgList args);

  // Init as a global transaction.
  void InitGlobal();

  // Init with a set of keys.
  void InitByKeys(KeyIndex keys);

  // Build shard index by distributing the arguments by shards based on the key index.
  void BuildShardIndex(KeyIndex keys, bool rev_mapping, std::vector<PerShardCache>* out);

  // Init shard data from shard index.
  void InitShardData(absl::Span<const PerShardCache> shard_index, size_t num_args,
                     bool rev_mapping);

  // Init multi. Record locks if needed.
  void InitMultiData(KeyIndex keys);

  // Store all key index keys in args_. Used only for single shard initialization.
  void StoreKeysInArgs(KeyIndex keys, bool rev_mapping);

  // Generic schedule used from Schedule() and ScheduleSingleHop() on slow path.
  void ScheduleInternal();

  // Schedule if only one shard is active.
  // Returns true if transaction ran out-of-order during the scheduling phase.
  bool ScheduleUniqueShard(EngineShard* shard);

  // Schedule on shards transaction queue.
  // Returns pair(schedule_success, lock_granted)
  // schedule_success is true if transaction was scheduled on db_slice.
  // lock_granted is true if lock was granted for all the keys on this shard.
  // Runs in the shard thread.
  std::pair<bool, bool> ScheduleInShard(EngineShard* shard);

  // Optimized version of RunInShard for single shard uncontended cases.
  void RunQuickie(EngineShard* shard);

  void ExecuteAsync();

  // Adds itself to watched queue in the shard. Must run in that shard thread.
  OpStatus WatchInShard(ArgSlice keys, EngineShard* shard);

  void UnwatchBlocking(bool should_expire, WaitKeysProvider wcb);

  // Returns true if we need to follow up with PollExecution on this shard.
  bool CancelShardCb(EngineShard* shard);

  void UnwatchShardCb(ArgSlice wkeys, bool should_expire, EngineShard* shard);

  void UnlockMultiShardCb(const std::vector<KeyList>& sharded_keys, EngineShard* shard,
                          uint32_t shard_journals_cnt);

  // In a multi-command transaction, we determine the number of shard journals that we wrote entries
  // to by updating the shard_journal_write vector during command execution. The total number of
  // shard journals written to can be found by summing the true values in the vector. This value is
  // then written to each shard journal with the journal EXEC op, enabling replication to
  // synchronize the multi-shard transaction.
  uint32_t CalcMultiNumOfShardJournals() const;

  void WaitForShardCallbacks() {
    run_ec_.await([this] { return 0 == run_count_.load(std::memory_order_relaxed); });

    seqlock_.fetch_add(1, std::memory_order_release);
  }

  // Log command in shard's journal, if this is a write command with auto-journaling enabled.
  // Should be called immediately after the last phase (hop).
  void LogAutoJournalOnShard(EngineShard* shard);

  // Returns the previous value of run count.
  uint32_t DecreaseRunCnt();

  uint32_t GetUseCount() const {
    return use_count_.load(std::memory_order_relaxed);
  }

  // Whether the transaction is multi and runs in an atomic mode.
  // This, instead of just IsMulti(), should be used to check for the possibility of
  // different optimizations, because they can safely be applied to non-atomic multi
  // transactions as well.
  bool IsAtomicMulti() const {
    return multi_ && multi_->mode != NON_ATOMIC;
  }

  unsigned SidToId(ShardId sid) const {
    return sid < shard_data_.size() ? sid : 0;
  }

  // Iterate over shards and run function accepting (PerShardData&, ShardId) on all active ones.
  template <typename F> void IterateActiveShards(F&& f) {
    if (!global_ && unique_shard_cnt_ == 1) {  // unique_shard_id_ is set only for non-global.
      auto i = unique_shard_id_;
      f(shard_data_[SidToId(i)], i);
    } else {
      for (ShardId i = 0; i < shard_data_.size(); ++i) {
        if (auto& sd = shard_data_[i]; global_ || (sd.local_mask & ACTIVE)) {
          f(sd, i);
        }
      }
    }
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

  // True if NO_AUTOJOURNAL command asked to enable auto journal
  std::atomic<bool> renabled_auto_journal_ = false;

  // Reverse argument mapping for ReverseArgIndex to convert from shard index to original index.
  std::vector<uint32_t> reverse_index_;

  RunnableType cb_;                   // Run on shard threads
  const CommandId* cid_;              // Underlying command
  std::unique_ptr<MultiData> multi_;  // Initialized when the transaction is multi/exec.

  TxId txid_{0};
  bool global_{false};
  DbIndex db_index_{0};
  uint64_t time_now_ms_{0};

  std::atomic<TxId> notify_txid_{kuint64max};
  std::atomic_uint32_t use_count_{0}, run_count_{0}, seqlock_{0};

  // unique_shard_cnt_ and unique_shard_id_ are accessed only by coordinator thread.
  uint32_t unique_shard_cnt_{0};  // number of unique shards span by args_
  ShardId unique_shard_id_{kInvalidSid};

  util::fibers_ext::EventCount blocking_ec_;  // Used to wake blocking transactions.
  util::fibers_ext::EventCount run_ec_;       // Used to wait for shard callbacks

  // MVP
  bool non_blocking_;
  StubMode stub_;

  // Transaction coordinator state, written and read by coordinator thread.
  // Can be read by shard threads as long as we respect ordering rules, i.e. when
  // they read this variable the coordinator thread is stalled and can not cause data races.
  // If COORDINATOR_XXX has been set, it means we passed or crossed stage XXX.
  uint8_t coordinator_state_ = 0;
  uint32_t coordinator_index_;  // thread_index of the coordinator thread.

  // Used for single-hop transactions with unique_shards_ == 1, hence no data-race.
  OpStatus local_result_ = OpStatus::OK;

 private:
  struct TLTmpSpace {
    absl::flat_hash_set<std::string_view> uniq_keys;

    std::vector<PerShardCache>& GetShardIndex(unsigned size);

   private:
    std::vector<PerShardCache> shard_cache;
  };

  static thread_local TLTmpSpace tmp_space;
};

template <typename F> auto Transaction::ScheduleSingleHopT(F&& f) -> decltype(f(this, nullptr)) {
  decltype(f(this, nullptr)) res;

  ScheduleSingleHop([&res, f = std::forward<F>(f)](Transaction* t, EngineShard* shard) {
    res = f(t, shard);
    return res.status();
  });
  return res;
}

inline uint16_t trans_id(const Transaction* ptr) {
  return (intptr_t(ptr) >> 8) & 0xFFFF;
}

OpResult<KeyIndex> DetermineKeys(const CommandId* cid, CmdArgList args);

}  // namespace dfly
