// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/base/internal/spinlock.h>
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <absl/container/inlined_vector.h>
#include <absl/functional/function_ref.h>

#include <string_view>
#include <variant>
#include <vector>

#include "core/intent_lock.h"
#include "core/tx_queue.h"
#include "facade/op_status.h"
#include "server/cluster/unique_slot_checker.h"
#include "server/common.h"
#include "server/journal/types.h"
#include "server/table.h"

namespace dfly {

class EngineShard;
class BlockingController;

using facade::OpResult;
using facade::OpStatus;

// Central building block of the transactional framework.
//
// Use it to run callbacks on the shard threads - such dispatches are called hops.
//
// Callbacks are not allowed to keep any possibly dangling pointers to data within the shards - it
// must be copied explicitly. The callbacks running on different threads should also never pass any
// messages or wait for each other, as it would block the execution of other transactions.
//
// The shards to run on are determined by the keys of the underlying command.
// Global transactions run on all shards.
//
// Use ScheduleSingleHop() if only a single hop is needed.
// Otherwise, schedule the transaction with Schedule() and run successive hops
// with Execute().
//
// 1. Multi transactions
//
// Multi transactions are handled by a single transaction, which exposes the same interface for
// commands as regular transactions, but internally avoids rescheduling. There are multiple modes in
// which a mutli-transaction can run, those are documented in the MultiMode enum.
//
// The flow of EXEC and EVAL is as follows:
//
// ```
// trans->StartMulti_MultiMode_()
// for ([cmd, args]) {
//   trans->MultiSwitchCmd(cmd)  // 1. Set new command
//   trans->InitByArgs(args)     // 2. Re-initialize with arguments
//   cmd->Invoke(trans)          // 3. Run
// }
// trans->UnlockMulti()
// ```
//
// 2. Multi squashing
//
// An important optimization for multi transactions is executing multiple single shard commands in
// parallel. Because multiple commands are "squashed" into a single hop, its called multi squashing.
// To mock the interface for commands, special "stub" transactions are created for each shard that
// directly execute hop callbacks without any scheduling. Transaction roles are represented by the
// MultiRole enum. See MultiCommandSquasher for the detailed squashing approach.
//
// The flow is as follows:
//
// ```
// for (cmd in single_shard_sequence)
//   sharded[shard].push_back(cmd)
//
// tx->PrepareSquashedMultiHop()
// tx->ScheduleSingleHop({
//   Transaction stub_tx {tx}
//   for (cmd)
//     // use stub_tx as regular multi tx, see 1. above
// })
//
// ```
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
  // Result returned by callbacks. Most should use the implcit conversion from OpStatus.
  struct RunnableResult {
    enum Flag : uint16_t {
      // Can be issued by a **single** shard callback to avoid concluding, i.e. perform one more hop
      // even if not requested ahead. Used for blocking command fallback.
      AVOID_CONCLUDING = 1,
    };

    RunnableResult(OpStatus status = OpStatus::OK, uint16_t flags = 0)
        : status(status), flags(flags) {
    }

    operator OpStatus() const {
      return status;
    }

    OpStatus status;
    uint16_t flags;
  };

  static_assert(sizeof(RunnableResult) == 4);

  using time_point = ::std::chrono::steady_clock::time_point;
  // Runnable that is run on shards during hop executions (often named callback).
  // Callacks should return `OpStatus` which is implicitly converitble to `RunnableResult`!
  using RunnableType = absl::FunctionRef<RunnableResult(Transaction* t, EngineShard*)>;
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
    // Each command is executed separately. Equivalent to a pipeline.
    NON_ATOMIC = 3,
  };

  // Squashed parallel execution requires a separate transaction for each shard. Those "stubs"
  // perform no scheduling or real hops, but instead execute the handlers directly inline.
  enum MultiRole {
    DEFAULT = 0,        // Regular multi transaction
    SQUASHER = 1,       // Owner of stub transactions
    SQUASHED_STUB = 2,  // Stub transaction
  };

  // State on specific shard.
  enum LocalMask : uint16_t {
    ACTIVE = 1,  // Set on all active shards.
    OUT_OF_ORDER =
        1 << 2,  // Whether it can run out of order. Undefined if KEYLOCK_ACQUIRED is not set.
    KEYLOCK_ACQUIRED = 1 << 3,  // Whether its key locks are acquired
    SUSPENDED_Q = 1 << 4,       // Whether is suspended (by WatchInShard())
    AWAKED_Q = 1 << 5,          // Whether it was awakened (by NotifySuspended())
    UNLOCK_MULTI = 1 << 6,      // Whether this shard executed UnlockMultiShardCb
  };

 public:
  explicit Transaction(const CommandId* cid);

  // Initialize transaction for squashing placed on a specific shard with a given parent tx
  explicit Transaction(const Transaction* parent, ShardId shard_id, std::optional<SlotId> slot_id);

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

  // Conclude transaction. Ignored if not scheduled
  void Conclude();

  // Called by engine shard to execute a transaction hop.
  // txq_ooo is set to true if the transaction is running out of order
  // not as the tx queue head.
  // Returns true if the transaction continues running in the thread
  bool RunInShard(EngineShard* shard, bool txq_ooo);

  // Registers transaction into watched queue and blocks until a) either notification is received.
  // or b) tp is reached. If tp is time_point::max() then waits indefinitely.
  // Expects that the transaction had been scheduled before, and uses Execute(.., true) to register.
  // Returns false if timeout occurred, true if was notified by one of the keys.
  facade::OpStatus WaitOnWatch(const time_point& tp, WaitKeysProvider cb, KeyReadyChecker krc);

  // Returns true if transaction is awaked, false if it's timed-out and can be removed from the
  // blocking queue.
  bool NotifySuspended(TxId committed_ts, ShardId sid, std::string_view key);

  // Cancel all blocking watches. Set COORD_CANCELLED.
  // Must be called from coordinator thread.
  void CancelBlocking(std::function<OpStatus(ArgSlice)>);

  // In some cases for non auto-journaling commands we want to enable the auto journal flow.
  void RenableAutoJournal() {
    renabled_auto_journal_.store(true, std::memory_order_relaxed);
  }

  // Prepare a squashed hop on given shards.
  // Only compatible with multi modes that acquire all locks ahead - global and lock_ahead.
  void PrepareSquashedMultiHop(const CommandId* cid, absl::FunctionRef<bool(ShardId)> enabled);

  // Start multi in GLOBAL mode.
  void StartMultiGlobal(DbIndex dbid);

  // Start multi in LOCK_AHEAD mode with given keys.
  void StartMultiLockedAhead(DbIndex dbid, CmdArgVec keys);

  // Start multi in NON_ATOMIC mode.
  void StartMultiNonAtomic();

  // Report which shards had write commands that executed on stub transactions
  // and thus did not mark itself in MultiData::shard_journal_write.
  void ReportWritesSquashedMulti(absl::FunctionRef<bool(ShardId)> had_write);

  // Unlock key locks of a multi transaction.
  void UnlockMulti();

  // Set new command for multi transaction.
  void MultiSwitchCmd(const CommandId* cid);

  // Returns locking arguments needed for DbSlice to Acquire/Release transactional locks.
  // Runs in the shard thread.
  KeyLockArgs GetLockArgs(ShardId sid) const;

  // Returns true if the transaction spans this shard_id.
  bool IsActive(ShardId shard_id) const;

  // Returns true if the transaction is waiting for shard callbacks and the shard is armed.
  // Safe to read transaction state (and update shard local) until following RunInShard() finishes.
  bool IsArmedInShard(ShardId sid) const {
    if (sid >= shard_data_.size())  // For multi transactions shard_data_ spans all shards.
      sid = 0;

    // Barrier has acquire semantics
    return run_barrier_.Active() && shard_data_[sid].is_armed.load(std::memory_order_relaxed);
  }

  // Called from engine set shard threads.
  uint16_t GetLocalMask(ShardId sid) const {
    return shard_data_[SidToId(sid)].local_mask;
  }

  uint32_t GetLocalTxqPos(ShardId sid) const {
    return shard_data_[SidToId(sid)].pq_pos;
  }

  TxId txid() const {
    return txid_;
  }

  IntentLock::Mode LockMode() const;  // Based on command mask

  std::string_view Name() const;  // Based on command name

  uint32_t GetUniqueShardCnt() const {
    return unique_shard_cnt_;
  }

  // This method is meaningless if GetUniqueShardCnt() != 1.
  ShardId GetUniqueShard() const;

  std::optional<SlotId> GetUniqueSlotId() const;

  bool IsMulti() const {
    return bool(multi_);
  }

  bool IsScheduled() const {
    return coordinator_state_ & COORD_SCHED;
  }

  MultiMode GetMultiMode() const {
    return multi_->mode;
  }

  bool IsGlobal() const;

  // If blocking tx was woken up on this shard, get wake key.
  std::optional<std::string_view> GetWakeKey(ShardId sid) const;

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

  // Prepares for running ScheduleSingleHop() for a single-shard multi tx.
  // It is safe to call ScheduleSingleHop() after calling this method, but the callback passed
  // to it must not block.
  void PrepareMultiForScheduleSingleHop(ShardId sid, DbIndex db, CmdArgList args);

  // Write a journal entry to a shard journal with the given payload. When logging a non-automatic
  // journal command, multiple journal entries may be necessary. In this case, call with set
  // multi_commands to true and  call the FinishLogJournalOnShard function after logging the final
  // entry.
  void LogJournalOnShard(EngineShard* shard, journal::Entry::Payload&& payload, uint32_t shard_cnt,
                         bool multi_commands, bool allow_await) const;
  void FinishLogJournalOnShard(EngineShard* shard, uint32_t shard_cnt) const;

  void Refurbish();

  // Get keys multi transaction was initialized with, normalized and unique
  const absl::flat_hash_set<std::string_view>& GetMultiKeys() const;

  // Send journal EXEC opcode after a series of MULTI commands on the currently active shard
  void FIX_ConcludeJournalExec();

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

  struct alignas(64) PerShardData {
    PerShardData(PerShardData&&) noexcept {
    }

    PerShardData() = default;

    // this is the only variable that is accessed by both shard and coordinator threads.
    std::atomic_bool is_armed{false};

    // We pad with some memory so that atomic loads won't cause false sharing between threads.
    char pad[46];  // to make sure PerShardData is 64 bytes and takes full cacheline.

    uint32_t arg_start = 0;  // Indices into args_ array.
    uint32_t arg_count = 0;

    // Needed to rollback inconsistent schedulings or remove OOO transactions from
    // tx queue.
    uint32_t pq_pos = TxQueue::kEnd;

    // Accessed within shard thread.
    // Bitmask of LocalMask enums.
    uint16_t local_mask = 0;

    // Index of key relative to args in shard that the shard was woken up after blocking wait.
    uint16_t wake_key_pos = UINT16_MAX;
  };

  static_assert(sizeof(PerShardData) == 64);  // cacheline

  // State of a multi transaction.
  struct MultiData {
    MultiRole role;
    MultiMode mode;
    std::optional<IntentLock::Mode> lock_mode;

    // Unique normalized keys used for scheduling the multi transaction.
    std::vector<std::string> frozen_keys;
    absl::flat_hash_set<std::string_view> frozen_keys_set;  // point to frozen_keys

    // Set if the multi command is concluding to avoid ambiguity with COORD_CONCLUDING
    bool concluding = false;

    // The shard_journal_write vector variable is used to determine the number of shards
    // involved in a multi-command transaction. This information is utilized by replicas when
    // executing multi-command. For every write to a shard journal, the corresponding index in the
    // vector is marked as true.
    absl::InlinedVector<bool, 4> shard_journal_write;
    unsigned cmd_seq_num = 0;  // used for debugging purposes.
  };

  enum CoordinatorState : uint8_t {
    COORD_SCHED = 1,
    COORD_CONCLUDING = 1 << 1,  // Whether its the last hop of a transaction
    COORD_CANCELLED = 1 << 2,
  };

  // Auxiliary structure used during initialization
  struct PerShardCache {
    std::vector<std::string_view> args;
    std::vector<uint32_t> original_index;

    void Clear() {
      args.clear();
      original_index.clear();
    }
  };

  // Barrier akin to helio's BlockingCounter, but with proper acquire semantics
  // for polling work from other threads (active, inactive phases). And without heap allocation.
  class PhasedBarrier {
   public:
    void Start(uint32_t count);  // Release: Store count
    void Wait();                 // Acquire: Wait until count = 0

    bool Active() const;                // Acquire: Return if count > 0. Use for polling for work
    void Dec(Transaction* keep_alive);  // Release: Decrease count, notify ec on count = 0

    uint32_t DEBUG_Count() const;  // Get current counter value
   private:
    std::atomic_uint32_t count_{0};
    EventCount ec_{};
  };

  // "Single claim - single modification" barrier. Multiple threads might try to claim it, only one
  // will succeed and will be allowed to modify the guarded object until it closes the barrier.
  // A closed barrier can't be claimed again or re-used in any way.
  class BatonBarrierrier {
   public:
    bool IsClaimed() const;  // Return if barrier is claimed, only for peeking
    bool TryClaim();         // Return if the barrier was claimed successfully
    void Close();            // Close barrier after it was claimed

    // Wait for barrier until time_point, or indefinitely if time_point::max() was passed.
    // After Wait returns, the barrier is guaranteed to be closed, including expiration.
    std::cv_status Wait(time_point);

   private:
    std::atomic_bool claimed_{false};
    std::atomic_bool closed_{false};
    EventCount ec_{};
  };

 private:
  // Init basic fields and reset re-usable.
  void InitBase(DbIndex dbid, CmdArgList args);

  // Init as a global transaction.
  void InitGlobal();

  // Init with a set of keys.
  void InitByKeys(const KeyIndex& keys);

  void EnableShard(ShardId sid);
  void EnableAllShards();

  // Build shard index by distributing the arguments by shards based on the key index.
  void BuildShardIndex(const KeyIndex& keys, std::vector<PerShardCache>* out);

  // Init shard data from shard index.
  void InitShardData(absl::Span<const PerShardCache> shard_index, size_t num_args,
                     bool rev_mapping);

  // Store all key index keys in args_. Used only for single shard initialization.
  void StoreKeysInArgs(const KeyIndex& key_index);

  // Multi transactions unlock asynchronously, so they need to keep a copy of all they keys.
  // "Launder" keys by filtering uniques and replacing pointers with same lifetime as transaction.
  void LaunderKeyStorage(CmdArgVec* keys);

  // Generic schedule used from Schedule() and ScheduleSingleHop() on slow path.
  void ScheduleInternal();

  // Schedule if only one shard is active.
  // Returns true if transaction ran out-of-order during the scheduling phase.
  bool ScheduleUniqueShard(EngineShard* shard);

  // Schedule on shards transaction queue. Returns true if scheduled successfully,
  // false if inconsistent order was detected and the schedule needs to be cancelled.
  bool ScheduleInShard(EngineShard* shard);

  // Optimized version of RunInShard for single shard uncontended cases.
  RunnableResult RunQuickie(EngineShard* shard);

  // Set ARMED flags, start run barrier and submit poll tasks. Doesn't wait for the run barrier
  void ExecuteAsync();

  // Adds itself to watched queue in the shard. Must run in that shard thread.
  OpStatus WatchInShard(ArgSlice keys, EngineShard* shard, KeyReadyChecker krc);

  // Expire blocking transaction, unlock keys and unregister it from the blocking controller
  void ExpireBlocking(WaitKeysProvider wcb);

  void ExpireShardCb(ArgSlice wkeys, EngineShard* shard);

  // Returns true if we need to follow up with PollExecution on this shard.
  bool CancelShardCb(EngineShard* shard);

  // Run callback inline as part of multi stub.
  OpStatus RunSquashedMultiCb(RunnableType cb);

  void UnlockMultiShardCb(absl::Span<const std::string_view> sharded_keys, EngineShard* shard,
                          uint32_t shard_journals_cnt);

  // In a multi-command transaction, we determine the number of shard journals that we wrote entries
  // to by updating the shard_journal_write vector during command execution. The total number of
  // shard journals written to can be found by summing the true values in the vector. This value is
  // then written to each shard journal with the journal EXEC op, enabling replication to
  // synchronize the multi-shard transaction.
  uint32_t CalcMultiNumOfShardJournals() const;

  // Log command in shard's journal, if this is a write command with auto-journaling enabled.
  // Should be called immediately after the last hop.
  void LogAutoJournalOnShard(EngineShard* shard, RunnableResult shard_result);

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

  bool IsActiveMulti() const {
    return multi_ && multi_->role != SQUASHED_STUB;
  }

  unsigned SidToId(ShardId sid) const {
    return sid < shard_data_.size() ? sid : 0;
  }

  // Iterate over shards and run function accepting (PerShardData&, ShardId) on all active ones.
  template <typename F> void IterateActiveShards(F&& f) {
    if (unique_shard_cnt_ == 1) {
      f(shard_data_[SidToId(unique_shard_id_)], unique_shard_id_);
    } else {
      for (ShardId i = 0; i < shard_data_.size(); ++i) {
        if (auto& sd = shard_data_[i]; sd.local_mask & ACTIVE) {
          f(sd, i);
        }
      }
    }
  }

 private:
  // Main synchronization point for dispatching hop callbacks and waiting for them to finish.
  // After scheduling, sequential hops are executed as follows:
  //    coordinator: Prepare hop, then Start(num_shards), dispatch poll jobs and Wait()
  //    tx queue:    Once IsArmedInShard() /* checks Active() */ -> run in shard and Dec()
  // As long as barrier is active, any writes by the coordinator are prohibited, so shard threads
  // can safely read transaction state and modify per-shard state belonging to them.
  // Inter-thread synchronization is provided by the barriers acquire/release pairs.
  PhasedBarrier run_barrier_;

  // Stores per-shard data: state flags and keys. Index only with SidToId(shard index)!
  // Theoretically, same size as number of shards, but contains only a single element for
  // single shard non-multi transactions (optimization).
  // TODO: explore dense packing
  absl::InlinedVector<PerShardData, 4> shard_data_;

  // Stores keys/values of the transaction partitioned by shards.
  // We need values as well since we reorder keys, and we need to know what value corresponds
  // to what key.
  absl::InlinedVector<std::string_view, 4> kv_args_;

  // Stores the full undivided command.
  CmdArgList full_args_;

  // True if NO_AUTOJOURNAL command asked to enable auto journal
  std::atomic<bool> renabled_auto_journal_ = false;

  // Reverse argument mapping for ReverseArgIndex to convert from shard index to original index.
  std::vector<uint32_t> reverse_index_;

  RunnableType* cb_ptr_ = nullptr;    // Run on shard threads
  const CommandId* cid_ = nullptr;    // Underlying command
  std::unique_ptr<MultiData> multi_;  // Initialized when the transaction is multi/exec.

  TxId txid_{0};
  bool global_{false};
  DbIndex db_index_{0};
  uint64_t time_now_ms_{0};

  std::atomic_uint32_t use_count_{0};  // transaction exists only as an intrusive_ptr

  uint32_t unique_shard_cnt_{0};          // Number of unique shards active
  ShardId unique_shard_id_{kInvalidSid};  // Set if unique_shard_cnt_ = 1
  UniqueSlotChecker unique_slot_checker_;

  // Barrier for waking blocking transactions that ensures exclusivity of waking operation.
  BatonBarrierrier blocking_barrier_{};

  // Transaction coordinator state, written and read by coordinator thread.
  uint8_t coordinator_state_ = 0;

  // Result of callbacks. Usually written by single shard only, lock below for multishard oom error
  OpStatus local_result_ = OpStatus::OK;
  absl::base_internal::SpinLock local_result_mu_;

 private:
  struct TLTmpSpace {
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
