// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/base/internal/spinlock.h>
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <absl/container/inlined_vector.h>
#include <absl/functional/function_ref.h>

#include <atomic>
#include <string_view>
#include <variant>
#include <vector>

#include "core/intent_lock.h"
#include "core/tx_queue.h"
#include "facade/op_status.h"
#include "server/common.h"
#include "server/journal/types.h"
#include "server/namespaces.h"
#include "server/table.h"
#include "server/tx_base.h"
#include "util/fibers/synchronization.h"

namespace dfly {

class EngineShard;
class BlockingController;
class DbSlice;

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
  // Result returned by callbacks. Most should use the implicit conversion from OpStatus.
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
  using WaitKeysProvider =
      std::function<std::variant<ShardArgs, ArgSlice>(Transaction*, EngineShard* shard)>;

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
    ACTIVE = 1,  // Whether its active on this shard (to schedule or execute hops)
    OPTIMISTIC_EXECUTION = 1 << 7,  // Whether the shard executed optimistically (during schedule)
    // Whether it can run out of order. Undefined if KEYLOCK_ACQUIRED isn't set
    OUT_OF_ORDER = 1 << 2,
    // Whether its key locks are acquired, never set for global commands.
    KEYLOCK_ACQUIRED = 1 << 3,
    SUSPENDED_Q = 1 << 4,   // Whether it suspended (by WatchInShard())
    AWAKED_Q = 1 << 5,      // Whether it was awakened (by NotifySuspended())
    UNLOCK_MULTI = 1 << 6,  // Whether this shard executed UnlockMultiShardCb
  };

  struct Guard {
    explicit Guard(Transaction* tx);
    ~Guard();

   private:
    Transaction* tx;
  };

  static void Init(unsigned num_shards);
  static void Shutdown();

  explicit Transaction(const CommandId* cid);

  // Initialize transaction for squashing placed on a specific shard with a given parent tx
  explicit Transaction(const Transaction* parent, ShardId shard_id, std::optional<SlotId> slot_id);

  // Initialize from command (args) on specific db.
  OpStatus InitByArgs(Namespace* ns, DbIndex index, CmdArgList args);

  // Get command arguments for specific shard. Called from shard thread.
  ShardArgs GetShardArgs(ShardId sid) const;

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
  facade::OpStatus WaitOnWatch(const time_point& tp, WaitKeysProvider cb, KeyReadyChecker krc,
                               bool* block_flag, bool* pause_flag);

  // Returns true if transaction is awaked, false if it's timed-out and can be removed from the
  // blocking queue.
  bool NotifySuspended(TxId committed_ts, ShardId sid, std::string_view key);

  // Cancel all blocking watches. Set COORD_CANCELLED.
  // Must be called from coordinator thread.
  void CancelBlocking(std::function<OpStatus(ArgSlice)>);

  // Prepare a squashed hop on given shards.
  // Only compatible with multi modes that acquire all locks ahead - global and lock_ahead.
  void PrepareSquashedMultiHop(const CommandId* cid, absl::FunctionRef<bool(ShardId)> enabled);

  // Start multi in GLOBAL mode.
  void StartMultiGlobal(Namespace* ns, DbIndex dbid);

  // Start multi in LOCK_AHEAD mode with given keys.
  void StartMultiLockedAhead(Namespace* ns, DbIndex dbid, CmdArgList keys,
                             bool skip_scheduling = false);

  // Start multi in NON_ATOMIC mode.
  void StartMultiNonAtomic();

  // Unlock key locks of a multi transaction.
  void UnlockMulti();

  // Set new command for multi transaction.
  void MultiSwitchCmd(const CommandId* cid);

  // Copy txid, time and unique slot from parent
  void MultiUpdateWithParent(const Transaction* parent);

  // Set squasher role
  void MultiBecomeSquasher();

  // Returns locking arguments needed for DbSlice to Acquire/Release transactional locks.
  // Runs in the shard thread.
  KeyLockArgs GetLockArgs(ShardId sid) const;

  // If the transaction is armed, disarm it and return the local mask (ACTIVE is always set).
  // Otherwise 0 is returned. Sync point (acquire).
  uint16_t DisarmInShard(ShardId sid);

  // Same as DisarmInShard, but the transaction is only disarmed if any of the req_flags is present.
  // If the transaction is armed, returns the local mask and a flag whether it was disarmed.
  std::pair<uint16_t, bool /* disarmed */> DisarmInShardWhen(ShardId sid, uint16_t req_flags);

  // Returns if the transaction spans this shard. Safe only when the transaction is armed.
  bool IsActive(ShardId sid) const;

  // If blocking tx was woken up on this shard, get wake key.
  std::optional<std::string_view> GetWakeKey(ShardId sid) const;

  // Get OpArgs for specific shard
  OpArgs GetOpArgs(EngineShard* shard) const;

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

  // Whether the transaction is multi and runs in an atomic mode.
  // This, instead of just IsMulti(), should be used to check for the possibility of
  // different optimizations, because they can safely be applied to non-atomic multi
  // transactions as well.
  bool IsAtomicMulti() const {
    return multi_ && (multi_->mode == LOCK_AHEAD || multi_->mode == GLOBAL);
  }

  bool IsGlobal() const;

  DbContext GetDbContext() const {
    return DbContext{namespace_, db_index_, time_now_ms_};
  }

  Namespace& GetNamespace() const {
    return *namespace_;
  }

  DbSlice& GetDbSlice(ShardId sid) const;

  DbIndex GetDbIndex() const {
    return db_index_;
  }

  const CommandId* GetCId() const {
    return cid_;
  }

  // Return debug information about a transaction, include shard local info if passed
  std::string DebugId(std::optional<ShardId> sid = std::nullopt) const;

  // Prepares for running ScheduleSingleHop() for a single-shard multi tx.
  // It is safe to call ScheduleSingleHop() after calling this method, but the callback passed
  // to it must not block.
  void PrepareMultiForScheduleSingleHop(Namespace* ns, ShardId sid, DbIndex db, CmdArgList args);

  // Write a journal entry to a shard journal with the given payload.
  void LogJournalOnShard(EngineShard* shard, journal::Entry::Payload&& payload,
                         uint32_t shard_cnt) const;

  // Re-enable auto journal for commands marked as NO_AUTOJOURNAL. Call during setup.
  void ReviveAutoJournal();

  // Clear all state to make transaction re-usable
  void Refurbish();

  // Get keys multi transaction was initialized with, normalized and unique
  const absl::flat_hash_set<std::pair<ShardId, LockFp>>& GetMultiFps() const;

  // Print in-dept failure state for debugging.
  std::string DEBUG_PrintFailState(ShardId sid) const;

  uint32_t DEBUG_GetTxqPosInShard(ShardId sid) const {
    return shard_data_[SidToId(sid)].pq_pos;
  }

  bool DEBUG_IsArmedInShard(ShardId sid) const {
    return shard_data_[SidToId(sid)].is_armed.load(std::memory_order_relaxed);
  }

  uint16_t DEBUG_GetLocalMask(ShardId sid) const {
    return shard_data_[SidToId(sid)].local_mask;
  }

  void SetTrackingCallback(std::function<void(Transaction* trans)> f) {
    tracking_cb_ = std::move(f);
  }

  void MaybeInvokeTrackingCb() {
    if (tracking_cb_) {
      tracking_cb_(this);
    }
  }

  // Remove once BZPOP is stabilized
  std::string DEBUGV18_BlockInfo() {
    return "claimed=" + std::to_string(blocking_barrier_.IsClaimed()) +
           " coord_state=" + std::to_string(int(coordinator_state_)) +
           " local_res=" + std::to_string(int(local_result_));
  }

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
    PerShardData() {
    }
    PerShardData(PerShardData&& other) noexcept {
    }

    // State of shard - bitmask with LocalState flags
    uint16_t local_mask = 0;

    // Set when the shard is prepared for another hop. Sync point. Cleared when execution starts.
    std::atomic_bool is_armed = false;

    uint32_t slice_start = 0;  // Subspan in kv_args_ with local arguments.
    uint32_t slice_count = 0;

    // span into kv_fp_
    uint32_t fp_start = 0;
    uint32_t fp_count = 0;

    // Position in the tx queue. OOO or cancelled schedules remove themselves by this index.
    TxQueue::Iterator pq_pos = TxQueue::kEnd;

    // Index of key relative to args in shard that the shard was woken up after blocking wait.
    uint32_t wake_key_pos = UINT32_MAX;

    // Irrational stats purely for debugging purposes.
    struct Stats {
      unsigned total_runs = 0;  // total number of runs
    } stats;

    // Prevent "false sharing" between cache lines: occupy a full cache line (64 bytes)
    char pad[64 - 7 * sizeof(uint32_t) - sizeof(Stats)];
  };

  static_assert(sizeof(PerShardData) == 64);  // cacheline

  // State of a multi transaction.
  struct MultiData {
    MultiRole role;
    MultiMode mode;
    std::optional<IntentLock::Mode> lock_mode;

    // Unique normalized fingerprints used for scheduling the multi transaction.
    absl::flat_hash_set<std::pair<ShardId, LockFp>> tag_fps;

    // Set if the multi command is concluding to avoid ambiguity with COORD_CONCLUDING
    bool concluding = false;

    unsigned cmd_seq_num = 0;  // used for debugging purposes.
  };

  enum CoordinatorState : uint8_t {
    COORD_SCHED = 1,
    COORD_CONCLUDING = 1 << 1,  // Whether its the last hop of a transaction
    COORD_CANCELLED = 1 << 2,
  };

  // Auxiliary structure used during initialization
  struct PerShardCache {
    std::vector<IndexSlice> slices;
    unsigned key_step = 1;

    void Clear() {
      slices.clear();
    }
  };

  // "Single claim - single modification" barrier. Multiple threads might try to claim it, only one
  // will succeed and will be allowed to modify the guarded object until it closes the barrier.
  // A closed barrier can't be claimed again or re-used in any way.
  class BatonBarrier {
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
    util::fb2::EventCount ec_{};
  };

  // Init basic fields and reset re-usable.
  void InitBase(Namespace* ns, DbIndex dbid, CmdArgList args);

  // Init as a global transaction.
  void InitGlobal();

  // Init with a set of keys.
  void InitByKeys(const KeyIndex& keys);

  void EnableShard(ShardId sid);
  void EnableAllShards();

  // Build shard index by distributing the arguments by shards based on the key index.
  void BuildShardIndex(const KeyIndex& keys, std::vector<PerShardCache>* out);

  // Init shard data from shard index.
  void InitShardData(absl::Span<const PerShardCache> shard_index, size_t num_args);

  // Store all key index keys in args_. Used only for single shard initialization.
  void StoreKeysInArgs(const KeyIndex& key_index);

  // Multi transactions unlock asynchronously, so they need to keep fingerprints of keys.
  void PrepareMultiFps(CmdArgList keys);

  void ScheduleInternal();

  // Schedule on shards transaction queue. Returns true if scheduled successfully,
  // false if inconsistent order was detected and the schedule needs to be cancelled.
  // if execute_optimistic is true - means we can try executing during the scheduling,
  // subject to uncontended keys.
  bool ScheduleInShard(EngineShard* shard, bool execute_optimistic);

  // Optimized extension of ScheduleInShard. Pulls several transactions queued for scheduling.
  static void ScheduleBatchInShard();

  // Set ARMED flags, start run barrier and submit poll tasks. Doesn't wait for the run barrier
  void DispatchHop();

  // Finish hop, decrement run barrier
  void FinishHop();

  // Run actual callback on shard, store result if single shard or OOM was catched
  void RunCallback(EngineShard* shard);

  // Adds itself to watched queue in the shard. Must run in that shard thread.
  OpStatus WatchInShard(Namespace* ns, std::variant<ShardArgs, ArgSlice> keys, EngineShard* shard,
                        KeyReadyChecker krc);

  // Expire blocking transaction, unlock keys and unregister it from the blocking controller
  void ExpireBlocking(WaitKeysProvider wcb);

  void ExpireShardCb(std::variant<ShardArgs, ArgSlice> keys, EngineShard* shard);

  // Returns true if we need to follow up with PollExecution on this shard.
  bool CancelShardCb(EngineShard* shard);

  // Run callback inline as part of multi stub.
  OpStatus RunSquashedMultiCb(RunnableType cb);

  // Set time_now_ms_
  void InitTxTime();

  void UnlockMultiShardCb(absl::Span<const LockFp> fps, EngineShard* shard);

  // In a multi-command transaction, we determine the number of shard journals that we wrote entries
  // to by updating the shard_journal_write vector during command execution. The total number of
  // shard journals written to can be found by summing the true values in the vector. This value is
  // then written to each shard journal with the journal EXEC op, enabling replication to
  // synchronize the multi-shard transaction.
  uint32_t CalcMultiNumOfShardJournals() const;

  // Log command in shard's journal, if this is a write command with auto-journaling enabled.
  // Should be called immediately after the last hop.
  void LogAutoJournalOnShard(EngineShard* shard, RunnableResult shard_result);

  // Whether the callback can be run directly on this thread without dispatching on the shard queue
  bool CanRunInlined() const;

  uint32_t GetUseCount() const {
    return use_count_.load(std::memory_order_relaxed);
  }

  bool IsActiveMulti() const {
    return multi_ && multi_->role != SQUASHED_STUB;
  }

  unsigned SidToId(ShardId sid) const {
    return sid < shard_data_.size() ? sid : 0;
  }

  // Iterate over all available shards, run functor accepting (PerShardData&, ShardId)
  template <typename F> void IterateShards(F&& f) {
    if (unique_shard_cnt_ == 1) {
      f(shard_data_[SidToId(unique_shard_id_)], unique_shard_id_);
    } else {
      for (ShardId i = 0; i < shard_data_.size(); ++i) {
        f(shard_data_[i], i);
      }
    }
  }

  // Iterate over ACTIVE shards, run functor accepting (PerShardData&, ShardId)
  template <typename F> void IterateActiveShards(F&& f) {
    IterateShards([&f](auto& sd, auto i) {
      if (sd.local_mask & ACTIVE)
        f(sd, i);
    });
  }

  // Used for waiting for all hop callbacks to run.
  util::fb2::EmbeddedBlockingCounter run_barrier_{0};

  // Stores per-shard data: state flags and keys. Index only with SidToId(shard index)!
  // Theoretically, same size as number of shards, but contains only a single element for
  // single shard non-multi transactions (optimization).
  // TODO: explore dense packing
  absl::InlinedVector<PerShardData, 4> shard_data_;

  // Stores slices of key/values partitioned by shards.
  // Slices reference full_args_.
  // We need values as well since we reorder keys, and we need to know what value corresponds
  // to what key.
  absl::InlinedVector<IndexSlice, 4> args_slices_;

  // Fingerprints of keys, precomputed once during the transaction initialization.
  absl::InlinedVector<LockFp, 4> kv_fp_;

  // Stores the full undivided command.
  CmdArgList full_args_;

  // Set if a NO_AUTOJOURNAL command asked to enable auto journal again
  bool re_enabled_auto_journal_ = false;

  RunnableType* cb_ptr_ = nullptr;    // Run on shard threads
  const CommandId* cid_ = nullptr;    // Underlying command
  std::unique_ptr<MultiData> multi_;  // Initialized when the transaction is multi/exec.

  TxId txid_{0};
  bool global_{false};
  Namespace* namespace_{nullptr};
  DbIndex db_index_{0};
  uint64_t time_now_ms_{0};

  std::atomic_uint32_t use_count_{0};  // transaction exists only as an intrusive_ptr

  uint32_t unique_shard_cnt_{0};          // Number of unique shards active
  ShardId unique_shard_id_{kInvalidSid};  // Set if unique_shard_cnt_ = 1
  UniqueSlotChecker unique_slot_checker_;

  // Barrier for waking blocking transactions that ensures exclusivity of waking operation.
  BatonBarrier blocking_barrier_{};
  // Stores status if COORD_CANCELLED was set. Apart from cancelled, it can be moved for cluster
  // changes
  OpStatus block_cancel_result_ = OpStatus::OK;

  // Transaction coordinator state, written and read by coordinator thread.
  uint8_t coordinator_state_ = 0;

  // Result of callbacks. Usually written by single shard only, lock below for multishard oom error
  OpStatus local_result_ = OpStatus::OK;
  absl::base_internal::SpinLock local_result_mu_;

  // Stats purely for debugging purposes
  struct Stats {
    size_t schedule_attempts = 0;
    ShardId coordinator_index = 0;
  } stats_;

  std::function<void(Transaction* trans)> tracking_cb_;

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

OpResult<KeyIndex> DetermineKeys(const CommandId* cid, CmdArgList args);

}  // namespace dfly
