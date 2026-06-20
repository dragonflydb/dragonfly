// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/functional/function_ref.h>

#include <atomic>
#include <optional>
#include <string_view>

#include "core/intent_lock.h"
#include "core/tx_queue.h"
#include "facade/op_status.h"
#include "server/cluster_support.h"
#include "server/common.h"
#include "server/journal/types.h"
#include "server/tx_base.h"
#include "util/fibers/synchronization.h"

namespace dfly {

class BlockingController;
class EngineShard;

using facade::OpResult;
using facade::OpStatus;

// Base class for the transaction hierarchy. Provides the common state and virtual interface
// used by the scheduling infrastructure (TxQueue, PollExecution) and command callbacks.
//
// Subclasses:
//   Transaction      - full-featured: multi-shard, multi/exec, blocking, global
//   UniTransaction   - lightweight:   single-shard, non-multi, non-blocking
class TransactionBase {
  friend void intrusive_ptr_add_ref(TransactionBase* trans) noexcept {
    trans->use_count_.fetch_add(1, std::memory_order_relaxed);
  }

  friend void intrusive_ptr_release(TransactionBase* trans) noexcept {
    if (1 == trans->use_count_.fetch_sub(1, std::memory_order_release)) {
      std::atomic_thread_fence(std::memory_order_acquire);
      delete trans;
    }
  }

 public:
  struct RunnableResult {
    enum Flag : uint16_t {
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
  using RunnableType = absl::FunctionRef<RunnableResult(TransactionBase* t, EngineShard*)>;

  static constexpr std::nullopt_t kShardArgs{std::nullopt};
  using WaitKeys = std::optional<std::string_view>;

  enum MultiMode : uint8_t {
    NOT_DETERMINED = 0,
    GLOBAL = 1,
    LOCK_AHEAD = 2,
    NON_ATOMIC = 3,
  };

  enum MultiRole {
    DEFAULT = 0,
    SQUASHER = 1,
    SQUASHED_STUB = 2,
  };

  enum LocalMask : uint16_t {
    ACTIVE = 1,
    OPTIMISTIC_EXECUTION = 1 << 1,
    OUT_OF_ORDER = 1 << 2,
    KEYLOCK_ACQUIRED = 1 << 3,
    WAS_SUSPENDED = 1 << 4,
    AWAKED_Q = 1 << 5,
  };

  virtual ~TransactionBase();

  // --- Non-virtual public getters (hot path, no dispatch overhead) ---

  TxId txid() const {
    return txid_;
  }

  std::string_view Name() const;

  const CommandId* GetCId() const {
    return cid_;
  }

  DbContext GetDbContext() const {
    return DbContext{namespace_, db_index_, time_now_ms_};
  }

  DbIndex GetDbIndex() const {
    return db_index_;
  }

  Namespace& GetNamespace() const {
    return *namespace_;
  }

  IntentLock::Mode LockMode() const;

  bool IsGlobal() const {
    return global_;
  }

  bool IsScheduled() const {
    return coordinator_state_ & COORD_SCHED;
  }

  uint32_t GetUniqueShardCnt() const {
    return unique_shard_cnt_;
  }

  ShardId GetUniqueShard() const;

  util::fb2::EmbeddedBlockingCounter* Blocker() {
    return &run_barrier_;
  }

  OpStatus* LocalResultPtr() {
    return &local_result_;
  }

  DbSlice& GetDbSlice(ShardId shard_id) const;

  // --- Virtual interface (subclass-specific implementations) ---

  virtual OpStatus InitByArgs(Namespace* ns, DbIndex index, CmdArgList args) = 0;

  virtual void Execute(RunnableType cb, bool conclude) = 0;
  virtual OpStatus ScheduleSingleHop(RunnableType cb) = 0;
  virtual void SingleHopAsync(RunnableType cb) = 0;
  virtual void Conclude() = 0;

  virtual bool RunInShard(EngineShard* shard, bool allow_q_removal) = 0;
  virtual OpArgs GetOpArgs(EngineShard* shard) const = 0;
  virtual ShardArgs GetShardArgs(ShardId sid) const = 0;
  virtual KeyLockArgs GetLockArgs(ShardId sid) const = 0;

  virtual uint16_t DisarmInShard(ShardId sid) = 0;
  virtual std::pair<uint16_t, bool> DisarmInShardWhen(ShardId sid, uint16_t req_flags) = 0;
  virtual bool IsActive(ShardId sid) const = 0;

  virtual std::string DebugId(std::optional<ShardId> sid = std::nullopt) const = 0;
  virtual bool IsMulti() const = 0;
  virtual bool IsAtomicMulti() const = 0;
  virtual bool IsSquashedStub() const = 0;

  virtual void LogJournalOnShard(journal::Entry::Payload&& payload) const = 0;
  virtual void ReviveAutoJournal() = 0;
  virtual std::optional<SlotId> GetUniqueSlotId() const = 0;

  virtual uint32_t DEBUG_GetTxqPosInShard(ShardId sid) const = 0;
  virtual bool DEBUG_IsArmedInShard(ShardId sid) const = 0;
  virtual uint16_t DEBUG_GetLocalMask(ShardId sid) const = 0;

  template <typename F> auto ScheduleSingleHopT(F&& f) -> decltype(f(this, nullptr));

 protected:
  struct alignas(64) PerShardData {
    PerShardData() {
    }
    PerShardData(PerShardData&& other) noexcept {
    }

    uint16_t local_mask = 0;
    std::atomic_bool is_armed = false;

    uint32_t slice_start = 0;
    uint32_t slice_count = 0;
    uint32_t fp_start = 0;
    uint32_t fp_count = 0;

    TxQueue::Iterator pq_pos = TxQueue::kEnd;
    uint32_t wake_key_pos = UINT32_MAX;

    struct Stats {
      unsigned total_runs = 0;
    } stats;

    char pad[64 - 7 * sizeof(uint32_t) - sizeof(Stats)];
  };

  static_assert(sizeof(PerShardData) == 64);

  enum CoordinatorState : uint8_t {
    COORD_SCHED = 1,
    COORD_CONCLUDING = 1 << 1,
    COORD_CANCELLED = 1 << 2,
  };

  TransactionBase() = default;
  explicit TransactionBase(const CommandId* cid);

  void InitTxTime();
  bool CanRunInlined() const;
  void FinishHop();

  uint32_t GetUseCount() const {
    return use_count_.load(std::memory_order_relaxed);
  }

  const CommandId* cid_ = nullptr;
  TxId txid_{0};
  std::atomic_uint32_t use_count_{0};
  Namespace* namespace_{nullptr};
  DbIndex db_index_{0};
  uint64_t time_now_ms_{0};
  bool global_{false};

  uint8_t coordinator_state_ = 0;
  OpStatus local_result_ = OpStatus::OK;
  std::optional<RunnableType> cb_ptr_;
  util::fb2::EmbeddedBlockingCounter run_barrier_{0};

  uint32_t unique_shard_cnt_{0};
  ShardId unique_shard_id_{kInvalidSid};
  CmdArgList full_args_;
  bool re_enabled_auto_journal_ = false;
};

template <typename F>
auto TransactionBase::ScheduleSingleHopT(F&& f) -> decltype(f(this, nullptr)) {
  decltype(f(this, nullptr)) res;

  ScheduleSingleHop([&res, f = std::forward<F>(f)](TransactionBase* t, EngineShard* shard) {
    res = f(t, shard);
    return res.status();
  });
  return res;
}

OpResult<KeyIndex> DetermineKeys(const CommandId* cid, CmdArgList args);

// Shared transaction infrastructure used by both Transaction and UniTransaction.
extern std::atomic_uint64_t op_seq;
void AnalyzeTxQueue(const EngineShard* shard, const TxQueue* txq);
void RecordTxScheduleStats(const TransactionBase* tx);

}  // namespace dfly
