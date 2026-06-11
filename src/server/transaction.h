// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/base/internal/spinlock.h>
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <absl/container/inlined_vector.h>

#include <vector>

#include "server/transaction_base.h"

namespace dfly {

class BlockingController;

// Full-featured transaction: multi-shard, multi/exec, blocking, global.
// Inherits the common interface and state from TransactionBase.
//
// See TransactionBase for the scheduling/callback interface used by most command code.
// This class adds multi-transaction support, blocking commands, squashing, and other
// features not needed by simple single-shard commands.
class Transaction : public TransactionBase {
  friend class BlockingController;

  Transaction(const Transaction&);
  void operator=(const Transaction&) = delete;

 public:
  struct Guard {
    explicit Guard(Transaction* tx);
    ~Guard();

   private:
    Transaction* tx;
  };

  explicit Transaction(const CommandId* cid);

  // Initialize transaction for squashing placed on a specific shard with a given parent tx
  explicit Transaction(const Transaction* parent, ShardId shard_id, std::optional<SlotId> slot_id);

  ~Transaction() override;

  // --- TransactionBase virtual overrides ---

  OpStatus InitByArgs(Namespace* ns, DbIndex index, CmdArgList args) override;
  ShardArgs GetShardArgs(ShardId sid) const override;
  void Execute(RunnableType cb, bool conclude) override;
  OpStatus ScheduleSingleHop(RunnableType cb) override;
  void SingleHopAsync(RunnableType cb) override;
  void Conclude() override;
  bool RunInShard(EngineShard* shard, bool allow_q_removal) override;
  OpArgs GetOpArgs(EngineShard* shard) const override;
  KeyLockArgs GetLockArgs(ShardId sid) const override;
  uint16_t DisarmInShard(ShardId sid) override;
  std::pair<uint16_t, bool> DisarmInShardWhen(ShardId sid, uint16_t req_flags) override;
  bool IsActive(ShardId sid) const override;
  std::string DebugId(std::optional<ShardId> sid = std::nullopt) const override;

  bool IsMulti() const override {
    return bool(multi_);
  }

  bool IsAtomicMulti() const override {
    return multi_ && (multi_->mode == LOCK_AHEAD || multi_->mode == GLOBAL);
  }

  bool IsSquashedStub() const override {
    return multi_ && multi_->role == SQUASHED_STUB;
  }

  void LogJournalOnShard(journal::Entry::Payload&& payload) const override;
  void ReviveAutoJournal() override;
  std::optional<SlotId> GetUniqueSlotId() const override;

  uint32_t DEBUG_GetTxqPosInShard(ShardId sid) const override {
    return shard_data_[SidToId(sid)].pq_pos;
  }

  bool DEBUG_IsArmedInShard(ShardId sid) const override {
    return shard_data_[SidToId(sid)].is_armed.load(std::memory_order_relaxed);
  }

  uint16_t DEBUG_GetLocalMask(ShardId sid) const override {
    return shard_data_[SidToId(sid)].local_mask;
  }

  // --- Transaction-specific methods (not on TransactionBase) ---

  // Registers transaction into watched queue and blocks until notification or timeout.
  facade::OpStatus WaitOnWatch(const time_point& tp, WaitKeys keys, KeyReadyChecker krc,
                               bool* block_flag, bool* pause_flag);

  bool NotifySuspended(ShardId sid, std::string_view key);

  void CancelBlocking(const std::function<OpStatus(ArgSlice)>&);

  bool CancelScheduledTx();

  void PrepareSquashedMultiHop(const CommandId* cid, absl::FunctionRef<bool(ShardId)> enabled);
  void PrepareSingleSquash(Namespace* ns, ShardId sid, DbIndex db, CmdArgList keys, MultiMode mode);

  void StartMultiGlobal(Namespace* ns, DbIndex dbid);
  void StartMultiLockedAhead(Namespace* ns, DbIndex dbid, CmdArgList keys,
                             bool skip_scheduling = false);
  void StartMultiNonAtomic();
  void UnlockMulti(bool block = false);

  void MultiSwitchCmd(const CommandId* cid);
  void MultiUpdateWithParent(const Transaction* parent);
  void MultiBecomeSquasher();

  // If blocking tx was woken up on this shard, get wake key.
  std::optional<std::string_view> GetWakeKey(ShardId sid) const;

  MultiMode GetMultiMode() const {
    return multi_->mode;
  }

  void Refurbish();

  const absl::flat_hash_set<std::pair<ShardId, LockFp>>& GetMultiFps() const;

  void SetTrackingCallback(std::function<void(Transaction* trans)> f) {
    tracking_cb_ = std::move(f);
  }

  void MaybeInvokeTrackingCb() {
    if (tracking_cb_) {
      tracking_cb_(this);
    }
  }

  std::string DEBUGV18_BlockInfo() {
    return "claimed=" + std::to_string(blocking_barrier_.IsClaimed()) +
           " coord_state=" + std::to_string(int(coordinator_state_)) +
           " local_res=" + std::to_string(int(local_result_));
  }

 private:
  struct MultiData {
    MultiRole role = MultiRole::DEFAULT;
    MultiMode mode = MultiMode::NOT_DETERMINED;
    std::optional<IntentLock::Mode> lock_mode;

    absl::flat_hash_set<std::pair<ShardId, LockFp>> tag_fps;
    bool concluding = false;
    unsigned cmd_seq_num = 0;
  };

  struct PerShardCache {
    std::vector<IndexSlice> slices;
    unsigned key_step = 1;

    void Clear() {
      slices.clear();
    }
  };

  class BatonBarrier {
   public:
    bool IsClaimed() const;
    bool TryClaim();
    void Close();
    std::cv_status Wait(time_point);

   private:
    std::atomic_bool claimed_{false};
    std::atomic_bool closed_{false};
    util::fb2::EventCount ec_{};
  };

  void InitBase(Namespace* ns, DbIndex dbid, CmdArgList args);
  void InitGlobal();
  void InitByKeys(const KeyIndex& keys);
  void EnableShard(ShardId sid);
  void EnableAllShards();

  void BuildShardIndex(const KeyIndex& keys, std::vector<PerShardCache>* out);
  void InitShardData(absl::Span<const PerShardCache> shard_index, size_t num_args);
  void StoreKeysInArgs(const KeyIndex& key_index);
  void PrepareMultiFps(CmdArgList keys);

  void ScheduleInternal();
  bool ScheduleInShard(EngineShard* shard, bool execute_optimistic);
  void DispatchHop();

  void RunCallback(EngineShard* shard);

  void WatchInShard(Namespace* ns, ShardArgs keys, EngineShard* shard, KeyReadyChecker krc);
  void ExpireBlocking(WaitKeys keys);
  void ExpireShardCb(ShardArgs keys, EngineShard* shard);
  bool CancelShardCb(EngineShard* shard);

  OpStatus RunSquashedMultiCb(RunnableType cb);

  void UnlockMultiShardCb(absl::Span<const LockFp> fps, EngineShard* shard);
  void LogAutoJournalOnShard(EngineShard* shard, RunnableResult shard_result);

  bool IsActiveMulti() const {
    return multi_ && multi_->role != SQUASHED_STUB;
  }

  unsigned SidToId(ShardId sid) const {
    return sid < shard_data_.size() ? sid : 0;
  }

  template <typename F> void IterateShards(F&& f) {
    if (unique_shard_cnt_ == 1) {
      f(shard_data_[SidToId(unique_shard_id_)], unique_shard_id_);
    } else {
      for (ShardId i = 0; i < shard_data_.size(); ++i) {
        f(shard_data_[i], i);
      }
    }
  }

  template <typename F> void IterateActiveShards(F&& f) {
    IterateShards([&f](auto& sd, auto i) {
      if (sd.local_mask & ACTIVE)
        f(sd, i);
    });
  }

  // --- Transaction-specific state ---

  absl::InlinedVector<PerShardData, 4> shard_data_;
  absl::InlinedVector<IndexSlice, 4> args_slices_;
  absl::InlinedVector<LockFp, 4> kv_fp_;
  UniqueSlotChecker unique_slot_checker_;

  std::unique_ptr<MultiData> multi_;

  BatonBarrier blocking_barrier_{};
  OpStatus block_cancel_result_ = OpStatus::OK;
  absl::base_internal::SpinLock local_result_mu_;

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

}  // namespace dfly
