// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/inlined_vector.h>

#include "server/transaction_base.h"

namespace dfly {

// Lightweight single-shard transaction for non-multi, non-blocking commands.
// ~290 bytes vs ~610 for Transaction — uses a single inline PerShardData
// instead of an InlinedVector<PerShardData, 4>.
class UniTransaction final : public TransactionBase {
 public:
  explicit UniTransaction(const CommandId* cid);
  ~UniTransaction() override;

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
    return false;
  }

  bool IsAtomicMulti() const override {
    return false;
  }

  bool IsSquashedStub() const override {
    return false;
  }

  void LogJournalOnShard(journal::Entry::Payload&& payload) const override;
  void ReviveAutoJournal() override;
  std::optional<SlotId> GetUniqueSlotId() const override;

  uint32_t DEBUG_GetTxqPosInShard(ShardId sid) const override {
    return sd_.pq_pos;
  }

  bool DEBUG_IsArmedInShard(ShardId sid) const override {
    return sd_.is_armed.load(std::memory_order_relaxed);
  }

  uint16_t DEBUG_GetLocalMask(ShardId sid) const override {
    return sd_.local_mask;
  }

 private:
  void ScheduleInternal();
  bool ScheduleInShard(EngineShard* shard, bool execute_optimistic);
  void DispatchHop();
  void RunCallback(EngineShard* shard);
  void LogAutoJournalOnShard(EngineShard* shard, RunnableResult result);

  PerShardData sd_;
  absl::InlinedVector<IndexSlice, 4> args_slices_;
  absl::InlinedVector<LockFp, 4> kv_fp_;
  UniqueSlotChecker unique_slot_checker_;
};

}  // namespace dfly
