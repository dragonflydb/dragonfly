// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "facade/reply_capture.h"
#include "server/conn_context.h"
#include "server/main_service.h"

namespace dfly {

// MultiCommandSquasher allows executing a series of commands under a multi transaction
// and squashing multiple consecutive single-shard commands into one hop whenever it's possible,
// thus parallelizing command execution and greatly decreasing the dispatch overhead for them.
//
// Single shard commands are executed in small batches over multiple shards.
// For atomic multi transactions (global & locking ahead), the batch is executed with a regular hop
// of the multi transaction. Each shard contains a "stub" transaction to mimic the regular
// transactional api for commands. Non atomic multi transactions use regular shard_set dispatches
// instead of hops for executing batches. This allows avoiding locking many keys at once. Each shard
// contains a non-atomic multi transaction to execute squashed commands.
class MultiCommandSquasher {
 public:
  static size_t Execute(absl::Span<StoredCmd> cmds, facade::RedisReplyBuilder* rb,
                        ConnectionContext* cntx, Service* service, bool verify_commands = false,
                        bool error_abort = false) {
    return MultiCommandSquasher{cmds, cntx, service, verify_commands, error_abort}.Run(rb);
  }

  static size_t GetRepliesMemSize() {
    return current_reply_size_.load(std::memory_order_relaxed);
  }

 private:
  // Per-shard execution info.
  struct ShardExecInfo {
    ShardExecInfo() : cmds{}, replies{}, local_tx{nullptr} {
    }

    std::vector<StoredCmd*> cmds;  // accumulated commands
    std::vector<facade::CapturingReplyBuilder::Payload> replies;
    boost::intrusive_ptr<Transaction> local_tx;  // stub-mode tx for use inside shard
  };

  enum class SquashResult { SQUASHED, SQUASHED_FULL, NOT_SQUASHED, ERROR };

  static constexpr int kMaxSquashing = 32;

 private:
  MultiCommandSquasher(absl::Span<StoredCmd> cmds, ConnectionContext* cntx, Service* Service,
                       bool verify_commands, bool error_abort);

  // Lazy initialize shard info.
  ShardExecInfo& PrepareShardInfo(ShardId sid);

  // Retrun squash flags
  SquashResult TrySquash(StoredCmd* cmd);

  // Execute separate non-squashed cmd. Return false if aborting on error.
  bool ExecuteStandalone(facade::RedisReplyBuilder* rb, StoredCmd* cmd);

  // Callback that runs on shards during squashed hop.
  facade::OpStatus SquashedHopCb(Transaction* parent_tx, EngineShard* es,
                                 facade::RespVersion resp_v);

  // Execute all currently squashed commands. Return false if aborting on error.
  bool ExecuteSquashed(facade::RedisReplyBuilder* rb);

  // Run all commands until completion. Returns number of squashed commands.
  size_t Run(facade::RedisReplyBuilder* rb);

  bool IsAtomic() const;

 private:
  absl::Span<StoredCmd> cmds_;  // Input range of stored commands
  ConnectionContext* cntx_;     // Underlying context
  Service* service_;

  bool atomic_;                // Whether working in any of the atomic modes
  const CommandId* base_cid_;  // underlying cid (exec or eval) for executing batch hops

  bool verify_commands_ = false;  // Whether commands need to be verified before execution
  bool error_abort_ = false;      // Abort upon receiving error

  std::vector<ShardExecInfo> sharded_;
  std::vector<ShardId> order_;  // reply order for squashed cmds

  size_t num_squashed_ = 0;
  size_t num_shards_ = 0;

  std::vector<MutableSlice> tmp_keylist_;

  // we increase size in one thread and decrease in another
  static atomic_uint64_t current_reply_size_;
};

}  // namespace dfly
