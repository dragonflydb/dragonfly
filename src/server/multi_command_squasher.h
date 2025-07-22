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
  struct Opts {
    bool verify_commands = false;   // Whether commands need to be verified before execution
    bool error_abort = false;       // Abort upon receiving error
    unsigned max_squash_size = 32;  // How many commands to squash at once
  };

  MultiCommandSquasher(ConnectionContext* cntx, Service* Service, const Opts& opts);

  // Run all commands until completion. Returns number of processed commands.
  size_t Run(absl::Span<const StoredCmd> cmds, facade::RedisReplyBuilder* rb);

  static void SetMaxBusySquashUsec(uint32_t usec);

 private:
  // Per-shard execution info.
  struct ShardExecInfo {
    ShardExecInfo() : local_tx{nullptr} {
    }

    struct Command {
      const StoredCmd* cmd;
      facade::CapturingReplyBuilder::Payload reply;
    };
    std::vector<Command> dispatched;  // Dispatched commands
    unsigned reply_id = 0;

    std::atomic<size_t>* reply_size_total_ptr;   // Total size of replies on the IO thread
    size_t reply_size_delta = 0;                 // Size of replies for this shard
    boost::intrusive_ptr<Transaction> local_tx;  // stub-mode tx for use inside shard
  };

  enum class SquashResult { SQUASHED, SQUASHED_FULL, NOT_SQUASHED, ERROR };

  // Lazy initialize shard info.
  ShardExecInfo& PrepareShardInfo(ShardId sid);

  // Retrun squash flags
  SquashResult TrySquash(const StoredCmd* cmd);

  // Execute separate non-squashed cmd. Return false if aborting on error.
  bool ExecuteStandalone(facade::RedisReplyBuilder* rb, const StoredCmd* cmd);

  // Callback that runs on shards during squashed hop.
  facade::OpStatus SquashedHopCb(EngineShard* es, facade::RespVersion resp_v);

  // Execute all currently squashed commands. Return false if aborting on error.
  bool ExecuteSquashed(facade::RedisReplyBuilder* rb);

  bool IsAtomic() const;

  ConnectionContext* cntx_;  // Underlying context
  Service* service_;

  bool atomic_;                // Whether working in any of the atomic modes
  const CommandId* base_cid_;  // underlying cid (exec or eval) for executing batch hops

  Opts opts_;

  std::vector<ShardExecInfo> sharded_;
  std::vector<ShardId> order_;  // reply order for squashed cmds

  size_t num_squashed_ = 0;
  size_t num_shards_ = 0;

  std::vector<MutableSlice> tmp_keylist_;
};

}  // namespace dfly
