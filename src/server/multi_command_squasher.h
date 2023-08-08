// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "base/logging.h"
#include "core/fibers.h"
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
  static void Execute(absl::Span<StoredCmd> cmds, ConnectionContext* cntx, Service* service,
                      bool verify_commands = false, bool error_abort = false) {
    MultiCommandSquasher{cmds, cntx, service, verify_commands, error_abort}.Run();
  }

 private:
  // Per-shard exection info.
  struct ShardExecInfo {
    ShardExecInfo() : had_writes{false}, cmds{}, replies{}, local_tx{nullptr} {
    }

    bool had_writes;
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
  bool ExecuteStandalone(StoredCmd* cmd);

  // Callback that runs on shards during squashed hop.
  facade::OpStatus SquashedHopCb(Transaction* parent_tx, EngineShard* es);

  // Execute all currently squashed commands. Return false if aborting on error.
  bool ExecuteSquashed();

  // Run all commands until completion.
  void Run();

  bool IsAtomic() const;

 private:
  absl::Span<StoredCmd> cmds_;  // Input range of stored commands
  ConnectionContext* cntx_;     // Underlying context
  Service* service_;

  // underlying cid (exec or eval) for executing batch hops, nullptr for non-atomic
  const CommandId* base_cid_;

  bool verify_commands_ = false;  // Whether commands need to be verified before execution
  bool error_abort_ = false;      // Abort upon receiving error

  std::vector<ShardExecInfo> sharded_;
  std::vector<ShardId> order_;  // reply order for squashed cmds

  std::vector<MutableSlice> tmp_keylist_;
};

}  // namespace dfly
