// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <boost/intrusive_ptr.hpp>

#include "facade/facade_types.h"
#include "facade/reply_builder.h"
#include "server/common.h"
#include "server/common_types.h"

namespace dfly {

class CommandContext;
class CommandId;
class ConnectionContext;
class Service;
class Transaction;

// PipelineSquasher groups consecutive single-shard pipeline commands by shard and executes each
// group in parallel on its shard thread.  Unlike MultiCommandSquasher (used by V1 / MULTI-EXEC),
// it stores replies directly in ParsedCommand::reply_ via Resolve(), needs no StoredCmd copies, and
// relies on the existing linked-list order + ReplyBatch() for reply sequencing.
class PipelineSquasher {
 public:
  PipelineSquasher(ConnectionContext* cntx, Service* service, const CommandId* exec_cid);
  ~PipelineSquasher();

  // Try to add a command to the current squash batch.  Returns false if the
  // command cannot be squashed (multi-shard, blocking, global, etc.).
  bool TrySquash(CommandContext* cmd, const CommandId* cid, facade::ParsedArgs tail_args);

  // Flush and execute all queued shard batches in parallel.
  void ExecuteSquashed(facade::RespVersion resp_v);

  bool HasPending() const;

 private:
  struct ShardInfo {
    struct Entry {
      CommandContext* cmd;
      const CommandId* cid;
      CmdArgVec args_backing;
      CmdArgList tail_args;
    };
    std::vector<Entry> commands;
    boost::intrusive_ptr<Transaction> local_tx;
  };

  ShardInfo& PrepareShardInfo(ShardId sid);
  void SquashedHopCb(ShardId sid, facade::RespVersion resp_v);

  ConnectionContext* cntx_;
  Service* service_;
  const CommandId* exec_cid_;
  std::vector<ShardInfo> sharded_;
  unsigned num_shards_ = 0;
};

}  // namespace dfly
