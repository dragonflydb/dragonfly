// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/pipeline_squasher.h"

#include "base/logging.h"
#include "facade/reply_capture.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/engine_shard_set.h"
#include "server/main_service.h"
#include "server/transaction.h"

namespace dfly {

using namespace std;
using namespace facade;

PipelineSquasher::PipelineSquasher(ConnectionContext* cntx, Service* service,
                                   const CommandId* exec_cid)
    : cntx_(cntx), service_(service), exec_cid_(exec_cid) {
}

PipelineSquasher::~PipelineSquasher() {
  for (auto& sinfo : sharded_) {
    if (sinfo.local_tx)
      sinfo.local_tx->UnlockMulti();
  }
}

PipelineSquasher::ShardInfo& PipelineSquasher::PrepareShardInfo(ShardId sid) {
  if (sharded_.empty())
    sharded_.resize(shard_set->size());

  auto& sinfo = sharded_[sid];
  if (!sinfo.local_tx) {
    sinfo.local_tx = new Transaction{exec_cid_};
    sinfo.local_tx->StartMultiNonAtomic();
    num_shards_++;
  }
  return sinfo;
}

bool PipelineSquasher::TrySquash(CommandContext* cmd, const CommandId* cid,
                                 ParsedArgs tail_args_pa) {
  DCHECK(cid);

  if (!cid->IsTransactional() || (cid->opt_mask() & CO::BLOCKING) ||
      (cid->opt_mask() & CO::GLOBAL_TRANS))
    return false;

  if (cid->name() == "CLIENT" || cntx_->conn_state.tracking_info_.IsTrackingOn())
    return false;

  CmdArgVec tmp_args;
  CmdArgList tail_args = tail_args_pa.ToSlice(&tmp_args);
  if (tail_args.empty())
    return false;

  if (cid->Validate(tail_args).has_value())
    return false;

  auto keys = DetermineKeys(cid, tail_args);
  if (!keys.ok() || keys->NumArgs() == 0)
    return false;

  ShardId last_sid = kInvalidSid;
  for (string_view key : keys->Range(tail_args)) {
    ShardId sid = Shard(key, shard_set->size());
    if (last_sid == kInvalidSid || last_sid == sid)
      last_sid = sid;
    else
      return false;
  }

  auto& sinfo = PrepareShardInfo(last_sid);
  auto& entry = sinfo.commands.emplace_back();
  entry.cmd = cmd;
  entry.cid = cid;
  tail_args_pa.ToVec(&entry.args_backing);
  entry.tail_args = entry.args_backing;
  return true;
}

void PipelineSquasher::SquashedHopCb(ShardId sid, RespVersion resp_v) {
  auto& sinfo = sharded_[sid];
  DCHECK(!sinfo.commands.empty());

  auto* local_tx = sinfo.local_tx.get();
  CapturingReplyBuilder crb(ReplyMode::FULL, resp_v);
  CommandContext local_cmd{&crb, cntx_};
  local_cmd.SetupTx(nullptr, local_tx);

  for (auto& entry : sinfo.commands) {
    local_tx->MultiSwitchCmd(entry.cid);
    auto status = local_tx->InitByArgs(cntx_->ns, cntx_->conn_state.db_index, entry.tail_args);
    if (status != OpStatus::OK) {
      crb.SendError(status);
    } else {
      local_cmd.UpdateCid(entry.cid);
      local_cmd.SetTailArgs(entry.tail_args);
      service_->InvokeCmd(entry.tail_args, &local_cmd);
    }
    entry.cmd->Resolve(crb.Take());
  }
}

void PipelineSquasher::ExecuteSquashed(RespVersion resp_v) {
  unsigned active = 0;
  for (auto& sinfo : sharded_) {
    if (!sinfo.commands.empty())
      active++;
  }
  if (active == 0)
    return;

  // Mark all squashed commands as deferred before dispatching to shards.
  for (auto& sinfo : sharded_) {
    for (auto& entry : sinfo.commands)
      entry.cmd->SetDeferredReply();
  }

  util::fb2::BlockingCounter bc(active);
  for (ShardId i = 0; i < sharded_.size(); i++) {
    if (sharded_[i].commands.empty())
      continue;
    shard_set->AddL2(i, [this, i, bc, resp_v]() mutable {
      SquashedHopCb(i, resp_v);
      bc->Dec();
    });
  }
  bc->Wait();

  for (auto& sinfo : sharded_) {
    sinfo.commands.clear();
  }
}

bool PipelineSquasher::HasPending() const {
  for (auto& sinfo : sharded_) {
    if (!sinfo.commands.empty())
      return true;
  }
  return false;
}

}  // namespace dfly
