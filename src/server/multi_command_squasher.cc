// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/multi_command_squasher.h"

#include <absl/container/inlined_vector.h>

#include "base/logging.h"
#include "core/overloaded.h"
#include "facade/dragonfly_connection.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/engine_shard_set.h"
#include "server/transaction.h"
#include "server/tx_base.h"

namespace dfly {

using namespace std;
using namespace facade;
using namespace util;

namespace {

void CheckConnStateClean(const ConnectionState& state) {
  DCHECK_EQ(state.exec_info.state, ConnectionState::ExecInfo::EXEC_INACTIVE);
  DCHECK(state.exec_info.body.empty());
  DCHECK(!state.script_info);
  DCHECK(!state.subscribe_info);
}

size_t Size(const facade::CapturingReplyBuilder::Payload& payload) {
  size_t payload_size = sizeof(facade::CapturingReplyBuilder::Payload);
  return visit(
      Overloaded{
          [&](monostate) { return payload_size; },
          [&](long) { return payload_size; },
          [&](double) { return payload_size; },
          [&](OpStatus) { return payload_size; },
          [&](CapturingReplyBuilder::Null) { return payload_size; },
          // ignore SSO because it's insignificant
          [&](const CapturingReplyBuilder::SimpleString& data) {
            return payload_size + data.size();
          },
          [&](const CapturingReplyBuilder::BulkString& data) { return payload_size + data.size(); },
          [&](const CapturingReplyBuilder::Error& data) {
            return payload_size + data.first.size() + data.second.size();
          },
          [&](const unique_ptr<CapturingReplyBuilder::CollectionPayload>& data) {
            if (!data || (data->len == 0 && data->type == RedisReplyBuilder::ARRAY)) {
              return payload_size;
            }
            for (const auto& pl : data->arr) {
              payload_size += Size(pl);
            }
            return payload_size;
          },
      },
      payload);
}

}  // namespace

atomic_uint64_t MultiCommandSquasher::current_reply_size_ = 0;

MultiCommandSquasher::MultiCommandSquasher(absl::Span<StoredCmd> cmds, ConnectionContext* cntx,
                                           Service* service, const Opts& opts)
    : cmds_{cmds}, cntx_{cntx}, service_{service}, base_cid_{nullptr}, opts_{opts} {
  auto mode = cntx->transaction->GetMultiMode();
  base_cid_ = cntx->transaction->GetCId();
  atomic_ = mode != Transaction::NON_ATOMIC;
}

MultiCommandSquasher::ShardExecInfo& MultiCommandSquasher::PrepareShardInfo(ShardId sid) {
  if (sharded_.empty())
    sharded_.resize(shard_set->size());

  auto& sinfo = sharded_[sid];
  if (!sinfo.local_tx) {
    if (IsAtomic()) {
      sinfo.local_tx = new Transaction{cntx_->transaction, sid, nullopt};
    } else {
      // Non-atomic squashing does not use the transactional framework for fan out, so local
      // transactions have to be fully standalone, check locks and release them immediately.
      sinfo.local_tx = new Transaction{base_cid_};
      sinfo.local_tx->StartMultiNonAtomic();
    }
    num_shards_++;
  }

  return sinfo;
}

MultiCommandSquasher::SquashResult MultiCommandSquasher::TrySquash(const StoredCmd* cmd) {
  DCHECK(cmd->Cid());

  if (!cmd->Cid()->IsTransactional() || (cmd->Cid()->opt_mask() & CO::BLOCKING) ||
      (cmd->Cid()->opt_mask() & CO::GLOBAL_TRANS))
    return SquashResult::NOT_SQUASHED;

  if (cmd->Cid()->name() == "CLIENT" || cntx_->conn_state.tracking_info_.IsTrackingOn()) {
    return SquashResult::NOT_SQUASHED;
  }

  auto args = cmd->ArgList(&tmp_keylist_);
  if (args.empty())
    return SquashResult::NOT_SQUASHED;

  auto keys = DetermineKeys(cmd->Cid(), args);
  if (!keys.ok())
    return SquashResult::ERROR;
  if (keys->NumArgs() == 0)
    return SquashResult::NOT_SQUASHED;

  // Check if all command keys belong to one shard
  ShardId last_sid = kInvalidSid;

  for (string_view key : keys->Range(args)) {
    ShardId sid = Shard(key, shard_set->size());
    if (last_sid == kInvalidSid || last_sid == sid)
      last_sid = sid;
    else
      return SquashResult::NOT_SQUASHED;  // at least two shards
  }

  auto& sinfo = PrepareShardInfo(last_sid);

  sinfo.dispatched.push_back({.cmd = cmd, .reply = {}});
  order_.push_back(last_sid);

  num_squashed_++;

  bool need_flush = sinfo.dispatched.size() >= opts_.max_squash_size;
  return need_flush ? SquashResult::SQUASHED_FULL : SquashResult::SQUASHED;
}

bool MultiCommandSquasher::ExecuteStandalone(facade::RedisReplyBuilder* rb, const StoredCmd* cmd) {
  DCHECK(order_.empty());  // check no squashed chain is interrupted

  auto args = cmd->ArgList(&tmp_keylist_);

  if (opts_.verify_commands) {
    if (auto err = service_->VerifyCommandState(cmd->Cid(), args, *cntx_); err) {
      rb->SendError(std::move(*err));
      rb->ConsumeLastError();
      return !opts_.error_abort;
    }
  }

  auto* tx = cntx_->transaction;
  cntx_->SwitchTxCmd(cmd->Cid());

  if (cmd->Cid()->IsTransactional())
    tx->InitByArgs(cntx_->ns, cntx_->conn_state.db_index, args);
  service_->InvokeCmd(cmd->Cid(), args, CommandContext{tx, rb, cntx_});

  return true;
}

OpStatus MultiCommandSquasher::SquashedHopCb(EngineShard* es, RespVersion resp_v) {
  auto& sinfo = sharded_[es->shard_id()];
  DCHECK(!sinfo.dispatched.empty());

  auto* local_tx = sinfo.local_tx.get();
  facade::CapturingReplyBuilder crb(ReplyMode::FULL, resp_v);
  ConnectionContext local_cntx{cntx_, local_tx};
  if (cntx_->conn()) {
    local_cntx.skip_acl_validation = cntx_->conn()->IsPrivileged();
  }

  CmdArgVec arg_vec;

  for (auto& dispatched : sinfo.dispatched) {
    auto args = dispatched.cmd->ArgList(&arg_vec);
    if (opts_.verify_commands) {
      // The shared context is used for state verification, the local one is only for replies
      if (auto err = service_->VerifyCommandState(dispatched.cmd->Cid(), args, *cntx_); err) {
        crb.SendError(std::move(*err));
        dispatched.reply = crb.Take();
        current_reply_size_.fetch_add(Size(dispatched.reply), std::memory_order_relaxed);

        continue;
      }
    }

    local_cntx.SwitchTxCmd(dispatched.cmd->Cid());
    crb.SetReplyMode(dispatched.cmd->ReplyMode());

    local_tx->InitByArgs(cntx_->ns, local_cntx.conn_state.db_index, args);
    service_->InvokeCmd(dispatched.cmd->Cid(), args,
                        CommandContext{local_cntx.transaction, &crb, &local_cntx});

    dispatched.reply = crb.Take();
    current_reply_size_.fetch_add(Size(dispatched.reply), std::memory_order_relaxed);

    // Assert commands made no persistent state changes to stub context state
    const auto& local_state = local_cntx.conn_state;
    DCHECK_EQ(local_state.db_index, cntx_->conn_state.db_index);
    CheckConnStateClean(local_state);
  }

  return OpStatus::OK;
}

bool MultiCommandSquasher::ExecuteSquashed(facade::RedisReplyBuilder* rb) {
  DCHECK(!cntx_->conn_state.exec_info.IsCollecting());

  if (order_.empty())
    return true;

  unsigned num_shards = 0;
  for (auto& sd : sharded_) {
    if (!sd.dispatched.empty())
      ++num_shards;
  }

  Transaction* tx = cntx_->transaction;
  ServerState::tlocal()->stats.multi_squash_executions++;
  ProactorBase* proactor = ProactorBase::me();
  uint64_t start = proactor->GetMonotonicTimeNs();

  // Atomic transactions (that have all keys locked) perform hops and run squashed commands via
  // stubs, non-atomic ones just run the commands in parallel.
  if (IsAtomic()) {
    cntx_->cid = base_cid_;
    auto cb = [this](ShardId sid) { return !sharded_[sid].dispatched.empty(); };
    tx->PrepareSquashedMultiHop(base_cid_, cb);
    tx->ScheduleSingleHop(
        [this, rb](auto* tx, auto* es) { return SquashedHopCb(es, rb->GetRespVersion()); });
  } else {
    fb2::BlockingCounter bc(num_shards);
    DVLOG(1) << "Squashing " << num_shards << " " << tx->DebugId();

    auto cb = [this, bc, rb]() mutable {
      this->SquashedHopCb(EngineShard::tlocal(), rb->GetRespVersion());
      bc->Dec();
    };

    for (unsigned i = 0; i < sharded_.size(); ++i) {
      if (!sharded_[i].dispatched.empty())
        shard_set->AddL2(i, cb);
    }
    bc->Wait();
  }

  uint64_t after_hop = proactor->GetMonotonicTimeNs();
  bool aborted = false;

  for (auto idx : order_) {
    auto& sinfo = sharded_[idx];
    DCHECK_LT(sinfo.reply_id, sinfo.dispatched.size());

    auto& reply = sinfo.dispatched[sinfo.reply_id++].reply;
    aborted |= opts_.error_abort && CapturingReplyBuilder::TryExtractError(reply);

    current_reply_size_.fetch_sub(Size(reply), std::memory_order_relaxed);
    CapturingReplyBuilder::Apply(std::move(reply), rb);
    if (aborted)
      break;
  }
  uint64_t after_reply = proactor->GetMonotonicTimeNs();
  ServerState::SafeTLocal()->stats.multi_squash_exec_hop_usec += (after_hop - start) / 1000;
  ServerState::SafeTLocal()->stats.multi_squash_exec_reply_usec += (after_reply - after_hop) / 1000;

  for (auto& sinfo : sharded_) {
    sinfo.dispatched.clear();
    sinfo.reply_id = 0;
  }

  order_.clear();
  return !aborted;
}

size_t MultiCommandSquasher::Run(RedisReplyBuilder* rb) {
  DVLOG(1) << "Trying to squash " << cmds_.size() << " commands for transaction "
           << cntx_->transaction->DebugId();

  for (auto& cmd : cmds_) {
    auto res = TrySquash(&cmd);

    if (res == SquashResult::ERROR)
      break;

    if (res == SquashResult::NOT_SQUASHED || res == SquashResult::SQUASHED_FULL) {
      if (!ExecuteSquashed(rb))
        break;

      // if the last command was not added - we squash it separately.
      if (res == SquashResult::NOT_SQUASHED) {
        if (!ExecuteStandalone(rb, &cmd))
          break;
      }
    }
  }

  ExecuteSquashed(rb);  // Flush leftover

  // Set last txid.
  cntx_->last_command_debug.clock = cntx_->transaction->txid();

  // UnlockMulti is a no-op for non-atomic multi transactions,
  // still called for correctness and future changes
  if (!IsAtomic()) {
    for (auto& sd : sharded_) {
      if (sd.local_tx)
        sd.local_tx->UnlockMulti();
    }
  }

  VLOG(1) << "Squashed " << num_squashed_ << " of " << cmds_.size()
          << " commands, max fanout: " << num_shards_ << ", atomic: " << atomic_;
  return num_squashed_;
}

bool MultiCommandSquasher::IsAtomic() const {
  return atomic_;
}

}  // namespace dfly
