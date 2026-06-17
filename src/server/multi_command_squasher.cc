// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/multi_command_squasher.h"

#include <absl/container/inlined_vector.h>

#include "base/cycle_clock.h"
#include "base/flag_utils.h"
#include "base/flags.h"
#include "base/logging.h"
#include "core/overloaded.h"
#include "facade/dragonfly_connection.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/engine_shard_set.h"
#include "server/transaction.h"
#include "server/tx_base.h"

ABSL_FLAG(uint32_t, max_busy_squash_usec, 1000,
          "Maximum time in microseconds to execute squashed commands before yielding.");

ABSL_FLAG(uint32_t, log_squash_info_threshold_usec, 1 << 31,
          "Threshold in microseconds above which to log squashing timings.");

namespace dfly {

using namespace std;
using namespace facade;
using namespace util;
using base::CycleClock;

namespace {

thread_local uint64_t max_busy_squash_cycles_cached = 1ULL << 32;
thread_local uint32_t log_squash_threshold_cached = 1ULL << 31;

size_t Size(const CapturingReplyBuilder::Payload& payload) {
  size_t payload_size = sizeof(CapturingReplyBuilder::Payload);
  return payload_size +
         visit(Overloaded{[](const payload::SimpleString& data) { return data.size(); },
                          [](const payload::BulkString& data) { return data.size(); },
                          [](const payload::Error& data) {
                            return data->first.size() + data->second.size();
                          },
                          [](const unique_ptr<payload::CollectionPayload>& data) {
                            if (!data || (data->len == 0 && data->type == CollectionType::ARRAY)) {
                              return 0ul;
                            }
                            size_t res = 0;
                            for (const auto& pl : data->arr) {
                              res += Size(pl);
                            }
                            return res;
                          },
                          // Other payload types are small
                          [](const auto&) { return 0ul; }},
               payload);
}

}  // namespace

MultiCommandSquasher::Stats& MultiCommandSquasher::Stats::operator+=(const Stats& o) {
  squashed_commands += o.squashed_commands;
  hop_usec += o.hop_usec;
  reply_usec += o.reply_usec;
  hops += o.hops;
  yields += o.yields;

  return *this;
}

MultiCommandSquasher::MultiCommandSquasher(CmdGenerator cmd_gen, ConnectionContext* cntx,
                                           Service* service, const Opts& opts)
    : cmd_gen_{std::move(cmd_gen)},
      cntx_{cntx},
      service_{service},
      base_cid_{nullptr},
      opts_{opts} {
  auto mode = cntx->transaction->GetMultiMode();
  base_cid_ = cntx->transaction->GetCId();
  atomic_ = mode != Transaction::NON_ATOMIC;
}

MultiCommandSquasher::ShardExecInfo& MultiCommandSquasher::PrepareShardInfo(ShardId sid) {
  if (sharded_.empty()) {
    sharded_.resize(shard_set->size());
    for (size_t i = 0; i < sharded_.size(); i++) {
      sharded_[i].reply_size_total_ptr = &tl_facade_stats->reply_stats.squashing_current_reply_size;
    }
  }

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

MultiCommandSquasher::SquashResult MultiCommandSquasher::TrySquash(CmdRef cmd) {
  DCHECK(cmd.cid);

  const CommandId& cid = *cmd.cid;
  if (!cid.IsTransactional() || (cid.opt_mask() & CO::BLOCKING) ||
      (cid.opt_mask() & CO::GLOBAL_TRANS))
    return SquashResult::NOT_SQUASHED;

  if (cid.name() == "CLIENT" || cntx_->conn_state.tracking_info_.IsTrackingOn()) {
    return SquashResult::NOT_SQUASHED;
  }

  auto args = cmd.Slice(&tmp_keylist_);
  if (args.empty())
    return SquashResult::NOT_SQUASHED;

  // Instead of returning an error, we treat command as non-squashable, allowing the
  // standalone execution path to handle it.
  // Validate returns an optional ErrorReply
  if (cid.Validate(args).has_value())
    return SquashResult::NOT_SQUASHED;

  auto keys = DetermineKeys(&cid, args);
  if (!keys.ok() || keys->NumArgs() == 0)
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

  // Transaction is not active on the requested shard.
  // Command should result in undeclared key error
  if (IsAtomic() && !cntx_->transaction->IsActive(last_sid))
    return SquashResult::NOT_SQUASHED;

  auto& sinfo = PrepareShardInfo(last_sid);

  sinfo.dispatched.push_back({cmd, {}});
  order_.push_back(last_sid);

  bool need_flush = sinfo.dispatched.size() >= opts_.max_squash_size;
  return need_flush ? SquashResult::SQUASHED_FULL : SquashResult::SQUASHED;
}

bool MultiCommandSquasher::ExecuteStandalone(RedisReplyBuilder* rb, CmdRef cmd) {
  DCHECK(order_.empty());  // check no squashed chain is interrupted

  auto args = cmd.Slice(&tmp_keylist_);

  // In pipeline mode the reply is captured and deferred into the parsed command, preserving
  // the reply order with squashed commands whose replies are sent later by the connection.
  optional<CapturingReplyBuilder> crb;
  if (opts_.pipeline_mode) {
    DCHECK(cmd.cmd_cntx);
    DCHECK(cmd.reply_mode == ReplyMode::FULL);
    crb.emplace(ReplyMode::FULL, rb->GetRespVersion());
    rb = &*crb;
  }

  auto resolve = [&] {
    if (crb)
      cmd.cmd_cntx->Resolve(crb->Take());
  };

  if (opts_.verify_commands) {
    if (auto err = service_->VerifyCommandState(*cmd.cid, args, *cntx_); err) {
      rb->SendError(std::move(*err));
      resolve();
      return !opts_.error_abort;
    }
  }

  auto* tx = cntx_->transaction;
  if (cmd.cid->IsTransactional()) {
    tx->MultiSwitchCmd(cmd.cid);
    auto status = tx->InitByArgs(cntx_->ns, cntx_->conn_state.db_index, args);
    if (status != OpStatus::OK) {
      rb->SendError(status);
      resolve();
      return !opts_.error_abort;
    }
  }

  CommandContext cmd_cntx{rb, cntx_};
  cmd_cntx.SetupTx(cmd.cid, tx);
  cmd_cntx.SetTailArgs(cmd.args);
  service_->InvokeCmd(args, &cmd_cntx);
  resolve();

  return true;
}

OpStatus MultiCommandSquasher::SquashedHopCb(EngineShard* es, RespVersion resp_v) {
  auto& sinfo = sharded_[es->shard_id()];
  DCHECK(!sinfo.dispatched.empty());

  CapturingReplyBuilder crb(ReplyMode::FULL, resp_v);
  CmdArgVec arg_vec;
  CommandContext local_cntx{&crb, cntx_};
  local_cntx.SetupTx(nullptr, sinfo.local_tx.get());

  auto move_reply = [&sinfo, &crb](ShardExecInfo::Command* cmd) {
    if (cmd->cmd_cntx)
      return cmd->cmd_cntx->Resolve(crb.Take());

    cmd->reply = crb.Take();
    size_t sz = Size(cmd->reply);
    sinfo.reply_size_delta += sz;
    sinfo.reply_size_total_ptr->fetch_add(sz, std::memory_order_relaxed);
  };

  for (auto& dispatched : sinfo.dispatched) {
    auto* ctx = &local_cntx;
    auto args = dispatched.Slice(&arg_vec);
    if (opts_.verify_commands) {
      // The shared context is used for state verification, the local one is only for replies
      if (auto err = service_->VerifyCommandState(*dispatched.cid, args, *cntx_); err) {
        crb.SendError(std::move(*err));
        move_reply(&dispatched);
        continue;
      }
    }

    crb.SetReplyMode(dispatched.reply_mode);

    // With tiered storage enabled, it makes sense to dispatch async commands concurrently
    // to allow concurrent disk operations. Tiered futures are only blocked on during replies
    bool do_async = es->tiered_storage() && !IsAtomic() && opts_.pipeline_mode &&
                    dispatched.cid->SupportsAsync();
    if (do_async) {
      ctx = dispatched.cmd_cntx;
      ctx->SetDeferredReply();
    }

    ctx->SetupTx(dispatched.cid, local_cntx.tx());
    ctx->tx()->MultiSwitchCmd(dispatched.cid);

    auto status = ctx->tx()->InitByArgs(cntx_->ns, cntx_->conn_state.db_index, args);
    if (status != OpStatus::OK) {
      ctx->SendError(status);  // Calls Resolve() in async, routes to crb in non async
    } else {
      ctx->UpdateCid(dispatched.cid);
      ctx->SetTailArgs(dispatched.args);
      service_->InvokeCmd(args, ctx);
    }

    if (!do_async) {
      move_reply(&dispatched);  // Async commands resolve the context directly
    } else if (!ctx->CanReply()) {
      ctx->Blocker()->Wait();  // Transaction didn't finish inline (likely locked key), wait for it
    }
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
  ServerState::tlocal()->stats.squash_width_freq_arr[num_shards - 1]++;
  uint64_t start = CycleClock::Now();
  atomic_uint64_t max_sched_cycles{0}, max_exec_cycles{0};
  base::SpinLock lock;
  uint64_t fiber_running_cycles{0}, proactor_running_cycles{0};
  uint32_t max_sched_thread_id{0}, max_sched_seq_num{0};

  // Atomic transactions (that have all keys locked) perform hops and run squashed commands via
  // stubs, non-atomic ones just run the commands in parallel.
  if (IsAtomic()) {
    auto cb = [this](ShardId sid) { return !sharded_[sid].dispatched.empty(); };
    tx->PrepareSquashedMultiHop(base_cid_, cb);
    tx->ScheduleSingleHop(
        [this, rb](auto* tx, auto* es) { return SquashedHopCb(es, rb->GetRespVersion()); });
  } else {
    fb2::BlockingCounter bc(num_shards);
    DVLOG(1) << "Squashing " << num_shards << " " << tx->DebugId();

    // Saves work in case logging is disable (i.e. log_squash_threshold_cached is high).
    const uint64_t min_threshold_cycles = CycleClock::FromUsec(log_squash_threshold_cached / 5);
    auto cb = [&, bc, rb]() mutable {
      uint64_t sched_time = CycleClock::Now() - start;

      // Update max_sched_cycles in lock-free fashion, to avoid contention
      uint64_t current = max_sched_cycles.load(memory_order_relaxed);
      while (sched_time > min_threshold_cycles && sched_time > current) {
        if (max_sched_cycles.compare_exchange_weak(current, sched_time, memory_order_relaxed,
                                                   memory_order_relaxed)) {
          lock_guard<base::SpinLock> g(lock);

          // If it is still the longest scheduling time
          if (max_sched_cycles.load(memory_order_relaxed) == sched_time) {
            // Store the stats from the callback with longest scheduling time.
            fiber_running_cycles = ThisFiber::GetRunningTimeCycles();
            proactor_running_cycles = ProactorBase::me()->GetCurrentBusyCycles();
            max_sched_thread_id = ProactorBase::me()->GetPoolIndex();
            max_sched_seq_num = fb2::GetFiberRunSeq();
          }
          break;
        }
        // current is updated to the current value of max_sched_cycles, so the loop will retry
        // with the new value if sched_time is still greater than it.
      }

      if (ThisFiber::GetRunningTimeCycles() > max_busy_squash_cycles_cached) {
        ThisFiber::Yield();
        stats_.yields++;
      }
      this->SquashedHopCb(EngineShard::tlocal(), rb->GetRespVersion());
      uint64_t exec_time = CycleClock::Now() - start;
      current = max_exec_cycles.load(memory_order_relaxed);
      while (exec_time > current) {
        if (max_exec_cycles.compare_exchange_weak(current, exec_time, memory_order_relaxed,
                                                  memory_order_relaxed))
          break;
      }

      bc->Dec();  // Release barrier: Must be the last one in the callback.
    };
    for (unsigned i = 0; i < sharded_.size(); ++i) {
      if (!sharded_[i].dispatched.empty())
        shard_set->AddL2(i, cb);
    }
    bc->Wait();
  }

  uint64_t after_hop = CycleClock::Now();
  bool aborted = false;

  if (!opts_.pipeline_mode) {
    size_t total_reply_size = 0;
    for (auto& sinfo : sharded_) {
      total_reply_size += sinfo.reply_size_delta;
    }

    for (auto idx : order_) {
      auto& sinfo = sharded_[idx];
      DCHECK_LT(sinfo.reply_id, sinfo.dispatched.size());

      auto& reply = sinfo.dispatched[sinfo.reply_id++].reply;
      aborted |= opts_.error_abort && CapturingReplyBuilder::TryExtractError(reply);

      CapturingReplyBuilder::Apply(std::move(reply), rb);
      if (aborted)
        break;
    }

    tl_facade_stats->reply_stats.squashing_current_reply_size.fetch_sub(total_reply_size,
                                                                        std::memory_order_release);
  }

  uint64_t after_reply = CycleClock::Now();
  uint64_t total_usec = CycleClock::ToUsec(after_reply - start);
  stats_.hop_usec += total_usec;
  stats_.reply_usec += CycleClock::ToUsec(after_reply - after_hop);
  stats_.hops++;
  stats_.squashed_commands += order_.size();

  if (total_usec > log_squash_threshold_cached) {
    uint64_t max_sched_usec = CycleClock::ToUsec(max_sched_cycles.load());
    uint64_t fiber_running_usec = CycleClock::ToUsec(fiber_running_cycles);
    uint64_t proactor_running_usec = CycleClock::ToUsec(proactor_running_cycles);
    uint64_t max_exec_usec = CycleClock::ToUsec(max_exec_cycles.load());

    LOG_EVERY_T(INFO, 0.1)
        << "Squashed " << order_.size() << " commands. "
        << "Total/Fanout/MaxSchedTime/ThreadCbTime/ThreadId/FiberCbTime/FiberSeq/"
        << "MaxExecTime: " << total_usec << "/" << num_shards_ << "/" << max_sched_usec << "/"
        << proactor_running_usec << "/" << max_sched_thread_id << "/" << fiber_running_usec << "/"
        << "/" << max_sched_seq_num << "/" << max_exec_usec << "\ncoordinator thread running time: "
        << CycleClock::ToUsec(ProactorBase::me()->GetCurrentBusyCycles());
  }

  for (auto& sinfo : sharded_) {
    sinfo.dispatched.clear();
    sinfo.reply_id = 0;
    // Reset so a subsequent flush of the same instance does not re-count this batch's
    // reply size into total_reply_size and underflow squashing_current_reply_size.
    sinfo.reply_size_delta = 0;
  }

  order_.clear();
  return !aborted;
}

void MultiCommandSquasher::Run(RedisReplyBuilder* rb) {
  DVLOG(1) << "Trying to squash commands for transaction " << cntx_->transaction->DebugId();

  for (CmdRef cmd = cmd_gen_(); cmd.IsValid(); cmd = cmd_gen_()) {
    num_commands_++;
    auto res = TrySquash(cmd);

    if (res == SquashResult::NOT_SQUASHED || res == SquashResult::SQUASHED_FULL) {
      if (!ExecuteSquashed(rb))
        break;

      // if the last command was not added - we squash it separately.
      if (res == SquashResult::NOT_SQUASHED) {
        if (!ExecuteStandalone(rb, cmd))
          break;
      }
    }
  }

  ExecuteSquashed(rb);  // Flush leftover

  // Set last txid.
  cntx_->last_cmd_stats.clock = cntx_->transaction->txid();

  // UnlockMulti is a no-op for non-atomic multi transactions,
  // still called for correctness and future changes
  if (!IsAtomic()) {
    for (auto& sd : sharded_) {
      if (sd.local_tx)
        sd.local_tx->UnlockMulti();
    }
  }

  VLOG(1) << "Handled " << num_commands_ << " commands, max fanout: " << num_shards_
          << ", atomic: " << atomic_;
}

bool MultiCommandSquasher::IsAtomic() const {
  return atomic_;
}

void MultiCommandSquasher::UpdateFromFlags() {
  max_busy_squash_cycles_cached = CycleClock::FromUsec(absl::GetFlag(FLAGS_max_busy_squash_usec));
  log_squash_threshold_cached = absl::GetFlag(FLAGS_log_squash_info_threshold_usec);
}

vector<string> MultiCommandSquasher::GetMutableFlagNames() {
  return base::GetFlagNames(FLAGS_max_busy_squash_usec, FLAGS_log_squash_info_threshold_usec);
}

}  // namespace dfly
