// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "server/dflycmd.h"

#include <absl/random/random.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_split.h>
#include <absl/strings/strip.h>

#include <limits>
#include <memory>
#include <optional>
#include <utility>

#include "absl/cleanup/cleanup.h"
#include "absl/strings/numbers.h"
#include "base/flags.h"
#include "base/logging.h"
#include "core/detail/gen_utils.h"
#include "facade/cmd_arg_parser.h"
#include "facade/dragonfly_connection.h"
#include "facade/dragonfly_listener.h"
#include "facade/reply_builder.h"
#include "server/cluster_support.h"
#include "server/engine_shard_set.h"
#include "server/error.h"
#include "server/journal/journal.h"
#include "server/journal/streamer.h"
#include "server/main_service.h"
#include "server/namespaces.h"
#include "server/rdb_save.h"
#include "server/replica.h"
#include "server/server_family.h"
#include "server/server_state.h"
#include "server/transaction.h"
#include "util/fibers/synchronization.h"
using namespace std;

ABSL_DECLARE_FLAG(bool, info_replication_valkey_compatible);
ABSL_DECLARE_FLAG(uint32_t, replication_timeout);
ABSL_DECLARE_FLAG(uint32_t, shard_repl_backlog_len);

namespace dfly {

using namespace facade;
using namespace util;

using std::string;
using util::ProactorBase;

std::string_view SyncStateName(DflyCmd::SyncState sync_state) {
  switch (sync_state) {
    case DflyCmd::SyncState::PREPARATION:
      return "preparation";
    case DflyCmd::SyncState::FULL_SYNC:
      return "full_sync";
    case DflyCmd::SyncState::STABLE_SYNC:
      return absl::GetFlag(FLAGS_info_replication_valkey_compatible) ? "online" : "stable_sync";
    case DflyCmd::SyncState::CANCELLED:
      return "cancelled";
  }
  DCHECK(false) << "Unspported state " << int(sync_state);
  return "unsupported";
}

namespace {
const char kBadMasterId[] = "bad master id";
const char kIdNotFound[] = "syncid not found";
const char kInvalidSyncId[] = "bad sync id";
const char kInvalidState[] = "invalid state";

// Per-proactor cached view of replica_infos_.
// Every mutation of centralized `replica_infos_` warrants subsequent call to
// UpdateReplicaInfoCacheLocked.
thread_local std::shared_ptr<const DflyCmd::ReplicaInfoMap> tl_replica_infos;

bool ToSyncId(string_view str, uint32_t* num) {
  if (!absl::StartsWith(str, "SYNC"))
    return false;
  str.remove_prefix(4);

  return absl::SimpleAtoi(str, num);
}

bool WaitReplicaFlowToCatchup(absl::Time end_time, const DflyCmd::ReplicaInfo* replica,
                              EngineShard* shard, bool with_ping) {
  // We don't want any writes to the journal after we send the `PING`,
  // and expirations could ruin that.
  namespaces->GetDefaultNamespace().GetDbSlice(shard->shard_id()).SetExpireAllowed(false);

  if (with_ping) {
    // PING forces replica to send the most recent last_acked_lsn.
    // ACKS from the replica are send only every X commands or every 3 seconds (flag configurable)
    // or when forced (by the PING above).
    journal::RecordEntry(0, journal::Op::PING, 0, nullopt, {});
  }

  const FlowInfo* flow = &replica->GetFlow(shard->shard_id());

  while (flow->last_acked_lsn < journal::GetLsn()) {
    if (absl::Now() > end_time) {
      LOG(WARNING) << "Couldn't synchronize with replica for takeover in time: "
                   << replica->GetAddress() << ":" << replica->GetListeningPort()
                   << ", last acked: " << flow->last_acked_lsn << ", expecting "
                   << journal::GetLsn();
      return false;
    }
    if (!replica->GetExecState().IsRunning()) {
      return false;
    }
    LOG_EVERY_T(INFO, 1) << "Replica lsn:" << flow->last_acked_lsn
                         << " master lsn:" << journal::GetLsn()
                         << "; Journal streamer state: " << flow->streamer->FormatInternalState();
    ThisFiber::SleepFor(1ms);
  }

  return true;
}

}  // namespace

void DflyCmd::ReplicaInfo::Cancel() {
  util::fb2::LockGuard lk{shared_mu_};
  if (replica_state_.load(std::memory_order_relaxed) == SyncState::CANCELLED) {
    return;
  }

  LOG(INFO) << "Disconnecting from replica " << address_ << ":" << listening_port_;

  // Update state and cancel context.
  replica_state_.store(SyncState::CANCELLED, std::memory_order_relaxed);
  exec_st_.ReportCancelError();
  // Wait for tasks to finish.
  shard_set->RunBlockingInParallel([this](EngineShard* shard) {
    VLOG(2) << "Disconnecting flow " << shard->shard_id();

    FlowInfo* flow = &flows_[shard->shard_id()];
    if (flow->cleanup) {
      flow->cleanup();
    }
    VLOG(2) << "After flow cleanup " << shard->shard_id();
    flow->conn = nullptr;
  });
  // Wait for error handler to quit.
  exec_st_.JoinErrorHandler();
  VLOG(1) << "Disconnecting replica " << address_ << ":" << listening_port_;
}

DflyCmd::DflyCmd(ServerFamily* server_family) : sf_(server_family) {
}

void DflyCmd::Run(CmdArgParser parser, CommandContext* cmd_cntx) {
  // Remaining-arg counts below are relative to the position after the subcommand token.
  string sub_cmd = absl::AsciiStrToUpper(parser.Next<string_view>());

  if (sub_cmd == "THREAD") {
    return Thread(parser, cmd_cntx);
  }

  if (sub_cmd == "FLOW" && parser.HasAtLeast(3) && !parser.HasAtLeast(6)) {
    return Flow(parser, cmd_cntx);
  }

  if (sub_cmd == "SYNC" && parser.HasAtLeast(1) && !parser.HasAtLeast(2)) {
    return Sync(parser, cmd_cntx);
  }

  if (sub_cmd == "STARTSTABLE" && parser.HasAtLeast(1) && !parser.HasAtLeast(2)) {
    return StartStable(parser, cmd_cntx);
  }

  if (sub_cmd == "TAKEOVER" && parser.HasAtLeast(2) && !parser.HasAtLeast(4)) {
    return TakeOver(parser, cmd_cntx);
  }

  if (sub_cmd == "EXPIRE") {
    return Expire(parser, cmd_cntx);
  }

  if (sub_cmd == "REPLICAOFFSET" && !parser.HasNext()) {
    return ReplicaOffset(parser, cmd_cntx);
  }

  if (sub_cmd == "LOAD") {
    return Load(parser, cmd_cntx);
  }

  auto* rb = static_cast<facade::RedisReplyBuilder*>(cmd_cntx->rb());
  if (sub_cmd == "HELP") {
    string_view help_arr[] = {
        "DFLY <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
        "THREAD",
        "    Returns connection thread index and number of threads",
        "THREAD <thread-id>",
        "    Migrates connection to thread <thread-id>",
        "EXPIRE",
        "    Collects all expired items.",
        "REPLICAOFFSET",
        "    Returns LSN (log sequence number) per shard. These are the sequential ids of the ",
        "    journal entry.",
        "LOAD <filename> [APPEND]",
        "    Loads <filename> RDB/DFS file into the data store.",
        "    * APPEND: Existing keys are NOT removed before loading the file, conflicting ",
        "      keys (that exist in both data store and in file) are overridden.",
        "HELP",
        "    Prints this help.",
    };
    return rb->SendSimpleStrArr(help_arr);
  }

  cmd_cntx->SendError(kSyntaxErr);
}

void DflyCmd::Thread(CmdArgParser parser, CommandContext* cmd_cntx) {
  util::ProactorPool* pool = shard_set->pool();

  auto* rb = static_cast<facade::RedisReplyBuilder*>(cmd_cntx->rb());
  if (!parser.HasNext()) {  // DFLY THREAD : returns connection thread index and number of threads.
    rb->StartArray(2);
    rb->SendLong(ProactorBase::me()->GetPoolIndex());
    rb->SendLong(long(pool->size()));
    return;
  }

  // DFLY THREAD to_thread : migrates current connection to a different thread.
  unsigned num_thread = parser.Next<unsigned>();
  if (parser.TakeError()) {
    return cmd_cntx->SendError(kSyntaxErr);
  }

  if (num_thread < pool->size()) {
    if (int(num_thread) != ProactorBase::me()->GetPoolIndex()) {
      auto* conn = cmd_cntx->conn();
      if (!conn->Migrate(pool->at(num_thread))) {
        // Listener::PreShutdown() triggered
        if (conn->socket()->IsOpen()) {
          return cmd_cntx->SendError(kInvalidState);
        }
        return;
      }
    }

    return rb->SendOk();
  }

  return cmd_cntx->SendError(kInvalidIntErr);
}

void DflyCmd::Flow(CmdArgParser parser, CommandContext* cmd_cntx) {
  string_view master_id = parser.Next<string_view>();
  string_view sync_id_str = parser.Next<string_view>();
  string_view flow_id_str = parser.Next<string_view>();

  std::optional<LSN> seqid;
  std::optional<string> last_master_id;
  std::optional<string> last_master_lsn;
  if (parser.HasAtLeast(2)) {
    last_master_id = parser.Next<string>();
    last_master_lsn = parser.Next<string>();
  } else if (parser.HasNext()) {
    seqid = parser.Next<LSN>();
    if (parser.TakeError()) {
      return cmd_cntx->SendError(facade::kInvalidIntErr);
    }
  }

  VLOG(1) << "Got DFLY FLOW master_id: " << master_id << " sync_id: " << sync_id_str
          << " flow: " << flow_id_str << " seq: " << seqid.value_or(-1);

  if (master_id != sf_->master_replid()) {
    return cmd_cntx->SendError(kBadMasterId);
  }

  unsigned flow_id;
  if (!absl::SimpleAtoi(flow_id_str, &flow_id) || flow_id >= shard_set->size()) {
    return cmd_cntx->SendError(facade::kInvalidIntErr);
  }

  auto [sync_id, replica_ptr] = GetReplicaInfoOrReply(sync_id_str, cmd_cntx);
  if (!sync_id)
    return;

  string eof_token;
  std::string sync_type{"FULL"};
  {
    util::fb2::LockGuard lk{replica_ptr->GetMutex()};

    if (replica_ptr->GetReplicaState() != SyncState::PREPARATION) {
      return cmd_cntx->SendError(kInvalidState);
    }

    // Set meta info on connection.
    auto* conn_cntx = cmd_cntx->server_conn_cntx();
    cmd_cntx->conn()->SetName(absl::StrCat("repl_flow_", sync_id));
    conn_cntx->conn_state.replication_info.repl_session_id = sync_id;
    conn_cntx->conn_state.replication_info.repl_flow_id = flow_id;
    conn_cntx->replica_conn = true;

    absl::InsecureBitGen gen;
    eof_token = GetRandomHex(gen, 40);

    auto& flow = replica_ptr->GetFlow(flow_id);
    conn_cntx->master_repl_flow = &flow;
    conn_cntx->conn()->SetConnectionMemoryAccounting(false);
    flow.conn = cmd_cntx->conn();
    flow.eof_token = eof_token;

    if (!conn_cntx->conn()->Migrate(shard_set->pool()->at(flow_id))) {
      // Listener::PreShutdown() triggered
      if (conn_cntx->conn()->socket()->IsOpen()) {
        return cmd_cntx->SendError(kInvalidState);
      }
      return;
    }

    journal::StartInThread();

    std::optional<Replica::LastMasterSyncData> data = sf_->GetLastMasterData();
    std::optional<LSN> lsn_to_start_partial;
    // In this flow the master and the registered replica where synced from the same master.
    if (last_master_id && data && data->id == *last_master_id) {
      ++ServerState::tlocal()->stats.psync_requests_total;
      auto flow_lsn =
          ParseLsnVec(*last_master_lsn, data->last_journal_LSNs.size(), flow_id, cmd_cntx);
      if (!flow_lsn) {
        return;  // ParseLsnVec replies in case of error
      }

      if (IsLSNInPartialSyncBuffer(*flow_lsn)) {
        lsn_to_start_partial.emplace(*flow_lsn);
      }

    } else if (seqid.has_value() && IsLSNInPartialSyncBuffer(*seqid)) {
      lsn_to_start_partial.emplace(*seqid);
    }

    if (lsn_to_start_partial) {
      flow.start_partial_sync_at = *lsn_to_start_partial;
      sync_type = "PARTIAL";
      VLOG(1) << "Partial sync requested from LSN=" << flow.start_partial_sync_at.value()
              << " and is available. (current_lsn=" << journal::GetLsn() << ")";
    }
  }

  auto* rb = static_cast<facade::RedisReplyBuilder*>(cmd_cntx->rb());
  rb->StartArray(2);
  rb->SendSimpleString(sync_type);
  rb->SendSimpleString(eof_token);
}

void DflyCmd::Sync(CmdArgParser parser, CommandContext* cmd_cntx) {
  string_view sync_id_str = parser.Next<string_view>();

  VLOG(1) << "Got DFLY SYNC " << sync_id_str;

  auto [sync_id, replica_ptr] = GetReplicaInfoOrReply(sync_id_str, cmd_cntx);
  if (!sync_id)
    return;

  util::fb2::LockGuard lk{replica_ptr->GetMutex()};
  if (!CheckReplicaStateOrReply(*replica_ptr, SyncState::PREPARATION, cmd_cntx))
    return;

  // Start full sync.
  {
    Transaction::Guard tg{cmd_cntx->tx()};
    AggregateStatus status;

    // Use explicit assignment for replica_ptr, because capturing structured bindings is C++20.
    auto cb = [this, &status, replica_ptr = replica_ptr](EngineShard* shard) {
      FlowInfo* flow = &replica_ptr->GetFlow(shard->shard_id());
      // By the time DFLY SYNC arrives, all DFLY FLOW responses have already been sent (the replica
      // sends SYNC only after receiving all FLOW replies), so start_partial_sync_at is settled.
      // If partial sync was arranged, reject DFLY SYNC: the replica sent SYNC after receiving
      // PARTIAL, which means it does not properly implement the partial sync protocol and would
      // deadlock or corrupt state if we proceeded.
      if (flow->start_partial_sync_at.has_value()) {
        status = OpStatus::INVALID_VALUE;
        return;
      }
      status = StartFullSyncInThread(replica_ptr->GetVersion(), flow, &replica_ptr->GetExecState(),
                                     shard);
    };
    shard_set->RunBlockingInParallel(std::move(cb));

    // TODO: Send better error
    if (*status != OpStatus::OK)
      return cmd_cntx->SendError(kInvalidState);
  }

  LOG(INFO) << "Started sync with replica " << replica_ptr->GetAddress() << ":"
            << replica_ptr->GetListeningPort();

  // protected by lk above.
  replica_ptr->SetReplicaState(SyncState::FULL_SYNC);

  return cmd_cntx->SendOk();
}

void DflyCmd::StartStable(CmdArgParser parser, CommandContext* cmd_cntx) {
  string_view sync_id_str = parser.Next<string_view>();

  VLOG(1) << "Got DFLY STARTSTABLE " << sync_id_str;

  auto [sync_id, replica_ptr] = GetReplicaInfoOrReply(sync_id_str, cmd_cntx);
  if (!sync_id)
    return;

  util::fb2::LockGuard lk{replica_ptr->GetMutex()};
  auto repl_state = replica_ptr->GetReplicaState();
  if (repl_state != SyncState::FULL_SYNC && repl_state != SyncState::PREPARATION) {
    cmd_cntx->SendError(kInvalidState);
    return;
  }

  // This might happen if a flow abruptly disconnected before sending the SYNC request.
  if (!replica_ptr->AllFlowsConnected()) {
    cmd_cntx->SendError(kInvalidState);
    return;
  }

  {
    Transaction::Guard tg{cmd_cntx->tx()};
    AggregateStatus status;

    auto cb = [this, &status, replica_ptr = replica_ptr](EngineShard* shard) {
      FlowInfo* flow = &replica_ptr->GetFlow(shard->shard_id());

      // We are doing partial sync. We never started FullSync so we don't need to stop it.
      bool is_partial = flow->start_partial_sync_at.has_value();
      if (!is_partial) {
        status = StopFullSyncInThread(flow, &replica_ptr->GetExecState(), shard);
        if (*status != OpStatus::OK) {
          return;
        }
      }

      StartStableSyncInThread(flow, &replica_ptr->GetExecState(), shard);
    };
    shard_set->RunBlockingInParallel(std::move(cb));

    if (*status != OpStatus::OK)
      return cmd_cntx->SendError(kInvalidState);
  }

  LOG(INFO) << "Transitioned into stable sync with replica " << replica_ptr->GetAddress() << ":"
            << replica_ptr->GetListeningPort();

  replica_ptr->SetReplicaState(SyncState::STABLE_SYNC);
  return cmd_cntx->SendOk();
}

bool DflyCmd::IsLSNInPartialSyncBuffer(LSN lsn) const {
  const bool exists = journal::GetLsn() == lsn || journal::IsLSNInBuffer(lsn);
  if (!exists) {
    LOG(INFO) << "Partial sync requested from stale LSN=" << lsn
              << " that the replication buffer doesn't contain this anymore (current_lsn="
              << journal::GetLsn() << "). Will perform a full sync of the data.";
    LOG(INFO) << "If this happens often you can control the replication buffer's size with the "
                 "--shard_repl_backlog_len option";
  }
  return exists;
}

std::optional<LSN> DflyCmd::ParseLsnVec(std::string_view last_master_lsn,
                                        size_t last_journal_lsn_size, size_t flow_id,
                                        CommandContext* cmd_cntx) {
  std::vector<std::string_view> lsn_str_vec = absl::StrSplit(last_master_lsn, '-');
  if (lsn_str_vec.size() != last_journal_lsn_size) {
    cmd_cntx->SendError(facade::kSyntaxErr);  // Unexpected flow. LSN vector of same master
                                              // should be the same size on all replicas.
    return std::nullopt;
  }

  std::vector<LSN> lsn_vec;
  lsn_vec.reserve(lsn_str_vec.size());

  for (string_view lsn_str : lsn_str_vec) {
    int64_t value;
    if (!absl::SimpleAtoi(lsn_str, &value)) {
      cmd_cntx->SendError(facade::kInvalidIntErr);
      return std::nullopt;
    }
    lsn_vec.push_back(value);
  }

  DCHECK(flow_id < lsn_vec.size());
  if (flow_id >= lsn_vec.size()) {
    LOG(ERROR) << "Invalid flow_id: " << flow_id << " exceeds LSN vector size: " << lsn_vec.size()
               << ". Disabling partial sync.";
    return std::nullopt;
  }

  return {lsn_vec[flow_id]};
}

// DFLY TAKEOVER <timeout_sec> [SAVE] <sync_id>
// timeout_sec - number of seconds to wait for TAKEOVER to converge.
// SAVE option is used only by tests.
void DflyCmd::TakeOver(CmdArgParser parser, CommandContext* cmd_cntx) {
  float timeout = std::ceil(parser.Next<float>());
  if (timeout < 0) {
    // allow 0s timeout for tests.
    return cmd_cntx->SendError("timeout is negative");
  }

  bool save_flag = static_cast<bool>(parser.Check("SAVE"));

  string_view sync_id_str = parser.Next<std::string_view>();

  RETURN_ON_PARSE_ERROR(parser, cmd_cntx);

  VLOG(1) << "Got DFLY TAKEOVER " << sync_id_str << " time out:" << timeout;

  auto [sync_id, replica_ptr] = GetReplicaInfoOrReply(sync_id_str, cmd_cntx);
  if (!sync_id)
    return;

  {
    dfly::SharedLock lk{replica_ptr->GetMutex()};
    if (!CheckReplicaStateOrReply(*replica_ptr, SyncState::STABLE_SYNC, cmd_cntx))
      return;

    auto prev_state = sf_->service().SwitchState(GlobalState::ACTIVE, GlobalState::TAKEN_OVER);
    if (prev_state != GlobalState::ACTIVE) {
      LOG(WARNING) << prev_state << " in progress, could not take over";
      return cmd_cntx->SendError("Takeover failed!");
    }
  }

  auto cluster_config_before = cluster::ClusterConfig::Current();

  LOG(INFO) << "Takeover initiated, locking down the database.";
  absl::Duration timeout_dur = absl::Seconds(timeout);
  absl::Time end_time = absl::Now() + timeout_dur;
  AggregateStatus status;

  // We need to await for all dispatches to finish: Otherwise a transaction might be scheduled
  // after this function exits but before the actual shutdown.
  facade::DispatchTracker tracker{sf_->GetNonPriviligedListeners(), cmd_cntx->conn(), false, false};
  shard_set->pool()->AwaitFiberOnAll([&](unsigned index, auto* pb) {
    sf_->CancelBlockingOnThread();
    tracker.TrackOnThread();
  });

  if (!tracker.Wait(timeout_dur)) {
    LOG(WARNING) << "Couldn't wait for commands to finish dispatching. " << timeout_dur;
    status = OpStatus::TIMED_OUT;

    auto cb = [&](unsigned thread_index, util::Connection* conn) {
      facade::Connection* dcon = static_cast<facade::Connection*>(conn);
      LOG(INFO) << dcon->DebugInfo();
    };

    for (auto* listener : sf_->GetListeners()) {
      listener->TraverseConnections(cb);
    }
  }

  VLOG(1) << "AwaitCurrentDispatches done";

  absl::Cleanup cleanup([] {
    VLOG(2) << "Enabling expiration";
    shard_set->RunBriefInParallel([](EngineShard* shard) {
      namespaces->GetDefaultNamespace().GetDbSlice(shard->shard_id()).SetExpireAllowed(true);
    });
  });

  atomic_bool catchup_success = true;
  if (*status == OpStatus::OK) {
    dfly::SharedLock lk{replica_ptr->GetMutex()};
    auto cb = [replica_ptr = replica_ptr, end_time, &catchup_success](EngineShard* shard) {
      // PING to force the replica to send the last acked lsn.
      if (!WaitReplicaFlowToCatchup(end_time, replica_ptr.get(), shard, true)) {
        catchup_success.store(false);
      }
    };
    shard_set->RunBlockingInParallel(std::move(cb));
  }

  VLOG(1) << "WaitReplicaFlowToCatchup done";

  if (*status != OpStatus::OK || !catchup_success.load()) {
    sf_->service().SwitchState(GlobalState::TAKEN_OVER, GlobalState::ACTIVE);
    return cmd_cntx->SendError("Takeover failed!");
  }

  cmd_cntx->SendOk();

  atomic_bool rest_catchup_success = true;
  {
    util::fb2::LockGuard mu_lk(mu_);
    for (auto [id, repl_ptr] : replica_infos_) {
      if (replica_ptr == repl_ptr) {
        continue;
      }

      auto cb = [repl_ptr = repl_ptr, end_time, &rest_catchup_success](EngineShard* shard) {
        // We can't PING here as it will advance our LSN and disable partial sync for these nodes.
        // Instead, wait and be optimistic that the end_time is not short. If the nodes didn't sync
        // up in time, it's ok, they will fall back to full sync when reconfigured.
        if (!WaitReplicaFlowToCatchup(end_time, repl_ptr.get(), shard, false)) {
          rest_catchup_success.store(false);
        }
      };
      shard_set->RunBlockingInParallel(std::move(cb));
    }

    if (!rest_catchup_success) {
      LOG(WARNING) << "Some of the replica nodes did not sync in time.";
    }
  }

  if (save_flag) {
    VLOG(1) << "Save snapshot after Takeover.";
    if (auto ec = sf_->DoSave(true); ec) {
      LOG(WARNING) << "Failed to perform snapshot " << ec.Format();
    }
  }

  // For non-cluster mode we shutdown
  if (detail::cluster_mode != detail::ClusterMode::kRealCluster) {
    VLOG(1) << "Takeover accepted, shutting down.";
    CommandContext child_cmd_cntx{cmd_cntx->rb(), nullptr};
    child_cmd_cntx.PushArg("NOSAVE");
    sf_->ShutdownCmd(facade::CmdArgParser{child_cmd_cntx, 0}, &child_cmd_cntx);
    return;
  }

  auto cluster_config_after = cluster::ClusterConfig::Current();
  if (cluster_config_after.get() != cluster_config_before.get()) {
    LOG(INFO) << "ReconcileMasterSlots() early exit. Config already updated";
    return;
  }
  sf_->service().cluster_family().ReconcileMasterSlots(replica_ptr->GetId());
}

void DflyCmd::Expire(CmdArgParser parser, CommandContext* cmd_cntx) {
  cmd_cntx->tx()->ScheduleSingleHop([](Transaction* t, EngineShard* shard) {
    t->GetDbSlice(shard->shard_id()).ExpireAllIfNeeded();
    return OpStatus::OK;
  });

  return cmd_cntx->SendOk();
}

void DflyCmd::ReplicaOffset(CmdArgParser parser, CommandContext* cmd_cntx) {
  std::vector<LSN> lsns(shard_set->size());
  shard_set->RunBriefInParallel([&](EngineShard* shard) {
    lsns[shard->shard_id()] = shard->journal() ? journal::GetLsn() : 0;
  });

  auto* rb = static_cast<facade::RedisReplyBuilder*>(cmd_cntx->rb());
  rb->SendLongArr(absl::MakeConstSpan(lsns));
}

void DflyCmd::Load(CmdArgParser parser, CommandContext* cmd_cntx) {
  string filename = parser.Next<string>();
  ServerFamily::LoadExistingKeys existing_keys = ServerFamily::LoadExistingKeys::kFail;

  if (parser.HasNext()) {
    parser.ExpectTag("APPEND");
    existing_keys = ServerFamily::LoadExistingKeys::kOverride;
  }

  if (parser.TakeError() || parser.HasNext() || filename.empty()) {
    return cmd_cntx->SendError(kSyntaxErr);
  }

  if (existing_keys == ServerFamily::LoadExistingKeys::kFail) {
    sf_->FlushAll(cmd_cntx->server_conn_cntx()->ns);
  }

  if (auto fut_ec = sf_->Load(filename, existing_keys); fut_ec) {
    GenericError ec = fut_ec->Get();
    if (ec) {
      string msg = ec.Format();
      LOG(WARNING) << "Could not load file " << msg;
      return cmd_cntx->SendError(msg);
    }
  }

  cmd_cntx->SendOk();
}

OpStatus DflyCmd::StartFullSyncInThread(DflyVersion version, FlowInfo* flow,
                                        ExecutionState* exec_st, EngineShard* shard) {
  DCHECK(shard);
  DCHECK(flow->conn);

  // The summary contains the LUA scripts, so make sure at least (and exactly one)
  // of the flows also contain them.
  SaveMode save_mode =
      shard->shard_id() == 0 ? SaveMode::SINGLE_SHARD_WITH_SUMMARY : SaveMode::SINGLE_SHARD;
  flow->saver = std::make_unique<RdbSaver>(flow->conn->socket(), save_mode, false, "", version);

  flow->cleanup = [flow, shard]() {
    // socket shutdown is needed before calling saver->Cancel(). Because
    // we might cancel while the write to socket is blocking and
    // therefore if we wont cancel the socket the full sync fiber might
    // not get to pop entries from channel, which can cause dead lock if channel is full and some
    // callbacks are blocked on trying to insert to channel.
    flow->TryShutdownSocket();
    flow->saver->CancelInShard(shard);  // stops writing to journal stream to channel
    flow->saver.reset();
  };

  error_code ec;
  RdbSaver* saver = flow->saver.get();
  if (saver->Mode() == SaveMode::SUMMARY || saver->Mode() == SaveMode::SINGLE_SHARD_WITH_SUMMARY) {
    // Full sync summary - include all global data
    ec = saver->SaveHeader(saver->GetGlobalData(&sf_->service(), true));
  } else {
    // Per-shard - include only search index restore commands
    ec = saver->SaveHeader(saver->GetGlobalData(&sf_->service(), false));
  }
  if (ec) {
    exec_st->ReportError(ec);
    return OpStatus::CANCELLED;
  }

  saver->StartSnapshotInShard(true, exec_st, shard);

  return OpStatus::OK;
}

OpStatus DflyCmd::StopFullSyncInThread(FlowInfo* flow, ExecutionState* exec_st,
                                       EngineShard* shard) {
  DCHECK(shard);

  error_code ec = flow->saver->StopFullSyncInShard(shard);
  if (ec) {
    exec_st->ReportError(ec);
    return OpStatus::CANCELLED;
  }

  ec = flow->conn->socket()->Write(io::Buffer(flow->eof_token));
  if (ec) {
    exec_st->ReportError(ec);
    return OpStatus::CANCELLED;
  }

  // Reset cleanup and saver
  flow->cleanup = []() {};
  flow->saver.reset();
  return OpStatus::OK;
}

void DflyCmd::StartStableSyncInThread(FlowInfo* flow, ExecutionState* exec_st, EngineShard* shard) {
  // Create streamer for shard flows.
  DCHECK(shard);
  DCHECK(flow->conn);

  LSN partial_lsn = flow->start_partial_sync_at.value_or(0);
  JournalStreamer::Config config{
      .should_sent_lsn = true, .init_from_stable_sync = true, .start_partial_sync_at = partial_lsn};
  flow->streamer.reset(new JournalStreamer(exec_st, config));
  flow->streamer->Start(flow->conn->socket());

  // Register cleanup.
  flow->cleanup = [flow]() {
    flow->TryShutdownSocket();
    if (flow->streamer) {
      flow->streamer->Cancel();
    }
  };
}

auto DflyCmd::CreateSyncSession(ConnectionState* state) -> std::pair<uint32_t, unsigned> {
  util::fb2::LockGuard lk(mu_);
  unsigned sync_id = next_sync_id_++;

  unsigned flow_count = shard_set->size();
  auto err_handler = [this, sync_id](const GenericError& err) {
    LOG(INFO) << "Replication error: " << err.Format();

    // Spawn external fiber to allow destructing the context from outside
    // and return from the handler immediately.
    fb2::Fiber("stop_replication", &DflyCmd::StopReplication, this, sync_id).Detach();
  };

  string address = state->replication_info.repl_ip_address;
  uint32_t port = state->replication_info.repl_listening_port;

  LOG(INFO) << "Registered replica " << address << ":" << port;

  auto replica_ptr =
      make_shared<ReplicaInfo>(flow_count, std::move(address), port, std::move(err_handler));
  auto [it, inserted] = replica_infos_.emplace(sync_id, std::move(replica_ptr));
  CHECK(inserted);

  UpdateReplicaInfoCacheLocked();
  return {it->first, flow_count};
}

auto DflyCmd::GetReplicaInfoFromConnection(ConnectionState* state) -> std::shared_ptr<ReplicaInfo> {
  util::fb2::LockGuard lk(mu_);
  auto it = replica_infos_.find(state->replication_info.repl_session_id);
  if (it == replica_infos_.end()) {
    return nullptr;
  }

  return it->second;
}

void DflyCmd::OnClose(unsigned sync_id) {
  if (!sync_id)
    return;
  StopReplication(sync_id);
}

void DflyCmd::StopReplication(uint32_t sync_id) {
  auto replica_ptr = GetReplicaInfo(sync_id);
  if (!replica_ptr)
    return;
  VLOG(1) << "Stopping replication for sync_id: " << sync_id;

  // Because CancelReplication holds the per-replica mutex,
  // aborting connection will block here until cancellation finishes.
  // This allows keeping resources alive during the cleanup phase.
  replica_ptr->Cancel();

  util::fb2::LockGuard lk(mu_);
  replica_infos_.erase(sync_id);
  UpdateReplicaInfoCacheLocked();
}

// Because we need to annotate unique_lock
void DflyCmd::BreakStalledFlowsInShard() {
  std::unique_lock global_lock(mu_, try_to_lock);

  // give up on blocking because we run this function periodically in a background fiber,
  // so it will eventually grab the lock.
  if (!global_lock.owns_lock())
    return;

  ShardId sid = EngineShard::tlocal()->shard_id();

  vector<uint32_t> deleted;

  for (auto [sync_id, replica_ptr] : replica_infos_) {
    dfly::SharedLock replica_lock{replica_ptr->GetMutex()};

    if (!replica_ptr->GetFlow(sid).saver)
      continue;

    // If saver is present - we are currently using it for full sync.
    int64_t last_write_ns = replica_ptr->GetFlow(sid).saver->GetLastWriteTime();
    int64_t timeout_ns = int64_t(absl::GetFlag(FLAGS_replication_timeout)) * 1'000'000LL;
    int64_t now = absl::GetCurrentTimeNanos();
    if (last_write_ns > 0 && last_write_ns + timeout_ns < now) {
      LOG(INFO) << "Master detected replication timeout, breaking full sync with replica, sync_id: "
                << sync_id << " last_write_ms: " << last_write_ns / 1000'000
                << ", now: " << now / 1000'000;

      deleted.push_back(sync_id);
      replica_lock.unlock();
      replica_ptr->Cancel();
    }
  }

  for (auto sync_id : deleted)
    replica_infos_.erase(sync_id);

  if (!deleted.empty())
    UpdateReplicaInfoCacheLocked();
}

shared_ptr<DflyCmd::ReplicaInfo> DflyCmd::GetReplicaInfo(uint32_t sync_id) {
  util::fb2::LockGuard lk(mu_);

  auto it = replica_infos_.find(sync_id);
  if (it != replica_infos_.end())
    return it->second;
  return {};
}

std::vector<std::shared_ptr<DflyCmd::ReplicaInfo>> DflyCmd::GetReplicaInfoSnapshot() const {
  util::fb2::LockGuard lk(mu_);
  std::vector<std::shared_ptr<ReplicaInfo>> result;
  result.reserve(replica_infos_.size());
  for (const auto& [id, ptr] : replica_infos_) {
    result.push_back(ptr);
  }
  return result;
}

std::vector<ReplicaRoleInfo> DflyCmd::GetReplicasRoleInfo() const {
  std::vector<ReplicaRoleInfo> vec;
  auto replica_infos = tl_replica_infos;
  if (!replica_infos)
    return vec;

  vec.reserve(replica_infos->size());
  std::map<uint32_t, LSN> replication_lags = ReplicationLags(*replica_infos);

  for (const auto& [id, info] : *replica_infos) {
    SyncState state = info->GetReplicaState();
    // ReplicationLags only populates entries for STABLE_SYNC replicas, so a missing
    // entry defaults to lag=0 — exactly what we want for any other state.
    LSN lag = replication_lags[id];
    vec.push_back(ReplicaRoleInfo{std::string{info->GetId()}, info->GetAddress(),
                                  info->GetListeningPort(), SyncStateName(state), lag});
  }
  return vec;
}

ReplicationMemoryStats DflyCmd::GetReplicationMemoryStats(EngineShard* shard) {
  // Must run on the shard's own proactor: it reads thread-local tl_replica_infos
  // and the shard's flow saver/streamer, which only this thread mutates — so the
  // read is lock-free and a wrong-thread call would report 0/stale stats.
  DCHECK(shard && shard->IsMyThread());

  ReplicationMemoryStats stats;
  auto replica_infos = tl_replica_infos;
  if (!replica_infos)
    return stats;

  for (const auto& [_, info] : *replica_infos) {
    DCHECK_GT(info->GetFlowCount(), 0u);
    if (info->GetFlowCount() == 0)
      continue;

    const auto& flow = info->GetFlow(shard->shard_id());
    if (flow.streamer)
      stats.streamer_buf_capacity_bytes += flow.streamer->UsedBytes();
    if (flow.saver) {
      // Replication-flow savers must be single-shard, otherwise the saver's
      // GetTotalBuffersSize would internally call RunBriefInParallel and nest.
      DCHECK(flow.saver->Mode() == SaveMode::SINGLE_SHARD ||
             flow.saver->Mode() == SaveMode::SINGLE_SHARD_WITH_SUMMARY);
      stats.full_sync_buf_bytes += flow.saver->GetTotalBuffersSize();
    }
  }
  return stats;
}

pair<uint32_t, shared_ptr<DflyCmd::ReplicaInfo>> DflyCmd::GetReplicaInfoOrReply(
    std::string_view id_str, CommandContext* cmd_cntx) {
  uint32_t sync_id;
  if (!ToSyncId(id_str, &sync_id)) {
    cmd_cntx->SendError(kInvalidSyncId);
    return {0, nullptr};
  }

  util::fb2::LockGuard lk(mu_);
  auto sync_it = replica_infos_.find(sync_id);
  if (sync_it == replica_infos_.end()) {
    cmd_cntx->SendError(kIdNotFound);
    return {0, nullptr};
  }

  return {sync_id, sync_it->second};
}

std::map<uint32_t, LSN> DflyCmd::ReplicationLags(const ReplicaInfoMap& replicas_local_cache) const {
  if (replicas_local_cache.empty())
    return {};

  // In each shard we calculate a map of replica id to replication lag in the shard.
  // Only compute lag for replicas currently in STABLE_SYNC — for any other state the
  // lag is undefined (e.g. last_acked_lsn is still 0 mid-full-sync). Replicas that
  // transition out of (or into) STABLE_SYNC between this scan and the outer reader
  // are naturally handled: missing entries default to lag=0, and the outer reader
  // re-checks state before emitting the lag.
  // We fan out via RunBriefInParallel only because journal::GetLsn() is per-shard
  // thread-local state that must be read on each shard's own proactor.
  std::vector<std::map<uint32_t, LSN>> shard_lags(shard_set->size());
  shard_set->RunBriefInParallel([&shard_lags, &replicas_local_cache](EngineShard* shard) {
    if (!shard->journal())
      return;
    auto& lags = shard_lags[shard->shard_id()];
    const LSN cur_lsn = journal::GetLsn();
    for (const auto& info : replicas_local_cache) {
      const ReplicaInfo* replica = info.second.get();
      if (replica->GetReplicaState() != SyncState::STABLE_SYNC)
        continue;
      lags[info.first] = cur_lsn - replica->GetFlow(shard->shard_id()).last_acked_lsn;
    }
  });

  // Merge the maps from all shards and derive the maximum lag for each replica.
  std::map<uint32_t, LSN> rv;
  for (const auto& lags : shard_lags) {
    for (auto [replica_id, lag] : lags) {
      rv[replica_id] = std::max(rv[replica_id], lag);
    }
  }
  return rv;
}

void DflyCmd::UpdateReplicaInfoCacheLocked() {
  auto replica_infos = std::make_shared<const ReplicaInfoMap>(replica_infos_);
  // Dispatching under mu_ keeps
  // the per-proactor caches consistent — concurrent callers enqueue their updates in a
  // serialized order, so all proactors observe the same final snapshot. The shared_ptr is
  // captured by value because the local goes out of scope before the lambda runs.
  shard_set->pool()->DispatchBrief(
      [replica_infos](unsigned, ProactorBase*) { tl_replica_infos = replica_infos; });
}

void DflyCmd::SetDflyClientVersion(ConnectionState* state, DflyVersion version) {
  auto replica_ptr = GetReplicaInfo(state->replication_info.repl_session_id);
  VLOG(1) << "Client version for session_id=" << state->replication_info.repl_session_id << " is "
          << int(version);

  replica_ptr->SetVersion(version);
}

// Must run under locked replica_info.mu.
// TODO: it's a bad design that we enforce replies under a lock because Send can potentially
// block, leading to high contention in some case. Split it and avoid replying under a lock.
bool DflyCmd::CheckReplicaStateOrReply(const ReplicaInfo& repl_info, SyncState expected,
                                       CommandContext* cmd_cntx) {
  if (repl_info.GetReplicaState() != expected) {
    cmd_cntx->SendError(kInvalidState);
    return false;
  }

  // This might happen if a flow abruptly disconnected before sending the SYNC request.
  if (!repl_info.AllFlowsConnected()) {
    cmd_cntx->SendError(kInvalidState);
    return false;
  }

  return true;
}

void DflyCmd::CancelReplicas() {
  ReplicaInfoMap pending;
  {
    util::fb2::LockGuard lk(mu_);
    pending = std::move(replica_infos_);
    UpdateReplicaInfoCacheLocked();
  }

  for (auto& [_, replica_ptr] : pending) {
    replica_ptr->Cancel();
  }
}

void FlowInfo::TryShutdownSocket() {
  // Close socket for clean disconnect.
  if (conn->socket()->IsOpen()) {
    std::ignore = conn->socket()->Shutdown(SHUT_RDWR);
  }
}

FlowInfo::~FlowInfo() {
}

FlowInfo::FlowInfo() {
}

}  // namespace dfly
