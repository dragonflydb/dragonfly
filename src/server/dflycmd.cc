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
#include "facade/cmd_arg_parser.h"
#include "facade/dragonfly_connection.h"
#include "facade/dragonfly_listener.h"
#include "facade/reply_builder.h"
#include "server/cluster_support.h"
#include "server/debugcmd.h"
#include "server/engine_shard_set.h"
#include "server/error.h"
#include "server/journal/journal.h"
#include "server/journal/streamer.h"
#include "server/main_service.h"
#include "server/rdb_save.h"
#include "server/script_mgr.h"
#include "server/server_family.h"
#include "server/server_state.h"
#include "server/transaction.h"
#include "util/fibers/synchronization.h"
using namespace std;

ABSL_RETIRED_FLAG(uint32_t, allow_partial_sync_with_lsn_diff, 0,
                  "Do partial sync in case lsn diff is less than the given threshold");
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

bool ToSyncId(string_view str, uint32_t* num) {
  if (!absl::StartsWith(str, "SYNC"))
    return false;
  str.remove_prefix(4);

  return absl::SimpleAtoi(str, num);
}

bool WaitReplicaFlowToCatchup(absl::Time end_time, const DflyCmd::ReplicaInfo* replica,
                              EngineShard* shard) {
  // We don't want any writes to the journal after we send the `PING`,
  // and expirations could ruin that.
  namespaces->GetDefaultNamespace().GetDbSlice(shard->shard_id()).SetExpireAllowed(false);
  shard->journal()->RecordEntry(0, journal::Op::PING, 0, 0, nullopt, {});

  const FlowInfo* flow = &replica->flows[shard->shard_id()];

  while (flow->last_acked_lsn < shard->journal()->GetLsn()) {
    if (absl::Now() > end_time) {
      LOG(WARNING) << "Couldn't synchronize with replica for takeover in time: " << replica->address
                   << ":" << replica->listening_port << ", last acked: " << flow->last_acked_lsn
                   << ", expecting " << shard->journal()->GetLsn();
      return false;
    }
    if (!replica->exec_st.IsRunning()) {
      return false;
    }
    LOG_EVERY_T(INFO, 1) << "Replica lsn:" << flow->last_acked_lsn
                         << " master lsn:" << shard->journal()->GetLsn()
                         << "; Journal streamer state: " << flow->streamer->FormatInternalState();
    ThisFiber::SleepFor(1ms);
  }

  return true;
}

}  // namespace

void DflyCmd::ReplicaInfo::Cancel() {
  util::fb2::LockGuard lk{shared_mu};
  if (replica_state == SyncState::CANCELLED) {
    return;
  }

  LOG(INFO) << "Disconnecting from replica " << address << ":" << listening_port;

  // Update state and cancel context.
  replica_state = SyncState::CANCELLED;
  exec_st.ReportCancelError();
  // Wait for tasks to finish.
  shard_set->RunBlockingInParallel([this](EngineShard* shard) {
    VLOG(2) << "Disconnecting flow " << shard->shard_id();

    FlowInfo* flow = &flows[shard->shard_id()];
    if (flow->cleanup) {
      flow->cleanup();
    }
    VLOG(2) << "After flow cleanup " << shard->shard_id();
    flow->conn = nullptr;
  });
  // Wait for error handler to quit.
  exec_st.JoinErrorHandler();
  VLOG(1) << "Disconnecting replica " << address << ":" << listening_port;
}

DflyCmd::DflyCmd(ServerFamily* server_family) : sf_(server_family) {
}

void DflyCmd::Run(CmdArgList args, Transaction* tx, facade::RedisReplyBuilder* rb,
                  ConnectionContext* cntx) {
  DCHECK_GE(args.size(), 1u);
  string sub_cmd = absl::AsciiStrToUpper(ArgS(args, 0));

  if (sub_cmd == "THREAD") {
    return Thread(args, rb, cntx);
  }

  if (sub_cmd == "FLOW" && (args.size() >= 4 && args.size() <= 6)) {
    return Flow(args, rb, cntx);
  }

  if (sub_cmd == "SYNC" && args.size() == 2) {
    return Sync(args, tx, rb);
  }

  if (sub_cmd == "STARTSTABLE" && args.size() == 2) {
    return StartStable(args, tx, rb);
  }

  if (sub_cmd == "TAKEOVER" && (args.size() == 3 || args.size() == 4)) {
    return TakeOver(args, rb, cntx);
  }

  if (sub_cmd == "EXPIRE") {
    return Expire(args, tx, rb);
  }

  if (sub_cmd == "REPLICAOFFSET" && args.size() == 1) {
    return ReplicaOffset(args, rb);
  }

  if (sub_cmd == "LOAD") {
    return Load(args, rb, cntx);
  }

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

  rb->SendError(kSyntaxErr);
}

void DflyCmd::Thread(CmdArgList args, RedisReplyBuilder* rb, ConnectionContext* cntx) {
  util::ProactorPool* pool = shard_set->pool();

  if (args.size() == 1) {  // DFLY THREAD : returns connection thread index and number of threads.
    rb->StartArray(2);
    rb->SendLong(ProactorBase::me()->GetPoolIndex());
    rb->SendLong(long(pool->size()));
    return;
  }

  // DFLY THREAD to_thread : migrates current connection to a different thread.
  string_view arg = ArgS(args, 1);
  unsigned num_thread;
  if (!absl::SimpleAtoi(arg, &num_thread)) {
    return rb->SendError(kSyntaxErr);
  }

  if (num_thread < pool->size()) {
    if (int(num_thread) != ProactorBase::me()->GetPoolIndex()) {
      if (!cntx->conn()->Migrate(pool->at(num_thread))) {
        // Listener::PreShutdown() triggered
        if (cntx->conn()->socket()->IsOpen()) {
          return rb->SendError(kInvalidState);
        }
        return;
      }
    }

    return rb->SendOk();
  }

  return rb->SendError(kInvalidIntErr);
}

void DflyCmd::Flow(CmdArgList args, RedisReplyBuilder* rb, ConnectionContext* cntx) {
  string_view master_id = ArgS(args, 1);
  string_view sync_id_str = ArgS(args, 2);
  string_view flow_id_str = ArgS(args, 3);

  std::optional<LSN> seqid;
  std::optional<string> last_master_id;
  std::optional<string> last_master_lsn;
  if (args.size() == 5) {
    seqid.emplace();
    if (!absl::SimpleAtoi(ArgS(args, 4), &seqid.value())) {
      return rb->SendError(facade::kInvalidIntErr);
    }
  } else if (args.size() == 6) {
    last_master_id = ArgS(args, 4);
    last_master_lsn = ArgS(args, 5);
  }

  VLOG(1) << "Got DFLY FLOW master_id: " << master_id << " sync_id: " << sync_id_str
          << " flow: " << flow_id_str << " seq: " << seqid.value_or(-1);

  if (master_id != sf_->master_replid()) {
    return rb->SendError(kBadMasterId);
  }

  unsigned flow_id;
  if (!absl::SimpleAtoi(flow_id_str, &flow_id) || flow_id >= shard_set->size()) {
    return rb->SendError(facade::kInvalidIntErr);
  }

  auto [sync_id, replica_ptr] = GetReplicaInfoOrReply(sync_id_str, rb);
  if (!sync_id)
    return;

  string eof_token;
  std::string sync_type{"FULL"};
  {
    util::fb2::LockGuard lk{replica_ptr->shared_mu};

    if (replica_ptr->replica_state != SyncState::PREPARATION) {
      return rb->SendError(kInvalidState);
    }

    // Set meta info on connection.
    cntx->conn()->SetName(absl::StrCat("repl_flow_", sync_id));
    cntx->conn_state.replication_info.repl_session_id = sync_id;
    cntx->conn_state.replication_info.repl_flow_id = flow_id;
    cntx->replica_conn = true;

    absl::InsecureBitGen gen;
    eof_token = GetRandomHex(gen, 40);

    auto& flow = replica_ptr->flows[flow_id];
    cntx->replication_flow = &flow;
    flow.conn = cntx->conn();
    flow.eof_token = eof_token;
    flow.version = replica_ptr->version;

    if (!cntx->conn()->Migrate(shard_set->pool()->at(flow_id))) {
      // Listener::PreShutdown() triggered
      if (cntx->conn()->socket()->IsOpen()) {
        return rb->SendError(kInvalidState);
      }
      return;
    }

    sf_->journal()->StartInThread();

    std::optional<Replica::LastMasterSyncData> data = sf_->GetLastMasterData();
    std::optional<LSN> lsn_to_start_partial;
    // In this flow the master and the registered replica where synced from the same master.
    if (last_master_id && data && data->id == *last_master_id) {
      ++ServerState::tlocal()->stats.psync_requests_total;
      auto flow_lsn = ParseLsnVec(*last_master_lsn, data->last_journal_LSNs.size(), flow_id, rb);
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
              << " and is available. (current_lsn=" << sf_->journal()->GetLsn() << ")";
    }
  }

  rb->StartArray(2);
  rb->SendSimpleString(sync_type);
  rb->SendSimpleString(eof_token);
}

void DflyCmd::Sync(CmdArgList args, Transaction* tx, RedisReplyBuilder* rb) {
  string_view sync_id_str = ArgS(args, 1);

  VLOG(1) << "Got DFLY SYNC " << sync_id_str;

  auto [sync_id, replica_ptr] = GetReplicaInfoOrReply(sync_id_str, rb);
  if (!sync_id)
    return;

  util::fb2::LockGuard lk{replica_ptr->shared_mu};
  if (!CheckReplicaStateOrReply(*replica_ptr, SyncState::PREPARATION, rb))
    return;

  // Start full sync.
  {
    Transaction::Guard tg{tx};
    AggregateStatus status;

    // Use explicit assignment for replica_ptr, because capturing structured bindings is C++20.
    auto cb = [this, &status, replica_ptr = replica_ptr](EngineShard* shard) {
      status = StartFullSyncInThread(&replica_ptr->flows[shard->shard_id()], &replica_ptr->exec_st,
                                     shard);
    };
    shard_set->RunBlockingInParallel(std::move(cb));

    // TODO: Send better error
    if (*status != OpStatus::OK)
      return rb->SendError(kInvalidState);
  }

  LOG(INFO) << "Started sync with replica " << replica_ptr->address << ":"
            << replica_ptr->listening_port;

  // protected by lk above.
  replica_ptr->replica_state = SyncState::FULL_SYNC;
  return rb->SendOk();
}

void DflyCmd::StartStable(CmdArgList args, Transaction* tx, RedisReplyBuilder* rb) {
  string_view sync_id_str = ArgS(args, 1);

  VLOG(1) << "Got DFLY STARTSTABLE " << sync_id_str;

  auto [sync_id, replica_ptr] = GetReplicaInfoOrReply(sync_id_str, rb);
  if (!sync_id)
    return;

  util::fb2::LockGuard lk{replica_ptr->shared_mu};
  auto repl_state = replica_ptr->replica_state;
  if (repl_state != SyncState::FULL_SYNC && repl_state != SyncState::PREPARATION) {
    rb->SendError(kInvalidState);
    return;
  }

  // Check all flows are connected.
  // This might happen if a flow abruptly disconnected before sending the SYNC request.
  for (const FlowInfo& flow : replica_ptr->flows) {
    if (!flow.conn) {
      rb->SendError(kInvalidState);
      return;
    }
  }

  {
    Transaction::Guard tg{tx};
    AggregateStatus status;

    auto cb = [this, &status, replica_ptr = replica_ptr](EngineShard* shard) {
      FlowInfo* flow = &replica_ptr->flows[shard->shard_id()];

      // We are doing partial sync. We never started FullSync so we don't need to stop it.
      bool is_partial = flow->start_partial_sync_at.has_value();
      if (!is_partial) {
        status = StopFullSyncInThread(flow, &replica_ptr->exec_st, shard);
        if (*status != OpStatus::OK) {
          return;
        }
      }

      StartStableSyncInThread(flow, &replica_ptr->exec_st, shard);
    };
    shard_set->RunBlockingInParallel(std::move(cb));

    if (*status != OpStatus::OK)
      return rb->SendError(kInvalidState);
  }

  LOG(INFO) << "Transitioned into stable sync with replica " << replica_ptr->address << ":"
            << replica_ptr->listening_port;

  replica_ptr->replica_state = SyncState::STABLE_SYNC;
  return rb->SendOk();
}

bool DflyCmd::IsLSNInPartialSyncBuffer(LSN lsn) const {
  auto* jrnl = sf_->journal();

  const bool exists = jrnl->GetLsn() == lsn || jrnl->IsLSNInBuffer(lsn);
  if (!exists) {
    LOG(INFO) << "Partial sync requested from stale LSN=" << lsn
              << " that the replication buffer doesn't contain this anymore (current_lsn="
              << sf_->journal()->GetLsn() << "). Will perform a full sync of the data.";
    LOG(INFO) << "If this happens often you can control the replication buffer's size with the "
                 "--shard_repl_backlog_len option";
  }
  return exists;
}

std::optional<LSN> DflyCmd::ParseLsnVec(std::string_view last_master_lsn,
                                        size_t last_journal_lsn_size, size_t flow_id,
                                        facade::RedisReplyBuilder* rb) {
  std::vector<std::string_view> lsn_str_vec = absl::StrSplit(last_master_lsn, '-');
  if (lsn_str_vec.size() != last_journal_lsn_size) {
    rb->SendError(facade::kSyntaxErr);  // Unexpected flow. LSN vector of same master
                                        // should be the same size on all replicas.
    return std::nullopt;
  }

  std::vector<LSN> lsn_vec;
  lsn_vec.reserve(lsn_str_vec.size());

  for (string_view lsn_str : lsn_str_vec) {
    int64_t value;
    if (!absl::SimpleAtoi(lsn_str, &value)) {
      rb->SendError(facade::kInvalidIntErr);
      return std::nullopt;
    }
    lsn_vec.push_back(value);
  }

  return {lsn_vec[flow_id]};
}

// DFLY TAKEOVER <timeout_sec> [SAVE] <sync_id>
// timeout_sec - number of seconds to wait for TAKEOVER to converge.
// SAVE option is used only by tests.
void DflyCmd::TakeOver(CmdArgList args, RedisReplyBuilder* rb, ConnectionContext* cntx) {
  CmdArgParser parser{args};
  parser.Next();
  float timeout = std::ceil(parser.Next<float>());
  if (timeout < 0) {
    // allow 0s timeout for tests.
    return rb->SendError("timeout is negative");
  }

  bool save_flag = static_cast<bool>(parser.Check("SAVE"));

  string_view sync_id_str = parser.Next<std::string_view>();

  if (auto err = parser.TakeError(); err)
    return rb->SendError(err.MakeReply());

  VLOG(1) << "Got DFLY TAKEOVER " << sync_id_str << " time out:" << timeout;

  auto [sync_id, replica_ptr] = GetReplicaInfoOrReply(sync_id_str, rb);
  if (!sync_id)
    return;

  {
    dfly::SharedLock lk{replica_ptr->shared_mu};
    if (!CheckReplicaStateOrReply(*replica_ptr, SyncState::STABLE_SYNC, rb))
      return;

    auto new_state = sf_->service().SwitchState(GlobalState::ACTIVE, GlobalState::TAKEN_OVER);
    if (new_state != GlobalState::TAKEN_OVER) {
      LOG(WARNING) << new_state << " in progress, could not take over";
      return rb->SendError("Takeover failed!");
    }
  }

  LOG(INFO) << "Takeover initiated, locking down the database.";
  absl::Duration timeout_dur = absl::Seconds(timeout);
  absl::Time end_time = absl::Now() + timeout_dur;
  AggregateStatus status;

  // We need to await for all dispatches to finish: Otherwise a transaction might be scheduled
  // after this function exits but before the actual shutdown.
  facade::DispatchTracker tracker{sf_->GetNonPriviligedListeners(), cntx->conn(), false, false};
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
    dfly::SharedLock lk{replica_ptr->shared_mu};
    auto cb = [replica_ptr = replica_ptr, end_time, &catchup_success](EngineShard* shard) {
      if (!WaitReplicaFlowToCatchup(end_time, replica_ptr.get(), shard)) {
        catchup_success.store(false);
      }
    };
    shard_set->RunBlockingInParallel(std::move(cb));
  }

  VLOG(1) << "WaitReplicaFlowToCatchup done";

  if (*status != OpStatus::OK || !catchup_success.load()) {
    sf_->service().SwitchState(GlobalState::TAKEN_OVER, GlobalState::ACTIVE);
    return rb->SendError("Takeover failed!");
  }

  rb->SendOk();

  atomic_bool rest_catchup_success = true;
  {
    util::fb2::LockGuard mu_lk(mu_);
    for (auto [id, repl_ptr] : replica_infos_) {
      if (replica_ptr == repl_ptr) {
        continue;
      }

      auto cb = [repl_ptr = repl_ptr, end_time, &rest_catchup_success](EngineShard* shard) {
        if (!WaitReplicaFlowToCatchup(end_time, repl_ptr.get(), shard)) {
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
    std::string save_arg = "NOSAVE";
    MutableSlice sargs(save_arg);
    CommandContext cmd_cntx{nullptr, nullptr, rb, nullptr};
    sf_->ShutdownCmd(CmdArgList(&sargs, 1), &cmd_cntx);
    return;
  }

  sf_->service().cluster_family().ReconcileMasterSlots(replica_ptr->id);
}

void DflyCmd::Expire(CmdArgList args, Transaction* tx, RedisReplyBuilder* rb) {
  tx->ScheduleSingleHop([](Transaction* t, EngineShard* shard) {
    t->GetDbSlice(shard->shard_id()).ExpireAllIfNeeded();
    return OpStatus::OK;
  });

  return rb->SendOk();
}

void DflyCmd::ReplicaOffset(CmdArgList args, RedisReplyBuilder* rb) {
  std::vector<LSN> lsns(shard_set->size());
  shard_set->RunBriefInParallel([&](EngineShard* shard) {
    auto* journal = shard->journal();
    lsns[shard->shard_id()] = journal ? journal->GetLsn() : 0;
  });

  rb->SendLongArr(absl::MakeConstSpan(lsns));
}

void DflyCmd::Load(CmdArgList args, RedisReplyBuilder* rb, ConnectionContext* cntx) {
  CmdArgParser parser{args};
  parser.ExpectTag("LOAD");
  string filename = parser.Next<string>();
  ServerFamily::LoadExistingKeys existing_keys = ServerFamily::LoadExistingKeys::kFail;

  if (parser.HasNext()) {
    parser.ExpectTag("APPEND");
    existing_keys = ServerFamily::LoadExistingKeys::kOverride;
  }

  if (parser.TakeError() || parser.HasNext() || filename.empty()) {
    return rb->SendError(kSyntaxErr);
  }

  if (existing_keys == ServerFamily::LoadExistingKeys::kFail) {
    sf_->FlushAll(cntx->ns);
  }

  if (auto fut_ec = sf_->Load(filename, existing_keys); fut_ec) {
    GenericError ec = fut_ec->Get();
    if (ec) {
      string msg = ec.Format();
      LOG(WARNING) << "Could not load file " << msg;
      return rb->SendError(msg);
    }
  }

  rb->SendOk();
}

OpStatus DflyCmd::StartFullSyncInThread(FlowInfo* flow, ExecutionState* exec_st,
                                        EngineShard* shard) {
  DCHECK(shard);
  DCHECK(flow->conn);

  // The summary contains the LUA scripts, so make sure at least (and exactly one)
  // of the flows also contain them.
  SaveMode save_mode =
      shard->shard_id() == 0 ? SaveMode::SINGLE_SHARD_WITH_SUMMARY : SaveMode::SINGLE_SHARD;
  flow->saver = std::make_unique<RdbSaver>(flow->conn->socket(), save_mode, false, "");

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
    ec = saver->SaveHeader(saver->GetGlobalData(&sf_->service()));
  } else {
    ec = saver->SaveHeader({});
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
  flow->streamer.reset(new JournalStreamer(sf_->journal(), exec_st, config));
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
    dfly::SharedLock replica_lock{replica_ptr->shared_mu};

    if (!replica_ptr->flows[sid].saver)
      continue;

    // If saver is present - we are currently using it for full sync.
    int64_t last_write_ns = replica_ptr->flows[sid].saver->GetLastWriteTime();
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
}

shared_ptr<DflyCmd::ReplicaInfo> DflyCmd::GetReplicaInfo(uint32_t sync_id) {
  util::fb2::LockGuard lk(mu_);

  auto it = replica_infos_.find(sync_id);
  if (it != replica_infos_.end())
    return it->second;
  return {};
}

std::vector<ReplicaRoleInfo> DflyCmd::GetReplicasRoleInfo() const {
  std::vector<ReplicaRoleInfo> vec;
  util::fb2::LockGuard lk(mu_);

  vec.reserve(replica_infos_.size());
  map replication_lags = ReplicationLagsLocked();

  for (const auto& [id, info] : replica_infos_) {
    LSN lag = replication_lags[id];
    SyncState state = SyncState::PREPARATION;

    // If the replica state being updated, its lag is undefined,
    // the same applies of course if its state is not STABLE_SYNC.
    shared_lock lk(info->shared_mu, try_to_lock);
    if (lk.owns_lock()) {
      state = info->replica_state;
      // If the replica is not in stable sync, its lag is undefined, so we set it to 0.
      if (state != SyncState::STABLE_SYNC) {
        lag = 0;
      }
    } else {
      lag = 0;
    }
    vec.push_back(
        ReplicaRoleInfo{info->id, info->address, info->listening_port, SyncStateName(state), lag});
  }
  return vec;
}

void DflyCmd::GetReplicationMemoryStats(ReplicationMemoryStats* stats) const {
  atomic<size_t> streamer_bytes{0}, full_sync_bytes{0};

  {
    util::fb2::LockGuard lk{mu_};  // prevent state changes
    auto cb = [&](EngineShard* shard) ABSL_NO_THREAD_SAFETY_ANALYSIS {
      for (const auto& [_, info] : replica_infos_) {
        dfly::SharedLock repl_lk{info->shared_mu};

        // flows should not be empty.
        DCHECK(!info->flows.empty());
        if (info->flows.empty())
          continue;

        const auto& flow = info->flows[shard->shard_id()];
        if (flow.streamer)
          streamer_bytes.fetch_add(flow.streamer->UsedBytes(), memory_order_relaxed);
        if (flow.saver)
          full_sync_bytes.fetch_add(flow.saver->GetTotalBuffersSize(), memory_order_relaxed);
      }
    };
    shard_set->RunBlockingInParallel(cb);
  }
  stats->streamer_buf_capacity_bytes += streamer_bytes.load(memory_order_relaxed);
  stats->full_sync_buf_bytes += full_sync_bytes.load(memory_order_relaxed);
}

pair<uint32_t, shared_ptr<DflyCmd::ReplicaInfo>> DflyCmd::GetReplicaInfoOrReply(
    std::string_view id_str, RedisReplyBuilder* rb) {
  uint32_t sync_id;
  if (!ToSyncId(id_str, &sync_id)) {
    rb->SendError(kInvalidSyncId);
    return {0, nullptr};
  }

  util::fb2::LockGuard lk(mu_);
  auto sync_it = replica_infos_.find(sync_id);
  if (sync_it == replica_infos_.end()) {
    rb->SendError(kIdNotFound);
    return {0, nullptr};
  }

  return {sync_id, sync_it->second};
}

std::map<uint32_t, LSN> DflyCmd::ReplicationLagsLocked() const {
  DCHECK(!mu_.try_lock());  // expects to be under global lock
  if (replica_infos_.empty())
    return {};

  // In each shard we calculate a map of replica id to replication lag in the shard.
  std::vector<std::map<uint32_t, LSN>> shard_lags(shard_set->size());
  shard_set->RunBriefInParallel([&shard_lags, this](EngineShard* shard) {
    auto& lags = shard_lags[shard->shard_id()];
    for (const auto& info : ABSL_TS_UNCHECKED_READ(replica_infos_)) {
      const ReplicaInfo* replica = info.second.get();
      if (shard->journal()) {
        int64_t lag = shard->journal()->GetLsn() - replica->flows[shard->shard_id()].last_acked_lsn;
        lags[info.first] = lag;
      }
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

void DflyCmd::SetDflyClientVersion(ConnectionState* state, DflyVersion version) {
  auto replica_ptr = GetReplicaInfo(state->replication_info.repl_session_id);
  VLOG(1) << "Client version for session_id=" << state->replication_info.repl_session_id << " is "
          << int(version);

  replica_ptr->version = version;
}

// Must run under locked replica_info.mu.
// TODO: it's a bad design that we enforce replies under a lock because Send can potentially
// block, leading to high contention in some case. Split it and avoid replying under a lock.
bool DflyCmd::CheckReplicaStateOrReply(const ReplicaInfo& repl_info, SyncState expected,
                                       RedisReplyBuilder* rb) {
  if (repl_info.replica_state != expected) {
    rb->SendError(kInvalidState);
    return false;
  }

  // Check all flows are connected.
  // This might happen if a flow abruptly disconnected before sending the SYNC request.
  for (const FlowInfo& flow : repl_info.flows) {
    if (!flow.conn) {
      rb->SendError(kInvalidState);
      return false;
    }
  }

  return true;
}

void DflyCmd::Shutdown() {
  ReplicaInfoMap pending;
  {
    util::fb2::LockGuard lk(mu_);
    pending = std::move(replica_infos_);
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
