// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "server/dflycmd.h"

#include <absl/random/random.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/strip.h>

#include "base/flags.h"
#include "base/logging.h"
#include "facade/dragonfly_connection.h"
#include "server/engine_shard_set.h"
#include "server/error.h"
#include "server/journal/journal.h"
#include "server/journal/serializer.h"
#include "server/rdb_save.h"
#include "server/script_mgr.h"
#include "server/server_family.h"
#include "server/server_state.h"
#include "server/transaction.h"

using namespace std;

ABSL_DECLARE_FLAG(string, dir);

namespace dfly {

using namespace facade;
using namespace std;
using util::ProactorBase;

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

struct TransactionGuard {
  constexpr static auto kEmptyCb = [](Transaction* t, EngineShard* shard) { return OpStatus::OK; };

  TransactionGuard(Transaction* t) : t(t) {
    t->Schedule();
    t->Execute(kEmptyCb, false);
  }

  ~TransactionGuard() {
    t->Execute(kEmptyCb, true);
  }

  Transaction* t;
};

}  // namespace

DflyCmd::ReplicaRoleInfo::ReplicaRoleInfo(std::string address, SyncState sync_state)
    : address(address) {
  switch (sync_state) {
    case SyncState::PREPARATION:
      state = "preparation";
      break;
    case SyncState::FULL_SYNC:
      state = "full sync";
      break;
    case SyncState::STABLE_SYNC:
      state = "stable sync";
      break;
    case SyncState::CANCELLED:
      state = "cancelled";
      break;
    default:
      break;
  }
}

DflyCmd::DflyCmd(util::ListenerInterface* listener, ServerFamily* server_family)
    : sf_(server_family), listener_(listener) {
}

void DflyCmd::Run(CmdArgList args, ConnectionContext* cntx) {
  RedisReplyBuilder* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());

  DCHECK_GE(args.size(), 2u);
  ToUpper(&args[1]);
  string_view sub_cmd = ArgS(args, 1);

  if (sub_cmd == "JOURNAL" && args.size() >= 3) {
    return Journal(args, cntx);
  }

  if (sub_cmd == "THREAD") {
    return Thread(args, cntx);
  }

  if (sub_cmd == "FLOW" && args.size() == 5) {
    return Flow(args, cntx);
  }

  if (sub_cmd == "SYNC" && args.size() == 3) {
    return Sync(args, cntx);
  }

  if (sub_cmd == "STARTSTABLE" && args.size() == 3) {
    return StartStable(args, cntx);
  }

  if (sub_cmd == "EXPIRE") {
    return Expire(args, cntx);
  }

  rb->SendError(kSyntaxErr);
}

void DflyCmd::Journal(CmdArgList args, ConnectionContext* cntx) {
  DCHECK_GE(args.size(), 3u);
  ToUpper(&args[2]);

  std::string_view sub_cmd = ArgS(args, 2);
  Transaction* trans = cntx->transaction;
  DCHECK(trans);
  RedisReplyBuilder* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());

  if (sub_cmd == "START") {
    unique_lock lk(mu_);
    journal::Journal* journal = ServerState::tlocal()->journal();
    if (!journal) {
      string dir = absl::GetFlag(FLAGS_dir);

      atomic_uint32_t created{0};
      auto* pool = shard_set->pool();

      auto open_cb = [&](auto* pb) {
        auto ec = sf_->journal()->OpenInThread(true, dir);
        if (ec) {
          LOG(ERROR) << "Could not create journal " << ec;
        } else {
          created.fetch_add(1, memory_order_relaxed);
        }
      };

      pool->AwaitFiberOnAll(open_cb);
      if (created.load(memory_order_acquire) != pool->size()) {
        LOG(FATAL) << "TBD / revert";
      }

      // We can not use transaction distribution mechanism because we must open journal for all
      // threads and not only for shards.
      trans->Schedule();
      auto barrier_cb = [](Transaction* t, EngineShard* shard) { return OpStatus::OK; };
      trans->Execute(barrier_cb, true);

      // tx id starting from which we may reliably fetch journal records.
      journal_txid_ = trans->txid();
    }

    return rb->SendLong(journal_txid_);
  }

  if (sub_cmd == "STOP") {
    unique_lock lk(mu_);
    if (sf_->journal()->EnterLameDuck()) {
      auto barrier_cb = [](Transaction* t, EngineShard* shard) { return OpStatus::OK; };
      trans->ScheduleSingleHop(std::move(barrier_cb));

      auto ec = sf_->journal()->Close();
      LOG_IF(ERROR, ec) << "Error closing journal " << ec;
      journal_txid_ = trans->txid();
    }

    return rb->SendLong(journal_txid_);
  }

  string reply = UnknownSubCmd(sub_cmd, "DFLY");
  return rb->SendError(reply, kSyntaxErrType);
}

void DflyCmd::Thread(CmdArgList args, ConnectionContext* cntx) {
  RedisReplyBuilder* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  util::ProactorPool* pool = shard_set->pool();

  if (args.size() == 2) {  // DFLY THREAD : returns connection thread index and number of threads.
    rb->StartArray(2);
    rb->SendLong(ProactorBase::GetIndex());
    rb->SendLong(long(pool->size()));
    return;
  }

  // DFLY THREAD to_thread : migrates current connection to a different thread.
  string_view arg = ArgS(args, 2);
  unsigned num_thread;
  if (!absl::SimpleAtoi(arg, &num_thread)) {
    return rb->SendError(kSyntaxErr);
  }

  if (num_thread < pool->size()) {
    if (int(num_thread) != ProactorBase::GetIndex()) {
      listener_->Migrate(cntx->owner(), pool->at(num_thread));
    }

    return rb->SendOk();
  }

  return rb->SendError(kInvalidIntErr);
}

void DflyCmd::Flow(CmdArgList args, ConnectionContext* cntx) {
  RedisReplyBuilder* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  string_view master_id = ArgS(args, 2);
  string_view sync_id_str = ArgS(args, 3);
  string_view flow_id_str = ArgS(args, 4);

  VLOG(1) << "Got DFLY FLOW " << master_id << " " << sync_id_str << " " << flow_id_str;

  if (master_id != sf_->master_id()) {
    return rb->SendError(kBadMasterId);
  }

  unsigned flow_id;
  if (!absl::SimpleAtoi(flow_id_str, &flow_id) || flow_id >= shard_set->pool()->size()) {
    return rb->SendError(facade::kInvalidIntErr);
  }

  auto [sync_id, replica_ptr] = GetReplicaInfoOrReply(sync_id_str, rb);
  if (!sync_id)
    return;

  unique_lock lk(replica_ptr->mu);
  if (replica_ptr->state.load(memory_order_relaxed) != SyncState::PREPARATION)
    return rb->SendError(kInvalidState);

  // Set meta info on connection.
  cntx->owner()->SetName(absl::StrCat("repl_flow_", sync_id));
  cntx->conn_state.repl_session_id = sync_id;
  cntx->conn_state.repl_flow_id = flow_id;

  absl::InsecureBitGen gen;
  string eof_token = GetRandomHex(gen, 40);

  replica_ptr->flows[flow_id] = FlowInfo{cntx->owner(), eof_token};
  listener_->Migrate(cntx->owner(), shard_set->pool()->at(flow_id));

  rb->StartArray(2);
  rb->SendSimpleString("FULL");
  rb->SendSimpleString(eof_token);
}

void DflyCmd::Sync(CmdArgList args, ConnectionContext* cntx) {
  RedisReplyBuilder* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  string_view sync_id_str = ArgS(args, 2);

  VLOG(1) << "Got DFLY SYNC " << sync_id_str;

  auto [sync_id, replica_ptr] = GetReplicaInfoOrReply(sync_id_str, rb);
  if (!sync_id)
    return;

  unique_lock lk(replica_ptr->mu);
  if (!CheckReplicaStateOrReply(*replica_ptr, SyncState::PREPARATION, rb))
    return;

  // Start full sync.
  {
    TransactionGuard tg{cntx->transaction};
    AggregateStatus status;

    // Use explicit assignment for replica_ptr, because capturing structured bindings is C++20.
    auto cb = [this, &status, replica_ptr = replica_ptr](unsigned index, auto*) {
      status = StartFullSyncInThread(&replica_ptr->flows[index], &replica_ptr->cntx,
                                     EngineShard::tlocal());
    };
    shard_set->pool()->AwaitFiberOnAll(std::move(cb));

    // TODO: Send better error
    if (*status != OpStatus::OK)
      return rb->SendError(kInvalidState);
  }

  replica_ptr->state.store(SyncState::FULL_SYNC, memory_order_relaxed);
  return rb->SendOk();
}

void DflyCmd::StartStable(CmdArgList args, ConnectionContext* cntx) {
  RedisReplyBuilder* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  string_view sync_id_str = ArgS(args, 2);

  VLOG(1) << "Got DFLY STARTSTABLE " << sync_id_str;

  auto [sync_id, replica_ptr] = GetReplicaInfoOrReply(sync_id_str, rb);
  if (!sync_id)
    return;

  unique_lock lk(replica_ptr->mu);
  if (!CheckReplicaStateOrReply(*replica_ptr, SyncState::FULL_SYNC, rb))
    return;

  {
    TransactionGuard tg{cntx->transaction};
    AggregateStatus status;

    auto cb = [this, &status, replica_ptr = replica_ptr](unsigned index, auto*) {
      EngineShard* shard = EngineShard::tlocal();
      FlowInfo* flow = &replica_ptr->flows[index];

      StopFullSyncInThread(flow, shard);
      status = StartStableSyncInThread(flow, &replica_ptr->cntx, shard);
      return OpStatus::OK;
    };
    shard_set->pool()->AwaitFiberOnAll(std::move(cb));

    if (*status != OpStatus::OK)
      return rb->SendError(kInvalidState);
  }

  replica_ptr->state.store(SyncState::STABLE_SYNC, memory_order_relaxed);
  return rb->SendOk();
}

void DflyCmd::Expire(CmdArgList args, ConnectionContext* cntx) {
  RedisReplyBuilder* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  cntx->transaction->ScheduleSingleHop([](Transaction* t, EngineShard* shard) {
    shard->db_slice().ExpireAllIfNeeded();
    return OpStatus::OK;
  });

  return rb->SendOk();
}

OpStatus DflyCmd::StartFullSyncInThread(FlowInfo* flow, Context* cntx, EngineShard* shard) {
  DCHECK(!flow->full_sync_fb.IsJoinable());

  SaveMode save_mode = shard == nullptr ? SaveMode::SUMMARY : SaveMode::SINGLE_SHARD;
  flow->saver.reset(new RdbSaver(flow->conn->socket(), save_mode, false));

  flow->cleanup = [flow]() {
    flow->saver->Cancel();
    flow->TryShutdownSocket();
  };

  // Shard can be null for io thread.
  CHECK(!sf_->journal()->OpenInThread(false, ""sv));  // can only happen in persistent mode.
  if (shard != nullptr) {
    flow->saver->StartSnapshotInShard(true, cntx->GetCancellation(), shard);
  }

  flow->full_sync_fb = ::boost::fibers::fiber(&DflyCmd::FullSyncFb, this, flow, cntx);
  return OpStatus::OK;
}

void DflyCmd::StopFullSyncInThread(FlowInfo* flow, EngineShard* shard) {
  // Shard can be null for io thread.
  if (shard != nullptr) {
    flow->saver->StopSnapshotInShard(shard);
  }

  // Wait for full sync to finish.
  if (flow->full_sync_fb.IsJoinable()) {
    flow->full_sync_fb.Join();
  }

  // Reset cleanup and saver
  flow->cleanup = []() {};
  flow->saver.reset();
}

OpStatus DflyCmd::StartStableSyncInThread(FlowInfo* flow, Context* cntx, EngineShard* shard) {
  // Register journal listener and cleanup.
  uint32_t cb_id = 0;
  JournalWriter* writer = nullptr;
  if (shard != nullptr) {
    writer = new JournalWriter{};
    auto journal_cb = [flow, cntx, writer](const journal::Entry& je) mutable {
      writer->Write(je);
      if (auto ec = writer->Flush(flow->conn->socket()); ec)
        cntx->ReportError(ec);
    };
    cb_id = sf_->journal()->RegisterOnChange(std::move(journal_cb));
  }

  flow->cleanup = [this, cb_id, writer, flow]() {
    if (writer)
      delete writer;
    if (cb_id)
      sf_->journal()->UnregisterOnChange(cb_id);
    flow->TryShutdownSocket();
  };

  return OpStatus::OK;
}

void DflyCmd::FullSyncFb(FlowInfo* flow, Context* cntx) {
  error_code ec;
  RdbSaver* saver = flow->saver.get();

  if (saver->Mode() == SaveMode::SUMMARY) {
    auto scripts = sf_->script_mgr()->GetLuaScripts();
    ec = saver->SaveHeader(scripts);
  } else {
    ec = saver->SaveHeader({});
  }

  if (ec) {
    cntx->ReportError(ec);
    return;
  }

  if (ec = saver->SaveBody(cntx->GetCancellation(), nullptr); ec) {
    cntx->ReportError(ec);
    return;
  }

  ec = flow->conn->socket()->Write(io::Buffer(flow->eof_token));
  if (ec) {
    cntx->ReportError(ec);
    return;
  }
}

uint32_t DflyCmd::CreateSyncSession(ConnectionContext* cntx) {
  ;
  unique_lock lk(mu_);
  unsigned sync_id = next_sync_id_++;

  unsigned flow_count = shard_set->pool()->size();
  auto err_handler = [this, sync_id](const GenericError& err) {
    LOG(INFO) << "Replication error: " << err.Format();

    // Spawn external fiber to allow destructing the context from outside
    // and return from the handler immediately.
    ::boost::fibers::fiber{&DflyCmd::StopReplication, this, sync_id}.detach();
  };

  auto replica_ptr = make_shared<ReplicaInfo>(flow_count, cntx->owner()->RemoteEndpointAddress(),
                                              std::move(err_handler));
  auto [it, inserted] = replica_infos_.emplace(sync_id, std::move(replica_ptr));
  CHECK(inserted);

  return sync_id;
}

void DflyCmd::OnClose(ConnectionContext* cntx) {
  unsigned session_id = cntx->conn_state.repl_session_id;
  if (!session_id)
    return;

  auto replica_ptr = GetReplicaInfo(session_id);
  if (!replica_ptr)
    return;

  // Because CancelReplication holds the per-replica mutex,
  // aborting connection will block here until cancellation finishes.
  // This allows keeping resources alive during the cleanup phase.
  CancelReplication(session_id, replica_ptr);
}

void DflyCmd::StopReplication(uint32_t sync_id) {
  auto replica_ptr = GetReplicaInfo(sync_id);
  if (!replica_ptr)
    return;

  CancelReplication(sync_id, replica_ptr);
}

void DflyCmd::CancelReplication(uint32_t sync_id, shared_ptr<ReplicaInfo> replica_ptr) {
  lock_guard lk(replica_ptr->mu);
  if (replica_ptr->state.load(memory_order_relaxed) == SyncState::CANCELLED) {
    return;
  }

  LOG(INFO) << "Cancelling sync session " << sync_id;

  // Update replica_ptr state and cancel context.
  replica_ptr->state.store(SyncState::CANCELLED, memory_order_release);
  replica_ptr->cntx.Cancel();

  // Run cleanup for shard threads.
  shard_set->AwaitRunningOnShardQueue([replica_ptr](EngineShard* shard) {
    FlowInfo* flow = &replica_ptr->flows[shard->shard_id()];
    if (flow->cleanup) {
      flow->cleanup();
    }
  });

  // Wait for tasks to finish.
  shard_set->pool()->AwaitFiberOnAll([replica_ptr](unsigned index, auto*) {
    FlowInfo* flow = &replica_ptr->flows[index];

    // Cleanup hasn't been run for io-thread.
    if (EngineShard::tlocal() == nullptr) {
      if (flow->cleanup) {
        flow->cleanup();
      }
    }

    if (flow->full_sync_fb.IsJoinable()) {
      flow->full_sync_fb.Join();
    }
  });

  // Remove ReplicaInfo from global map
  {
    lock_guard lk(mu_);
    replica_infos_.erase(sync_id);
  }

  // Wait for error handler to quit.
  replica_ptr->cntx.JoinErrorHandler();

  LOG(INFO) << "Evicted sync session " << sync_id;
}

shared_ptr<DflyCmd::ReplicaInfo> DflyCmd::GetReplicaInfo(uint32_t sync_id) {
  unique_lock lk(mu_);

  auto it = replica_infos_.find(sync_id);
  if (it != replica_infos_.end())
    return it->second;
  return {};
}

std::vector<DflyCmd::ReplicaRoleInfo> DflyCmd::GetReplicasRoleInfo() {
  std::vector<ReplicaRoleInfo> vec;
  unique_lock lk(mu_);
  for (const auto& info : replica_infos_) {
    vec.emplace_back(info.second->address, info.second->state.load(memory_order_relaxed));
  }
  return vec;
}

pair<uint32_t, shared_ptr<DflyCmd::ReplicaInfo>> DflyCmd::GetReplicaInfoOrReply(
    std::string_view id_str, RedisReplyBuilder* rb) {
  unique_lock lk(mu_);

  uint32_t sync_id;
  if (!ToSyncId(id_str, &sync_id)) {
    rb->SendError(kInvalidSyncId);
    return {0, nullptr};
  }

  auto sync_it = replica_infos_.find(sync_id);
  if (sync_it == replica_infos_.end()) {
    rb->SendError(kIdNotFound);
    return {0, nullptr};
  }

  return {sync_id, sync_it->second};
}

bool DflyCmd::CheckReplicaStateOrReply(const ReplicaInfo& sync_info, SyncState expected,
                                       RedisReplyBuilder* rb) {
  if (sync_info.state != expected) {
    rb->SendError(kInvalidState);
    return false;
  }

  // Check all flows are connected.
  // This might happen if a flow abruptly disconnected before sending the SYNC request.
  for (const FlowInfo& flow : sync_info.flows) {
    if (!flow.conn) {
      rb->SendError(kInvalidState);
      return false;
    }
  }

  return true;
}

void DflyCmd::BreakOnShutdown() {
}

void DflyCmd::Shutdown() {
  ReplicaInfoMap pending;
  {
    std::lock_guard lk(mu_);
    pending = std::move(replica_infos_);
  }

  for (auto [sync_id, replica_ptr] : pending) {
    CancelReplication(sync_id, replica_ptr);
  }
}

void DflyCmd::FlowInfo::TryShutdownSocket() {
  // Close socket for clean disconnect.
  if (conn->socket()->IsOpen()) {
    conn->socket()->Shutdown(SHUT_RDWR);
  }
}

}  // namespace dfly
