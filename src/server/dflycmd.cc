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

  auto [sync_id, sync_info] = GetSyncInfoOrReply(sync_id_str, rb);
  if (!sync_id)
    return;

  unique_lock lk(sync_info->mu);
  if (sync_info->state != SyncState::PREPARATION)
    return rb->SendError(kInvalidState);

  // Set meta info on connection.
  cntx->owner()->SetName(absl::StrCat("repl_flow_", sync_id));
  cntx->conn_state.repl_session_id = sync_id;
  cntx->conn_state.repl_flow_id = flow_id;

  absl::InsecureBitGen gen;
  string eof_token = GetRandomHex(gen, 40);

  sync_info->flows[flow_id] = FlowInfo{cntx->owner(), eof_token};
  listener_->Migrate(cntx->owner(), shard_set->pool()->at(flow_id));

  rb->StartArray(2);
  rb->SendSimpleString("FULL");
  rb->SendSimpleString(eof_token);
}

void DflyCmd::Sync(CmdArgList args, ConnectionContext* cntx) {
  RedisReplyBuilder* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  string_view sync_id_str = ArgS(args, 2);

  VLOG(1) << "Got DFLY SYNC " << sync_id_str;

  auto [sync_id, sync_info] = GetSyncInfoOrReply(sync_id_str, rb);
  if (!sync_id)
    return;

  unique_lock lk(sync_info->mu);
  if (!CheckReplicaStateOrReply(*sync_info, SyncState::PREPARATION, rb))
    return;

  // Start full sync.
  {
    TransactionGuard tg{cntx->transaction};
    AggregateStatus status;

    auto cb = [this, &status, sync_info = sync_info](unsigned index, auto*) {
      status =
          StartFullSyncInThread(&sync_info->flows[index], &sync_info->cntx, EngineShard::tlocal());
    };
    shard_set->pool()->AwaitFiberOnAll(std::move(cb));

    // TODO: Send better error
    if (*status != OpStatus::OK)
      return rb->SendError(kInvalidState);
  }

  sync_info->state = SyncState::FULL_SYNC;
  return rb->SendOk();
}

void DflyCmd::StartStable(CmdArgList args, ConnectionContext* cntx) {
  RedisReplyBuilder* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  string_view sync_id_str = ArgS(args, 2);

  VLOG(1) << "Got DFLY STARTSTABLE " << sync_id_str;

  auto [sync_id, sync_info] = GetSyncInfoOrReply(sync_id_str, rb);
  if (!sync_id)
    return;

  unique_lock lk(sync_info->mu);
  if (!CheckReplicaStateOrReply(*sync_info, SyncState::FULL_SYNC, rb))
    return;

  {
    TransactionGuard tg{cntx->transaction};
    AggregateStatus status;

    auto cb = [this, &status, sync_info = sync_info](unsigned index, auto*) {
      EngineShard* shard = EngineShard::tlocal();
      FlowInfo* flow = &sync_info->flows[index];

      StopFullSyncInThread(flow, shard);
      status = StartStableSyncInThread(flow, shard);
      return OpStatus::OK;
    };
    shard_set->pool()->AwaitFiberOnAll(std::move(cb));

    if (*status != OpStatus::OK)
      return rb->SendError(kInvalidState);
  }

  sync_info->state = SyncState::STABLE_SYNC;
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
  DCHECK(!flow->task_fb.joinable());

  SaveMode save_mode = shard == nullptr ? SaveMode::SUMMARY : SaveMode::SINGLE_SHARD;
  flow->saver.reset(new RdbSaver(flow->conn->socket(), save_mode, false));

  flow->cleanup = [saver = flow->saver.get()]() { saver->Cancel(); };

  // Shard can be null for io thread.
  if (shard != nullptr) {
    CHECK(!sf_->journal()->OpenInThread(false, ""sv));  // can only happen in persistent mode.
    flow->saver->StartSnapshotInShard(true, *cntx, shard);
  }

  flow->task_fb = ::boost::fibers::fiber(&DflyCmd::FullSyncFb, this, flow, cntx);
  return OpStatus::OK;
}

void DflyCmd::StopFullSyncInThread(FlowInfo* flow, EngineShard* shard) {
  // Shard can be null for io thread.
  if (shard != nullptr) {
    flow->saver->StopSnapshotInShard(shard);
  }

  // Wait for full sync to finish.
  if (flow->task_fb.joinable()) {
    flow->task_fb.join();
  }

  // Reset cleanup and saver
  flow->cleanup = []() {};
  flow->saver.reset();
}

OpStatus DflyCmd::StartStableSyncInThread(FlowInfo* flow, EngineShard* shard) {
  // Register journal listener and cleanup.
  if (shard != nullptr) {
    uint32_t cb_id = sf_->journal()->RegisterOnChange([flow](const journal::Entry& je) {
      // TODO: Serialize event.
      ReqSerializer serializer{flow->conn->socket()};
      serializer.SendCommand(absl::StrCat("SET ", je.key, " ", je.pval_ptr->ToString()));
    });

    flow->cleanup = [this, cb_id]() { sf_->journal()->Unregister(cb_id); };
  }

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
    return cntx->Error(ec);
  }

  if ((ec = saver->SaveBody(*cntx, nullptr))) {
    return cntx->Error(ec);
  }

  ec = flow->conn->socket()->Write(io::Buffer(flow->eof_token));
  if (ec) {
    return cntx->Error(ec);
  }
}

uint32_t DflyCmd::CreateSyncSession() {
  unique_lock lk(mu_);
  unsigned sync_id = next_sync_id_++;

  unsigned flow_count = shard_set->size() + 1;
  auto err_handler = [this, sync_id](const GenericError& err) {
    LOG(INFO) << "Running error handler for sync session " << sync_id;

    // Stop replication in case of error.
    // StopReplication needs to run async to prevent blocking
    // the error handler.
    ::boost::fibers::fiber{&DflyCmd::StopReplication, this, sync_id}.detach();

    return true;  // Cancel context
  };

  auto sync_info = make_shared<SyncInfo>(flow_count, std::move(err_handler));
  auto [it, inserted] = sync_infos_.emplace(sync_id, std::move(sync_info));
  CHECK(inserted);

  return sync_id;
}

void DflyCmd::OnClose(ConnectionContext* cntx) {
  unsigned session_id = cntx->conn_state.repl_session_id;
  if (!session_id)
    return;

  auto sync_info = GetSyncInfo(session_id);
  if (!sync_info)
    return;

  // Because cancellation holds the mutex,
  // this lock waits for it to finish before releasing the underlying connection
  lock_guard lk(sync_info->mu);
  if (sync_info->state != SyncState::CANCELLED) {
    // Nobody started cancellation yet - lets do it
    CancelSyncSession(session_id, sync_info);
  }
}

void DflyCmd::StopReplication(uint32_t sync_id) {
  auto ptr = GetSyncInfo(sync_id);
  if (!ptr)
    return;

  VLOG(0) << "<<<<<<<<<<<<<<<<<<< STOP IN";
  if (!ptr->cancel_flag.load()) {
    lock_guard lk(ptr->mu);
    CancelSyncSession(sync_id, ptr);
  }
  VLOG(0) << ">>>>>>>>>>>>>>>>>>>> STOP OUT";
}

void DflyCmd::CancelSyncSession(uint32_t sync_id, shared_ptr<SyncInfo> sync_info) {
  // Update sync_info state, cancel context and cancel flag.
  if (sync_info->state == SyncState::CANCELLED) {
    return;
  }

  LOG(INFO) << "Cancelling sync session " << sync_id;

  sync_info->state = SyncState::CANCELLED;
  sync_info->cancel_flag.store(true);
  sync_info->cntx.Cancel();

  // Run cleanup and await tasks.
  shard_set->pool()->AwaitFiberOnAll([sync_id, sync_info](unsigned index, auto*) {
    FlowInfo* flow = &sync_info->flows[index];
    if (flow->cleanup)
      flow->cleanup();
  });

  for (FlowInfo& flow : sync_info->flows) {
    if (flow.task_fb.joinable()) {
      flow.task_fb.join();
    }
  }

  // Unlock on-hold and remove from map.
  {
    lock_guard lk(mu_);
    sync_infos_.erase(sync_id);
  }

  LOG(INFO) << "Evicted sync session " << sync_id;
}

shared_ptr<DflyCmd::SyncInfo> DflyCmd::GetSyncInfo(uint32_t sync_id) {
  unique_lock lk(mu_);

  auto it = sync_infos_.find(sync_id);
  if (it != sync_infos_.end())
    return it->second;
  return {};
}

pair<uint32_t, shared_ptr<DflyCmd::SyncInfo>> DflyCmd::GetSyncInfoOrReply(std::string_view id_str,
                                                                          RedisReplyBuilder* rb) {
  unique_lock lk(mu_);

  uint32_t sync_id;
  if (!ToSyncId(id_str, &sync_id)) {
    rb->SendError(kInvalidSyncId);
    return {0, nullptr};
  }

  auto sync_it = sync_infos_.find(sync_id);
  if (sync_it == sync_infos_.end()) {
    rb->SendError(kIdNotFound);
    return {0, nullptr};
  }

  return {sync_id, sync_it->second};
}

bool DflyCmd::CheckReplicaStateOrReply(const SyncInfo& sync_info, SyncState expected,
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
  VLOG(1) << "BreakOnShutdown";
}

}  // namespace dfly
