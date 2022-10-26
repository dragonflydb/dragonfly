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

  if (sub_cmd == "EXPIRE") {
    return Expire(args, cntx);
  }

  rb->SendError(kSyntaxErr);
}

void DflyCmd::OnClose(ConnectionContext* cntx) {
  unsigned session_id = cntx->conn_state.repl_session_id;
  unsigned flow_id = cntx->conn_state.repl_flow_id;

  if (!session_id)
    return;

  if (flow_id == kuint32max) {
    DeleteSyncSession(session_id);
  } else {
    shared_ptr<SyncInfo> sync_info = GetSyncInfo(session_id);
    if (sync_info) {
      lock_guard lk(sync_info->mu);
      if (sync_info->state != SyncState::CANCELLED) {
        UnregisterFlow(&sync_info->flows[flow_id]);
      }
    }
  }
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
  if (sync_info->state != SyncState::PREPARATION)
    return rb->SendError(kInvalidState);

  // Check all flows are connected.
  for (const FlowInfo& flow : sync_info->flows) {
    if (!flow.conn) {
      return rb->SendError(kInvalidState);
    }
  }

  // Start full sync.
  {
    TransactionGuard tg{cntx->transaction};
    AggregateStatus status;

    auto cb = [this, &status, sync_info = sync_info](unsigned index, auto*) {
      status = StartFullSyncInThread(&sync_info->flows[index], EngineShard::tlocal());
    };
    shard_set->pool()->AwaitFiberOnAll(std::move(cb));

    // TODO: Send better error
    if (*status != OpStatus::OK)
      return rb->SendError(kInvalidState);
  }

  sync_info->state = SyncState::FULL_SYNC;
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

OpStatus DflyCmd::StartFullSyncInThread(FlowInfo* flow, EngineShard* shard) {
  DCHECK(!flow->fb.joinable());

  SaveMode save_mode = shard == nullptr ? SaveMode::SUMMARY : SaveMode::SINGLE_SHARD;
  flow->saver.reset(new RdbSaver(flow->conn->socket(), save_mode, false));

  if (shard != nullptr) {
    flow->saver->StartSnapshotInShard(false, shard);
  }

  flow->fb = ::boost::fibers::fiber(&DflyCmd::FullSyncFb, this, flow);
  return OpStatus::OK;
}

void DflyCmd::FullSyncFb(FlowInfo* flow) {
  error_code ec;
  RdbSaver* saver = flow->saver.get();

  if (saver->Mode() == SaveMode::SUMMARY) {
    auto scripts = sf_->script_mgr()->GetLuaScripts();
    saver->SaveHeader(scripts);
  } else {
    saver->SaveHeader({});
  }

  if (ec) {
    LOG(ERROR) << ec;
    return;
  }

  if (saver->Mode() != SaveMode::SUMMARY) {
    // TODO: we should be able to stop earlier if requested.
    ec = saver->SaveBody(nullptr);
    if (ec) {
      LOG(ERROR) << ec;
      return;
    }
  }

  ec = flow->conn->socket()->Write(io::Buffer(flow->eof_token));
  if (ec) {
    LOG(ERROR) << ec;
    return;
  }

  ec = flow->conn->socket()->Shutdown(SHUT_RDWR);
}

uint32_t DflyCmd::CreateSyncSession() {
  unique_lock lk(mu_);

  auto sync_info = make_shared<SyncInfo>();
  sync_info->flows.resize(shard_set->size() + 1);

  auto [it, inserted] = sync_infos_.emplace(next_sync_id_, std::move(sync_info));
  CHECK(inserted);

  return next_sync_id_++;
}

void DflyCmd::UnregisterFlow(FlowInfo* flow) {
  flow->conn = nullptr;
  flow->saver.reset();
}

void DflyCmd::DeleteSyncSession(uint32_t sync_id) {
  shared_ptr<SyncInfo> sync_info;

  // Remove sync_info from map.
  // Store by value to keep alive.
  {
    unique_lock lk(mu_);

    auto it = sync_infos_.find(sync_id);
    if (it == sync_infos_.end())
      return;

    sync_info = it->second;
    sync_infos_.erase(it);
  }

  // Wait for all operations to finish.
  // Set state to CANCELLED so no other operations will run.
  {
    unique_lock lk(sync_info->mu);
    sync_info->state = SyncState::CANCELLED;
  }

  // Try to cleanup flows.
  for (auto& flow : sync_info->flows) {
    if (flow.conn != nullptr) {
      VLOG(1) << "Flow connection " << flow.conn->GetName() << " is still alive"
              << " on sync_id " << sync_id;
    }
    // TODO: Implement cancellation.
    if (flow.fb.joinable()) {
      VLOG(1) << "Force joining fiber on on sync_id " << sync_id;
      flow.fb.join();
    }
  }
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

void DflyCmd::BreakOnShutdown() {
  VLOG(1) << "BreakOnShutdown";
}

}  // namespace dfly
