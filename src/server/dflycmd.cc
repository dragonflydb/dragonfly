// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "server/dflycmd.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/strip.h>

#include "base/flags.h"
#include "base/logging.h"
#include "facade/dragonfly_connection.h"
#include "server/engine_shard_set.h"
#include "server/error.h"
#include "server/journal/journal.h"
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
const char kIdNotFound[] = "syncid not found";
const char kInvalidSyncId[] = "bad sync id";

bool ToSyncId(string_view str, uint32_t* num) {
  if (!absl::StartsWith(str, "SYNC"))
    return false;
  str.remove_prefix(4);

  return absl::SimpleAtoi(str, num);
}
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

  if (sub_cmd == "SYNC" && args.size() == 5) {
    return Sync(args, cntx);
  }

  if (sub_cmd == "EXPIRE") {
    return Expire(args, cntx);
  }

  rb->SendError(kSyntaxErr);
}

void DflyCmd::OnClose(ConnectionContext* cntx) {
  if (cntx->conn_state.repl_session_id > 0 && cntx->conn_state.repl_threadid != kuint32max) {
    DeleteSyncSession(cntx->conn_state.repl_session_id);
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

  rb->SendError(kInvalidIntErr);
  return;
}

void DflyCmd::Sync(CmdArgList args, ConnectionContext* cntx) {
  RedisReplyBuilder* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  string_view masterid = ArgS(args, 2);
  string_view sync_id_str = ArgS(args, 3);
  string_view flow_id_str = ArgS(args, 4);

  unsigned flow_id;
  VLOG(1) << "Got DFLY SYNC " << masterid << " " << sync_id_str << " " << flow_id_str;

  if (masterid != sf_->master_id()) {
    return rb->SendError("Bad master id");
  }

  if (!absl::SimpleAtoi(flow_id_str, &flow_id) || !absl::StartsWith(sync_id_str, "SYNC")) {
    return rb->SendError(kSyntaxErr);
  }

  auto [sync_id, sync_info] = GetSyncInfoOrReply(sync_id_str, rb);
  if (!sync_id)
    return;

  // assuming here that shard id and thread id is the same thing.
  if (int(flow_id) != ProactorBase::GetIndex()) {
    listener_->Migrate(cntx->owner(), shard_set->pool()->at(flow_id));
  }

  (void)sync_id;
  (void)sync_info;

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

uint32_t DflyCmd::CreateSyncSession() {
  unique_lock lk(mu_);

  auto [it, inserted] = sync_infos_.emplace(next_sync_id_, new SyncInfo);
  CHECK(inserted);

  return next_sync_id_++;
}

void DflyCmd::DeleteSyncSession(uint32_t sync_id) {
  unique_lock lk(mu_);

  auto it = sync_infos_.find(sync_id);
  if (it == sync_infos_.end())
    return;

  delete it->second;
  sync_infos_.erase(it);
}

pair<uint32_t, DflyCmd::SyncInfo*> DflyCmd::GetSyncInfoOrReply(std::string_view id_str,
                                                               RedisReplyBuilder* rb) {
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
