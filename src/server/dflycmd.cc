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
#include "server/rdb_save.h"
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
    : listener_(listener), sf_(server_family) {
}

void DflyCmd::Run(CmdArgList args, ConnectionContext* cntx) {
  DCHECK_GE(args.size(), 2u);

  ToUpper(&args[1]);
  RedisReplyBuilder* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());

  string_view sub_cmd = ArgS(args, 1);
  if (sub_cmd == "JOURNAL") {
    if (args.size() < 3) {
      return rb->SendError(WrongNumArgsError("DFLY JOURNAL"));
    }
    HandleJournal(args, cntx);
    return;
  }

  util::ProactorPool* pool = shard_set->pool();
  if (sub_cmd == "THREAD") {
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

  if (sub_cmd == "FLOW" && args.size() == 5) {
    // FLOW <masterid> <syncid> <shardid>

    string_view masterid = ArgS(args, 2);
    string_view syncid_str = ArgS(args, 3);
    string_view shard_id_str = ArgS(args, 4);

    unsigned shard_id, sync_id;
    VLOG(1) << "Got DFLY FLOW " << masterid << " " << syncid_str << " " << shard_id_str;

    if (masterid != sf_->master_id()) {
      return rb->SendError("Bad master id");
    }

    if (!absl::SimpleAtoi(shard_id_str, &shard_id) || shard_id >= shard_set->size()) {
      return rb->SendError(facade::kInvalidIntErr);
    }

    if (!ToSyncId(syncid_str, &sync_id)) {
      return rb->SendError(kInvalidSyncId);
    }

    {
      unique_lock lk(mu_);
      auto it = sync_info_.find(sync_id);
      if (it == sync_info_.end()) {
        return rb->SendError(kIdNotFound);
      }
      auto& entry = it->second.shard_map[shard_id];
      entry.conn = cntx->owner();
    }
    cntx->owner()->SetName(absl::StrCat("repl_flow_", sync_id));

    cntx->conn_state.repl_session_id = sync_id;
    cntx->conn_state.repl_threadid = shard_id;

    // assuming here that shard id and thread id is the same thing.
    listener_->Migrate(cntx->owner(), pool->at(shard_id));
    CHECK(EngineShard::tlocal() && EngineShard::tlocal()->shard_id() == shard_id);

    return rb->SendOk();
  }

  if (sub_cmd == "SYNC" && args.size() == 3) {
    string_view syncid_str = ArgS(args, 2);
    uint32_t syncid;

    if (!ToSyncId(syncid_str, &syncid)) {
      return rb->SendError(kInvalidSyncId);
    }

    cntx->transaction->Schedule();
    OpStatus status = OpStatus::OK;
    boost::fibers::mutex mu;
    cntx->transaction->Execute(
        [&](Transaction* t, EngineShard* shard) {
          OpStatus st = ReplicateInShard(syncid, t, shard);
          lock_guard lk(mu);
          status = st;
          return OpStatus::OK;
        },
        true);

    if (status == OpStatus::OK)
      return rb->SendOk();
    return rb->SendError("replication error");
  }
  rb->SendError(kSyntaxErr);
}

uint32_t DflyCmd::AllocateSyncSession() {
  unique_lock lk(mu_);
  auto [it, inserted] = sync_info_.emplace(next_sync_id_, SyncInfo{});
  CHECK(inserted);
  ++next_sync_id_;
  return it->first;
}

void DflyCmd::OnClose(ConnectionContext* cntx) {
  boost::fibers::fiber repl_fb;

  if (cntx->conn_state.repl_session_id > 0 && cntx->conn_state.repl_threadid != kuint32max) {
    unique_lock lk(mu_);

    auto it = sync_info_.find(cntx->conn_state.repl_session_id);
    if (it != sync_info_.end()) {
      VLOG(1) << "Found tbd: " << cntx->conn_state.repl_session_id;
    }
  }

  if (repl_fb.joinable()) {
    repl_fb.join();
  }
}

void DflyCmd::HandleJournal(CmdArgList args, ConnectionContext* cntx) {
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

OpStatus DflyCmd::ReplicateInShard(uint32_t syncid, Transaction* t, EngineShard* shard) {
  unique_lock lk(mu_);
  auto it = sync_info_.find(syncid);
  if (it == sync_info_.end()) {
    return OpStatus::KEY_NOTFOUND;
  }

  auto shard_it = it->second.shard_map.find(shard->shard_id());
  if (shard_it == it->second.shard_map.end()) {
    return OpStatus::KEY_NOTFOUND;
  }
  CHECK(!shard_it->second.repl_fb.joinable());
  Connection* conn = shard_it->second.conn;
  shard_it->second.repl_fb = boost::fibers::fiber(&DflyCmd::SnapshotFb, this, conn, shard);
  lk.unlock();

  return OpStatus::OK;
}

void DflyCmd::SnapshotFb(Connection* conn, EngineShard* shard) {
  RdbSaver saver(conn->socket(), true, false);

  // TODO: right now it's a poc code just for sake of sending something and show it can be done.
  // we will need to think how we send rdb header etc.
  error_code ec = saver.SaveHeader({});

  // also we need to think how to manage errors when replicating.
  if (ec) {
    LOG(ERROR) << ec;
    return;
  }

  saver.StartSnapshotInShard(true, shard);
  ec = saver.SaveBody(nullptr);
  if (ec) {
    LOG(ERROR) << ec;
    return;
  }

  LOG(INFO) << "shard sync finished";
  // Again, to remove....
  conn->socket()->Shutdown(SHUT_RDWR);
}

void DflyCmd::BreakOnShutdown() {
  VLOG(1) << "BreakOnShutdown";
}

}  // namespace dfly
