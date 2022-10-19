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

  if (sub_cmd == "SYNC") {
    // SYNC <masterid> <syncid> <shardid>

    if (args.size() == 5) {
      string_view masterid = ArgS(args, 2);
      string_view syncid_str = ArgS(args, 3);
      string_view shard_id_str = ArgS(args, 4);

      unsigned shard_id, sync_id;
      VLOG(1) << "Got DFLY SYNC " << masterid << " " << syncid_str << " " << shard_id_str;

      if (masterid != sf_->master_id()) {
        return rb->SendError("Bad master id");
      }

      if (!absl::SimpleAtoi(shard_id_str, &shard_id) || !absl::StartsWith(syncid_str, "SYNC")) {
        return rb->SendError(kSyntaxErr);
      }

      syncid_str.remove_prefix(4);
      if (!absl::SimpleAtoi(syncid_str, &sync_id) || shard_id >= shard_set->size()) {
        return rb->SendError("Bad id");
      }

      auto it = sync_info_.find(sync_id);
      if (it == sync_info_.end()) {
        return rb->SendError("syncid not found");
      }

      // assuming here that shard id and thread id is the same thing.
      if (int(shard_id) != ProactorBase::GetIndex()) {
        listener_->Migrate(cntx->owner(), pool->at(shard_id));
      }
      return rb->SendOk();
    }
  }

  if (sub_cmd == "EXPIRE") {
    cntx->transaction->ScheduleSingleHop([](Transaction* t, EngineShard* shard){
      shard->db_slice().ExpireAllIfNeeded();
      return OpStatus::OK;
    });

    return rb->SendOk();
  }

  rb->SendError(kSyntaxErr);
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

uint32_t DflyCmd::AllocateSyncSession() {
  unique_lock lk(mu_);
  auto [it, inserted] = sync_info_.emplace(next_sync_id_, SyncInfo{});
  CHECK(inserted);
  ++next_sync_id_;
  return it->first;
}

void DflyCmd::BreakOnShutdown() {
  VLOG(1) << "BreakOnShutdown";
}

}  // namespace dfly
