// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "server/dflycmd.h"

#include <absl/cleanup/cleanup.h>
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

  // Run command with args.
  using CmdType = void (DflyCmd::*)(CmdArgList, ConnectionContext * cntx);
  auto run_with_args = [this, args, cntx, rb, sub_cmd](CmdType cmd, unsigned expected_args) {
    if (args.size() != expected_args) {
      return rb->SendError(WrongNumArgsError(absl::StrCat("DFLY ", sub_cmd)));
    }
    (this->*cmd)(args, cntx);
  };

  if (sub_cmd == "JOURNAL") {
    return run_with_args(&DflyCmd::HandleJournal, 3);
  }

  if (sub_cmd == "THREAD") {
    return Thread(args, cntx);
  }

  if (sub_cmd == "FLOW") {
    return run_with_args(&DflyCmd::Flow, 5);
  }

  if (sub_cmd == "SYNC") {
    return run_with_args(&DflyCmd::Sync, 3);
  }

  if (sub_cmd == "SWITCH") {
    return run_with_args(&DflyCmd::Switch, 3);
  }

  rb->SendError(kSyntaxErr);
}

void DflyCmd::Thread(CmdArgList args, ConnectionContext* cntx) {
  RedisReplyBuilder* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  if (args.size() == 2) {  // DFLY THREAD : returns connection thread index and number of threads.
    rb->StartArray(2);
    rb->SendLong(ProactorBase::GetIndex());
    rb->SendLong(long(shard_set->pool()->size()));
    return;
  }

  // DFLY THREAD to_thread : migrates current connection to a different thread.
  string_view arg = ArgS(args, 2);
  unsigned num_thread;
  if (!absl::SimpleAtoi(arg, &num_thread)) {
    return rb->SendError(kSyntaxErr);
  }

  if (num_thread < shard_set->pool()->size()) {
    if (int(num_thread) != ProactorBase::GetIndex()) {
      listener_->Migrate(cntx->owner(), shard_set->pool()->at(num_thread));
    }

    return rb->SendOk();
  }

  rb->SendError(kInvalidIntErr);
  return;
}

void DflyCmd::Flow(CmdArgList args, ConnectionContext* cntx) {
  RedisReplyBuilder* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());

  string_view masterid = ArgS(args, 2);
  string_view syncid_str = ArgS(args, 3);
  string_view threadid_str = ArgS(args, 4);

  unsigned threadid;
  VLOG(0) << "Got DFLY FLOW " << masterid << " " << syncid_str << " " << threadid_str;

  if (masterid != sf_->master_id()) {
    return rb->SendError("Bad master id");
  }

  if (!absl::SimpleAtoi(threadid_str, &threadid) || threadid >= shard_set->pool()->size()) {
    return rb->SendError(facade::kInvalidIntErr);
  }

  auto sync_data = GetSyncInfoOrRespond(syncid_str, rb);
  if (!sync_data)
    return;
  auto [sync_id, sync_info] = *sync_data;

  absl::InsecureBitGen gen;
  string eof_token = GetRandomHex(gen, 40);
  {
    unique_lock lk(mu_);
    auto& entry = sync_info->thread_map[threadid];
    entry.conn = cntx->owner();
    entry.eof_token = eof_token;
  }
  cntx->owner()->SetName(absl::StrCat("repl_flow_", sync_id));

  cntx->conn_state.repl_session_id = sync_id;
  cntx->conn_state.repl_threadid = threadid;

  // assuming here that shard id and thread id is the same thing.
  listener_->Migrate(cntx->owner(), shard_set->pool()->at(threadid));

  // response is an array
  rb->StartArray(2);
  rb->SendSimpleString("FULL");
  rb->SendSimpleString(eof_token);
}

void DflyCmd::Sync(CmdArgList args, ConnectionContext* cntx) {
  RedisReplyBuilder* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());

  auto sync_data = GetSyncInfoOrRespond(ArgS(args, 2), rb);
  if (!sync_data)
    return;
  auto [sync_id, sync_info] = *sync_data;

  VLOG(0) << "Starting full sync on " << sync_id;
  {
    std::unique_lock lk(mu_);

    if (sync_info->state != ReplicaState::PREPARATION) {
      return rb->SendError("?");
    }

    // Initialize barrier. -1 for io_thread, +1 for last kick.
    unsigned flows = sync_info->thread_map.size();
    sync_info->snapshot_block.emplace(flows, &sync_info->fiber_flag_counter);
    sync_info->fiber_flag_counter.store(flows, std::memory_order_relaxed);

    // TODO: Who cares about errors possible errors?
    // State switch has to happen atomically (mutex POV). Add separate busy/transitioning flag?
    sync_info->state = ReplicaState::FULL_SYNC;
  }

  // Before setting up a transaction that initiates a full sync,
  // we kick-off non-shard replication that must start earlier than shards.
  // TODO: do in parallel with full sync fibers when transmitting header data.
  shard_set->pool()->AwaitFiberOnAll([this, sync_id = sync_id](unsigned index, auto*) {
    auto* shard = EngineShard::tlocal();
    if (!shard) {
      StartReplInThread(index, sync_id);
    }
  });

  cntx->transaction->Schedule();

  AggregateStatus status;
  // kick off the snapshotting simultaneously in all shards.
  // we do it via data sockets but reply here
  // via control socket that orchestrates the flow from the replica side.
  cntx->transaction->Execute(
      [this, &status, sync_id = sync_id](Transaction* t, EngineShard* shard) {
        status = StartFullSync(sync_id, t, shard);
        return OpStatus::OK;
      },
      true);

  // TODO: after the full sync we should continue streaming data from non-shard threads as well
  // because those threads provide locking/transaction information needed to provide atomicity
  // guarantees on replica.

  if (*status == OpStatus::OK)
    return rb->SendOk();

  // TODO: to stop the replication in shards that started doing this.
  // We can do it without the transaction via shard_set.

  return rb->SendError("replication error");
}

void DflyCmd::Switch(CmdArgList args, ConnectionContext* cntx) {
  RedisReplyBuilder* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());

  auto sync_data = GetSyncInfoOrRespond(ArgS(args, 2), rb);
  if (!sync_data)
    return;
  auto [sync_id, sync_info] = *sync_data;

  std::unique_lock lk(mu_);

  // Check replica is in correct state and all flows finished sending static data.
  if (sync_info->state != ReplicaState::FULL_SYNC ||
      sync_info->fiber_flag_counter.load(std::memory_order_relaxed) != 1) {
    VLOG(0) << "Wrong state!! " << sync_info->fiber_flag_counter.load(std::memory_order_relaxed);
    return rb->SendError("?");
  }

  VLOG(0) << "Switching to stable state on " << sync_id;

  {
    // Guard scope with two global cuts.
    Transaction* t = cntx->transaction;
    auto kEmptyOp = [](Transaction* t, EngineShard* shard) { return OpStatus::OK; };
    t->Schedule();
    t->Execute(kEmptyOp, false);
    absl::Cleanup transaction_cut = [kEmptyOp, t]() { t->Execute(kEmptyOp, true); };

    // Overflow barrier and unblock full sync fibers.
    sync_info->snapshot_block->wait();

    // Join full sync fibers and start stable state fibers.
    shard_set->pool()->DispatchOnAll(
        [this, sync_info = sync_info](unsigned flow_id, ProactorBase*) {
          StartStableSync(flow_id, &sync_info->thread_map[flow_id]);
        });

    sync_info->state = ReplicaState::STABLE_SYNC;
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

DflyCmd::SyncId DflyCmd::AllocateSyncSession() {
  unique_lock lk(mu_);
  auto [it, inserted] = sync_info_.emplace(next_sync_id_, new SyncInfo);
  CHECK(inserted);

  it->second->start_time_ns = ProactorBase::me()->GetMonotonicTimeNs();

  return next_sync_id_++;
}

void DflyCmd::OnClose(ConnectionContext* cntx) {
  VLOG(0) << "Closing context " << cntx->conn_state.repl_threadid;
  boost::fibers::fiber repl_fb;

  if (cntx->conn_state.repl_session_id > 0 && cntx->conn_state.repl_threadid != kuint32max) {
    unique_lock lk(mu_);

    auto it = sync_info_.find(cntx->conn_state.repl_session_id);
    if (it != sync_info_.end()) {
      SyncInfo* si = it->second;
      auto& thread_map = si->thread_map;
      auto shard_it = thread_map.find(cntx->conn_state.repl_threadid);
      if (shard_it != thread_map.end() && shard_it->second.conn == cntx->owner()) {
        repl_fb.swap(shard_it->second.repl_fb);
        thread_map.erase(shard_it);

        if (thread_map.empty()) {
          int64_t dur_ms = (ProactorBase::me()->GetMonotonicTimeNs() - si->start_time_ns) / 1000000;

          LOG(INFO) << "session " << it->first << " closed after " << dur_ms << "ms";
          delete si;
          sync_info_.erase(it);
        }
      }
    }
  }

  if (repl_fb.joinable()) {
    repl_fb.join();
  }
}

void DflyCmd::StartReplInThread(uint32_t thread_id, SyncId syncid) {
  VLOG(0) << "Start REPL in thread " << thread_id;

  // we can not check sync_info_ state in coordinator thread because by the time
  // this function runs things can change.
  unique_lock lk(mu_);
  auto it = sync_info_.find(syncid);
  if (it == sync_info_.end()) {
    return;
  }

  auto shard_it = it->second->thread_map.find(thread_id);
  if (shard_it == it->second->thread_map.end()) {
    return;
  }

  CHECK(!shard_it->second.repl_fb.joinable());
  Connection* conn = shard_it->second.conn;
  lk.unlock();

  // TODO: We do not support any replication yet.

  // Send RDB FS_CUT + RDB EOF + EOF token
  // TODO: Replace with header serialization
  RdbSaver::CloseImmediately(conn->socket());

  // dont block in this fiber.
  // it->second->snapshot_block->barrier_.wait();

  conn->socket()->Write(io::Buffer(shard_it->second.eof_token));
}

OpStatus DflyCmd::StartFullSync(SyncId syncid, Transaction* t, EngineShard* shard) {
  // we can not check sync_info_ state in coordinator thread because by the time
  // this function runs things can change.
  unique_lock lk(mu_);
  auto it = sync_info_.find(syncid);
  if (it == sync_info_.end()) {
    return OpStatus::KEY_NOTFOUND;
  }

  // I assume here that shard_id is thread id because we map threads 0..K to shards,
  // threads are 0..K..N, where K<=N.
  auto shard_it = it->second->thread_map.find(shard->shard_id());
  if (shard_it == it->second->thread_map.end()) {
    return OpStatus::KEY_NOTFOUND;
  }

  CHECK(!shard_it->second.repl_fb.joinable());
  Connection* conn = shard_it->second.conn;
  lk.unlock();

  unique_ptr<RdbSaver> saver = make_unique<RdbSaver>(conn->socket(), SaveMode::SINGLE_SHARD,
                                                     false /* do not align writes */);

  // Enable in-memory journaling. Please note that we need further enable it on all threads.
  auto ec = sf_->journal()->OpenInThread(false, string_view());
  CHECK(!ec);

  // We must initialize snapshotting in this function because it provides us
  // the snapshot isolation required for point-in-time full sync.
  // StartSnapshotInShard assigns the epoch to the snapshot to preserve the snapshot isolation.
  saver->StartSnapshotInShard(&*it->second->snapshot_block, shard);

  shard_it->second.repl_fb = boost::fibers::fiber(
      &DflyCmd::FullSyncFb, this, shard_it->second.eof_token, it->second, conn, saver.release());

  return OpStatus::OK;
}

void DflyCmd::FullSyncFb(string eof_token, SyncInfo* si, Connection* conn, RdbSaver* saver) {
  unique_ptr<RdbSaver> guard(saver);
  error_code ec;

  // TODO: right now it's a poc code just for sake of sending something and show it can be done.
  // we will need to think how we send rdb header etc.
  ec = saver->SaveHeader({});
  // also we need to think how to manage errors when replicating.
  if (ec) {
    LOG(ERROR) << ec;
    return;
  }

  // TODO: we should be able to stop earlier if requested.
  ec = saver->SaveBody(nullptr);
  if (ec) {
    LOG(ERROR) << ec;
    return;
  }

  VLOG(1) << "SaveBody sync finished";

  ec = conn->socket()->Write(io::Buffer(eof_token));
  if (ec) {
    LOG(ERROR) << ec;
    return;
  }

  // buggy code of course - when errors happen we won't decreases this counter,
  // but it's only for poc.
  if (si->fiber_flag_counter.fetch_sub(1, memory_order_acq_rel) == 1) {
    int64_t dur_ms = (ProactorBase::me()->GetMonotonicTimeNs() - si->start_time_ns) / 1000000;
    LOG(INFO) << "Finished full sync after " << dur_ms << "ms";
  }

  // Again, to remove....
  // Instead - to pull from a journal log incremental diffs and send them on the wire.
  // should be a loop picking up and writing it.

  // CHANGED: Dont close as stable state follows
  // ec = conn->socket()->Shutdown(SHUT_RDWR);
}

OpStatus DflyCmd::StartStableSync(unsigned flow_id, ReplicateFlow* flow) {
  // Wait for full sync to finish.
  if (flow->repl_fb.joinable()) {
    flow->repl_fb.join();
  } else {
    // TODO: store non shard repl fiber as well.
  }

  // Start stable state fiber.
  flow->repl_fb = boost::fibers::fiber(&DflyCmd::StableSyncFb, this, flow_id, flow);

  return OpStatus::OK;
}

void DflyCmd::StableSyncFb(unsigned flow_id, ReplicateFlow* flow) {
  // sf_->journal()->OpenInThread(false, "");
  auto cb = sf_->journal()->RegisterOnChange([flow, flow_id](const journal::Entry& je) {
    // TODO: Serialize event.
    VLOG(0) << "Got change event " << je.key << " at " << flow_id;
    ReqSerializer serializer{flow->conn->socket()};
    serializer.SendCommand(absl::StrCat("SET ", je.key, " ", je.pval_ptr->ToString()));
    CHECK(!serializer.ec());
  });

  // TODO: block on barrier again? Cancellation?
  ::boost::this_fiber::sleep_for(1000s);
}

std::optional<std::pair<DflyCmd::SyncId, DflyCmd::SyncInfo*>> DflyCmd::GetSyncInfoOrRespond(
    std::string_view id_str, RedisReplyBuilder* rb) {
  SyncId sync_id;

  if (!ToSyncId(id_str, &sync_id)) {
    rb->SendError(kInvalidSyncId);
    return {};
  }

  auto sync_it = sync_info_.find(sync_id);
  if (sync_it == sync_info_.end()) {
    rb->SendError(kIdNotFound);
    return {};
  }

  return {{sync_id, sync_it->second}};
}

void DflyCmd::BreakOnShutdown() {
  VLOG(1) << "BreakOnShutdown";
}

}  // namespace dfly
