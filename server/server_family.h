// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "server/engine_shard_set.h"
#include "server/global_state.h"
#include "util/proactor_pool.h"

namespace util {
class AcceptServer;
}  // namespace util

namespace dfly {

class ConnectionContext;
class CommandRegistry;
class Service;
class Replica;
class ScriptMgr;

struct Metrics {
  DbStats db;
  SliceEvents events;

  ConnectionStats conn_stats;
};

class ServerFamily {
 public:
  ServerFamily(Service* engine);
  ~ServerFamily();

  void Init(util::AcceptServer* acceptor);
  void Register(CommandRegistry* registry);
  void Shutdown();

  Metrics GetMetrics() const;

  GlobalState* global_state() {
    return &global_state_;
  }

  ScriptMgr* script_mgr() {
    return script_mgr_.get();
  }

 private:
  uint32_t shard_count() const {
    return ess_.size();
  }

  void Debug(CmdArgList args, ConnectionContext* cntx);
  void DbSize(CmdArgList args, ConnectionContext* cntx);
  void FlushDb(CmdArgList args, ConnectionContext* cntx);
  void FlushAll(CmdArgList args, ConnectionContext* cntx);
  void Info(CmdArgList args, ConnectionContext* cntx);
  void LastSave(CmdArgList args, ConnectionContext* cntx);
  void Psync(CmdArgList args, ConnectionContext* cntx);
  void ReplicaOf(CmdArgList args, ConnectionContext* cntx);
  void Save(CmdArgList args, ConnectionContext* cntx);
  void Script(CmdArgList args, ConnectionContext* cntx);
  void Sync(CmdArgList args, ConnectionContext* cntx);

  void _Shutdown(CmdArgList args, ConnectionContext* cntx);

  void SyncGeneric(std::string_view repl_master_id, uint64_t offs, ConnectionContext* cntx);

  Service& engine_;
  util::ProactorPool& pp_;
  EngineShardSet& ess_;

  util::AcceptServer* acceptor_ = nullptr;
  ::boost::fibers::mutex replica_of_mu_;
  std::shared_ptr<Replica> replica_;  // protected by replica_of_mu_

  std::unique_ptr<ScriptMgr> script_mgr_;

  std::atomic<int64_t> last_save_;  // in seconds.
  GlobalState global_state_;
};

}  // namespace dfly
