// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "facade/conn_context.h"
#include "facade/redis_parser.h"
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
  TieredStats tiered_stats;

  size_t qps = 0;
  size_t heap_used_bytes = 0;
  size_t heap_comitted_bytes = 0;

  facade::ConnectionStats conn_stats;
};

class ServerFamily {
 public:
  ServerFamily(Service* service);
  ~ServerFamily();

  void Init(util::AcceptServer* acceptor);
  void Register(CommandRegistry* registry);
  void Shutdown();

  Service& service() { return service_;}

  Metrics GetMetrics() const;

  GlobalState* global_state() {
    return &global_state_;
  }

  ScriptMgr* script_mgr() {
    return script_mgr_.get();
  }

  void StatsMC(std::string_view section, facade::ConnectionContext* cntx);

  std::error_code DoSave(Transaction* transaction, std::string* err_details);
  std::error_code DoFlush(Transaction* transaction, DbIndex db_ind);

  std::string LastSaveFile() const;

 private:
  uint32_t shard_count() const {
    return ess_.size();
  }

  void Auth(CmdArgList args, ConnectionContext* cntx);
  void Client(CmdArgList args, ConnectionContext* cntx);
  void Config(CmdArgList args, ConnectionContext* cntx);
  void DbSize(CmdArgList args, ConnectionContext* cntx);
  void Debug(CmdArgList args, ConnectionContext* cntx);
  void Memory(CmdArgList args, ConnectionContext* cntx);
  void FlushDb(CmdArgList args, ConnectionContext* cntx);
  void FlushAll(CmdArgList args, ConnectionContext* cntx);
  void Info(CmdArgList args, ConnectionContext* cntx);
  void Hello(CmdArgList args, ConnectionContext* cntx);
  void LastSave(CmdArgList args, ConnectionContext* cntx);
  void Latency(CmdArgList args, ConnectionContext* cntx);
  void Psync(CmdArgList args, ConnectionContext* cntx);
  void ReplicaOf(CmdArgList args, ConnectionContext* cntx);
  void Role(CmdArgList args, ConnectionContext* cntx);
  void Save(CmdArgList args, ConnectionContext* cntx);
  void Script(CmdArgList args, ConnectionContext* cntx);
  void Sync(CmdArgList args, ConnectionContext* cntx);


  void _Shutdown(CmdArgList args, ConnectionContext* cntx);

  void SyncGeneric(std::string_view repl_master_id, uint64_t offs, ConnectionContext* cntx);

  uint32_t task_10ms_ = 0;
  Service& service_;
  EngineShardSet& ess_;

  util::AcceptServer* acceptor_ = nullptr;
  util::ProactorBase* pb_task_ = nullptr;

  mutable ::boost::fibers::mutex replicaof_mu_, save_mu_;
  std::shared_ptr<Replica> replica_;  // protected by replica_of_mu_

  std::unique_ptr<ScriptMgr> script_mgr_;


  int64_t last_save_;  // in seconds. protected by save_mu_.
  std::string last_save_file_;   // protected by save_mu_.

  GlobalState global_state_;
  time_t start_time_ = 0;  // in seconds, epoch time.

  // RDB_TYPE_xxx -> count mapping.
  std::vector<std::pair<unsigned, size_t>> last_save_freq_map_;
};

}  // namespace dfly
