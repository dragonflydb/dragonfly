// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "facade/conn_context.h"
#include "facade/redis_parser.h"
#include "server/engine_shard_set.h"
#include "util/proactor_pool.h"

namespace util {
class AcceptServer;
class ListenerInterface;
class HttpListenerBase;
}  // namespace util

namespace dfly {

namespace journal {
class Journal;
}  // namespace journal

class ConnectionContext;
class CommandRegistry;
class DflyCmd;
class Service;
class Replica;
class ScriptMgr;

struct Metrics {
  std::vector<DbStats> db;
  SliceEvents events;
  TieredStats tiered_stats;
  EngineShard::Stats shard_stats;

  size_t uptime = 0;
  size_t qps = 0;
  size_t heap_used_bytes = 0;
  size_t heap_comitted_bytes = 0;
  size_t small_string_bytes = 0;
  uint32_t traverse_ttl_per_sec = 0;
  uint32_t delete_ttl_per_sec = 0;

  facade::ConnectionStats conn_stats;
};

struct LastSaveInfo {
  time_t save_time;                                           // epoch time in seconds.
  std::string file_name;                                      //
  std::vector<std::pair<std::string_view, size_t>> freq_map;  // RDB_TYPE_xxx -> count mapping.
};

class ServerFamily {
 public:
  ServerFamily(Service* service);
  ~ServerFamily();

  void Init(util::AcceptServer* acceptor, util::ListenerInterface* main_listener);
  void Register(CommandRegistry* registry);
  void Shutdown();

  Service& service() {
    return service_;
  }

  Metrics GetMetrics() const;

  ScriptMgr* script_mgr() {
    return script_mgr_.get();
  }

  void StatsMC(std::string_view section, facade::ConnectionContext* cntx);

  std::error_code DoSave(Transaction* transaction, std::string* err_details);
  std::error_code DoFlush(Transaction* transaction, DbIndex db_ind);

  std::shared_ptr<const LastSaveInfo> GetLastSaveInfo() const;

  std::error_code LoadRdb(const std::string& rdb_file);

  // used within tests.
  bool IsSaving() const {
    return is_saving_.load(std::memory_order_relaxed);
  }

  void ConfigureMetrics(util::HttpListenerBase* listener);

 private:
  uint32_t shard_count() const {
    return shard_set->size();
  }

  void Auth(CmdArgList args, ConnectionContext* cntx);
  void Client(CmdArgList args, ConnectionContext* cntx);
  void Config(CmdArgList args, ConnectionContext* cntx);
  void DbSize(CmdArgList args, ConnectionContext* cntx);
  void Debug(CmdArgList args, ConnectionContext* cntx);
  void Dfly(CmdArgList args, ConnectionContext* cntx);
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

  void Load(const std::string& file_name);

  void SnapshotScheduling(const std::string &&time);

  boost::fibers::fiber load_fiber_, snapshot_fiber_;

  uint32_t stats_caching_task_ = 0;
  Service& service_;

  util::AcceptServer* acceptor_ = nullptr;
  util::ListenerInterface* main_listener_ = nullptr;
  util::ProactorBase* pb_task_ = nullptr;

  mutable ::boost::fibers::mutex replicaof_mu_, save_mu_, snapshot_mu_;
  ::boost::fibers::condition_variable snapshot_timer_cv_;
  std::shared_ptr<Replica> replica_;  // protected by replica_of_mu_

  std::unique_ptr<ScriptMgr> script_mgr_;
  std::unique_ptr<journal::Journal> journal_;
  std::unique_ptr<DflyCmd> dfly_cmd_;

  time_t start_time_ = 0;  // in seconds, epoch time.

  std::shared_ptr<LastSaveInfo> lsinfo_;  // protected by save_mu_;
  std::atomic_bool is_saving_{false};
  std::atomic_bool is_running_{false};
};

}  // namespace dfly
