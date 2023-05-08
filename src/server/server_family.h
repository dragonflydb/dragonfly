// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <string>

#include "facade/conn_context.h"
#include "facade/redis_parser.h"
#include "server/channel_store.h"
#include "server/cluster/cluster_data.h"
#include "server/engine_shard_set.h"

namespace util {
class AcceptServer;
class ListenerInterface;
class HttpListenerBase;
}  // namespace util

namespace dfly {

std::string GetPassword();

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
  uint64_t ooo_tx_transaction_cnt = 0;
  uint32_t traverse_ttl_per_sec = 0;
  uint32_t delete_ttl_per_sec = 0;

  facade::ConnectionStats conn_stats;
};

struct LastSaveInfo {
  time_t save_time = 0;  // epoch time in seconds.
  uint32_t duration_sec = 0;
  std::string file_name;                                      //
  std::vector<std::pair<std::string_view, size_t>> freq_map;  // RDB_TYPE_xxx -> count mapping.
};

struct SnapshotSpec {
  std::string hour_spec;
  std::string minute_spec;
};

struct ReplicaOffsetInfo {
  std::string sync_id;
  std::vector<uint64_t> flow_offsets;
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

  ClusterData* cluster_data() {
    return cluster_data_.get();
  }

  void StatsMC(std::string_view section, facade::ConnectionContext* cntx);

  // if new_version is true, saves DF specific, non redis compatible snapshot.
  GenericError DoSave(bool new_version, Transaction* transaction);

  // Calls DoSave with a default generated transaction and with the format
  // specified in --df_snapshot_format
  GenericError DoSave();

  // Burns down and destroy all the data from the database.
  // if kDbAll is passed, burns all the databases to the ground.
  std::error_code Drakarys(Transaction* transaction, DbIndex db_ind);

  std::shared_ptr<const LastSaveInfo> GetLastSaveInfo() const;

  // Load snapshot from file (.rdb file or summary.dfs file) and return
  // future with error_code.
  Future<std::error_code> Load(const std::string& file_name);

  // used within tests.
  bool IsSaving() const {
    return is_saving_.load(std::memory_order_relaxed);
  }

  void ConfigureMetrics(util::HttpListenerBase* listener);

  void PauseReplication(bool pause);
  std::optional<ReplicaOffsetInfo> GetReplicaOffsetInfo();

  const std::string& master_id() const {
    return master_id_;
  }

  journal::Journal* journal() {
    return journal_.get();
  }

  void OnClose(ConnectionContext* cntx);

  void BreakOnShutdown();

 private:
  uint32_t shard_count() const {
    return shard_set->size();
  }

  std::string BuildClusterNodeReply(ConnectionContext* cntx) const;

  void Auth(CmdArgList args, ConnectionContext* cntx);
  void Client(CmdArgList args, ConnectionContext* cntx);
  void Cluster(CmdArgList args, ConnectionContext* cntx);
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
  void ReadOnly(CmdArgList args, ConnectionContext* cntx);
  void ReplicaOf(CmdArgList args, ConnectionContext* cntx);
  void ReplConf(CmdArgList args, ConnectionContext* cntx);
  void Role(CmdArgList args, ConnectionContext* cntx);
  void Save(CmdArgList args, ConnectionContext* cntx);
  void Script(CmdArgList args, ConnectionContext* cntx);
  void Sync(CmdArgList args, ConnectionContext* cntx);

  void _Shutdown(CmdArgList args, ConnectionContext* cntx);

  void SyncGeneric(std::string_view repl_master_id, uint64_t offs, ConnectionContext* cntx);

  std::error_code LoadRdb(const std::string& rdb_file);

  void SnapshotScheduling(const SnapshotSpec& time);

  Fiber snapshot_schedule_fb_;
  Future<std::error_code> load_result_;

  uint32_t stats_caching_task_ = 0;
  Service& service_;

  util::AcceptServer* acceptor_ = nullptr;
  util::ListenerInterface* main_listener_ = nullptr;
  util::ProactorBase* pb_task_ = nullptr;

  mutable Mutex replicaof_mu_, save_mu_;
  std::shared_ptr<Replica> replica_;  // protected by replica_of_mu_

  std::unique_ptr<ScriptMgr> script_mgr_;
  std::unique_ptr<journal::Journal> journal_;
  std::unique_ptr<DflyCmd> dfly_cmd_;

  std::string master_id_;

  time_t start_time_ = 0;  // in seconds, epoch time.
  bool is_emulated_cluster_ = false;

  std::unique_ptr<ClusterData> cluster_data_;

  std::shared_ptr<LastSaveInfo> last_save_info_;  // protected by save_mu_;
  std::atomic_bool is_saving_{false};

  // Used to override save on shutdown behavior that is usually set
  // be --dbfilename.
  bool save_on_shutdown_{true};

  Done schedule_done_;
  std::unique_ptr<FiberQueueThreadPool> fq_threadpool_;
};

}  // namespace dfly
