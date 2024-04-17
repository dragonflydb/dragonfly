// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <optional>
#include <string>

#include "facade/conn_context.h"
#include "facade/dragonfly_listener.h"
#include "facade/redis_parser.h"
#include "facade/reply_builder.h"
#include "server/channel_store.h"
#include "server/detail/save_stages_controller.h"
#include "server/engine_shard_set.h"
#include "server/replica.h"
#include "server/server_state.h"
#include "util/fibers/fiberqueue_threadpool.h"
#include "util/fibers/future.h"

void SlowLogGet(dfly::CmdArgList args, dfly::ConnectionContext* cntx, dfly::Service& service,
                std::string_view sub_cmd);

namespace util {

class AcceptServer;
class ListenerInterface;
class HttpListenerBase;

}  // namespace util

namespace dfly {

namespace detail {

class SnapshotStorage;

}  // namespace detail

std::string GetPassword();

namespace journal {
class Journal;
}  // namespace journal

class ClusterFamily;
class ConnectionContext;
class CommandRegistry;
class DflyCmd;
class Service;
class ScriptMgr;

struct ReplicaRoleInfo {
  std::string id;
  std::string address;
  uint32_t listening_port;
  std::string_view state;
  uint64_t lsn_lag;
};

struct ReplicationMemoryStats {
  size_t streamer_buf_capacity_bytes_ = 0;  // total capacities of streamer buffers
  size_t full_sync_buf_bytes_ = 0;          // total bytes used for full sync buffers
};

// Global peak stats recorded after aggregating metrics over all shards.
// Note that those values are only updated during GetMetrics calls.
struct PeakStats {
  size_t conn_dispatch_queue_bytes = 0;  // peak value of conn_stats.dispatch_queue_bytes
  size_t conn_read_buf_capacity = 0;     // peak of total read buf capcacities
};

// Aggregated metrics over multiple sources on all shards
struct Metrics {
  SliceEvents events;              // general keyspace stats
  std::vector<DbStats> db_stats;   // dbsize stats
  EngineShard::Stats shard_stats;  // per-shard stats

  facade::FacadeStats facade_stats;  // client stats and buffer sizes
  TieredStats tiered_stats;          // stats for tiered storage
  TieredStatsV2 tiered_stats_v2;

  IoMgrStats disk_stats;  // disk stats for io_mgr
  SearchStats search_stats;
  ServerState::Stats coordinator_stats;  // stats on transaction running
  PeakStats peak_stats;

  size_t uptime = 0;
  size_t qps = 0;

  size_t heap_used_bytes = 0;
  size_t small_string_bytes = 0;
  uint32_t traverse_ttl_per_sec = 0;
  uint32_t delete_ttl_per_sec = 0;
  uint64_t fiber_switch_cnt = 0;
  uint64_t fiber_switch_delay_usec = 0;
  uint64_t tls_bytes = 0;

  // Statistics about fibers running for a long time (more than 1ms).
  uint64_t fiber_longrun_cnt = 0;
  uint64_t fiber_longrun_usec = 0;

  // Max length of the all the tx shard-queues.
  uint32_t tx_queue_len = 0;
  uint32_t worker_fiber_count = 0;
  size_t worker_fiber_stack_size = 0;

  // command call frequencies (count, aggregated latency in usec).
  std::map<std::string, std::pair<uint64_t, uint64_t>> cmd_stats_map;
  std::vector<ReplicaRoleInfo> replication_metrics;
};

struct LastSaveInfo {
  // last success save info
  void SetLastSaveError(const detail::SaveInfo& save_info) {
    last_error = save_info.error;
    last_error_time = save_info.save_time;
    failed_duration_sec = save_info.duration_sec;
  }

  time_t save_time = 0;  // epoch time in seconds.
  uint32_t success_duration_sec = 0;
  std::string file_name;                                      //
  std::vector<std::pair<std::string_view, size_t>> freq_map;  // RDB_TYPE_xxx -> count mapping.
  // last error save info
  GenericError last_error;
  time_t last_error_time = 0;      // epoch time in seconds.
  time_t failed_duration_sec = 0;  // epoch time in seconds.
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
  explicit ServerFamily(Service* service);
  ~ServerFamily();

  void Init(util::AcceptServer* acceptor, std::vector<facade::Listener*> listeners);
  void Register(CommandRegistry* registry);
  void Shutdown();

  void ShutdownCmd(CmdArgList args, ConnectionContext* cntx);

  Service& service() {
    return service_;
  }

  void ResetStat();

  Metrics GetMetrics() const;

  ScriptMgr* script_mgr() {
    return script_mgr_.get();
  }

  const ScriptMgr* script_mgr() const {
    return script_mgr_.get();
  }

  void StatsMC(std::string_view section, facade::ConnectionContext* cntx);

  // if new_version is true, saves DF specific, non redis compatible snapshot.
  // if basename is not empty it will override dbfilename flag.
  GenericError DoSave(bool new_version, std::string_view basename, Transaction* transaction,
                      bool ignore_state = false);

  // Calls DoSave with a default generated transaction and with the format
  // specified in --df_snapshot_format
  GenericError DoSave(bool ignore_state = false);

  // Burns down and destroy all the data from the database.
  // if kDbAll is passed, burns all the databases to the ground.
  std::error_code Drakarys(Transaction* transaction, DbIndex db_ind);

  LastSaveInfo GetLastSaveInfo() const;

  // Load snapshot from file (.rdb file or summary.dfs file) and return
  // future with error_code.
  util::fb2::Future<GenericError> Load(const std::string& file_name);

  bool TEST_IsSaving() const;

  void ConfigureMetrics(util::HttpListenerBase* listener);

  void PauseReplication(bool pause);
  std::optional<ReplicaOffsetInfo> GetReplicaOffsetInfo();

  const std::string& master_replid() const {
    return master_replid_;
  }

  journal::Journal* journal() {
    return journal_.get();
  }

  DflyCmd* GetDflyCmd() const {
    return dfly_cmd_.get();
  }

  absl::Span<facade::Listener* const> GetListeners() const {
    return listeners_;
  }

  std::vector<facade::Listener*> GetNonPriviligedListeners() const;

  bool HasReplica() const;
  std::optional<Replica::Info> GetReplicaInfo() const;

  void OnClose(ConnectionContext* cntx);

  void BreakOnShutdown();

  void CancelBlockingOnThread(std::function<facade::OpStatus(ArgSlice)> = {});

  // Sets the server to replicate another instance. Does not flush the database beforehand!
  void Replicate(std::string_view host, std::string_view port);

 private:
  void JoinSnapshotSchedule();
  void LoadFromSnapshot();

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
  void ReplicaOf(CmdArgList args, ConnectionContext* cntx);
  void AddReplicaOf(CmdArgList args, ConnectionContext* cntx);
  void ReplTakeOver(CmdArgList args, ConnectionContext* cntx);
  void ReplConf(CmdArgList args, ConnectionContext* cntx);
  void Role(CmdArgList args, ConnectionContext* cntx);
  void Save(CmdArgList args, ConnectionContext* cntx);
  void BgSave(CmdArgList args, ConnectionContext* cntx);
  void Script(CmdArgList args, ConnectionContext* cntx);
  void SlowLog(CmdArgList args, ConnectionContext* cntx);
  void Module(CmdArgList args, ConnectionContext* cntx);

  void SyncGeneric(std::string_view repl_master_id, uint64_t offs, ConnectionContext* cntx);

  enum ActionOnConnectionFail {
    kReturnOnError,        // if we fail to connect to master, return to err
    kContinueReplication,  // continue attempting to connect to master, regardless of initial
                           // failure
  };

  // REPLICAOF implementation. See arguments above
  void ReplicaOfInternal(CmdArgList args, ConnectionContext* cntx, ActionOnConnectionFail on_error);

  // Returns the number of loaded keys if successful.
  io::Result<size_t> LoadRdb(const std::string& rdb_file);

  void SnapshotScheduling();

  void SendInvalidationMessages() const;

  // Helper function to retrieve version(true if format is dfs rdb), and basename from args.
  // In case of an error an empty optional is returned.
  using VersionBasename = std::pair<bool, std::string_view>;
  std::optional<VersionBasename> GetVersionAndBasename(CmdArgList args, ConnectionContext* cntx);

  void BgSaveFb(boost::intrusive_ptr<Transaction> trans);

  GenericError DoSaveCheckAndStart(bool new_version, string_view basename, Transaction* trans,
                                   bool ignore_state = false);

  GenericError WaitUntilSaveFinished(Transaction* trans, bool ignore_state = false);
  void StopAllClusterReplicas();

  util::fb2::Fiber snapshot_schedule_fb_;
  util::fb2::Future<GenericError> load_result_;

  uint32_t stats_caching_task_ = 0;
  Service& service_;

  util::AcceptServer* acceptor_ = nullptr;
  std::vector<facade::Listener*> listeners_;
  util::ProactorBase* pb_task_ = nullptr;

  mutable util::fb2::Mutex replicaof_mu_, save_mu_;
  std::shared_ptr<Replica> replica_ ABSL_GUARDED_BY(replicaof_mu_);
  std::vector<std::unique_ptr<Replica>> cluster_replicas_
      ABSL_GUARDED_BY(replicaof_mu_);  // used to replicating multiple nodes to single dragonfly

  std::unique_ptr<ScriptMgr> script_mgr_;
  std::unique_ptr<journal::Journal> journal_;
  std::unique_ptr<DflyCmd> dfly_cmd_;

  std::string master_replid_;

  time_t start_time_ = 0;  // in seconds, epoch time.

  LastSaveInfo last_save_info_ ABSL_GUARDED_BY(save_mu_);
  std::unique_ptr<detail::SaveStagesController> save_controller_ ABSL_GUARDED_BY(save_mu_);

  // Used to override save on shutdown behavior that is usually set
  // be --dbfilename.
  bool save_on_shutdown_{true};

  util::fb2::Done schedule_done_;
  std::unique_ptr<util::fb2::FiberQueueThreadPool> fq_threadpool_;
  std::shared_ptr<detail::SnapshotStorage> snapshot_storage_;

  // protected by save_mu_
  util::fb2::Fiber bg_save_fb_;

  mutable util::fb2::Mutex peak_stats_mu_;
  mutable PeakStats peak_stats_;
};

// Reusable CLIENT PAUSE implementation that blocks while polling is_pause_in_progress
std::optional<util::fb2::Fiber> Pause(std::vector<facade::Listener*> listeners,
                                      facade::Connection* conn, ClientPause pause_state,
                                      std::function<bool()> is_pause_in_progress);

}  // namespace dfly
