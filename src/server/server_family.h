// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <functional>
#include <memory>
#include <optional>
#include <string>

#include "core/qlist.h"
#include "facade/cmd_arg_parser.h"
#include "facade/facade_types.h"
#include "io/proc_reader.h"
#include "server/db_slice.h"
#include "server/engine_shard_set.h"
#include "server/metrics.h"
#include "server/replica_types.h"
#include "server/server_state.h"
#include "server/stats.h"
#include "util/fibers/fiberqueue_threadpool.h"
#include "util/fibers/future.h"

struct hdr_histogram;

namespace facade {
class Listener;
}  // namespace facade

namespace util {

class AcceptServer;
class HttpListenerBase;

}  // namespace util

namespace dfly {

namespace detail {

struct SaveStagesController;
class SnapshotStorage;

}  // namespace detail

std::string GetPassword();

// Validates server-side TLS flags: when --tls is set, at least one auth method must be
// configured (a password or a CA cert). Returns false on an invalid configuration.
bool ValidateServerTlsFlags();

// Validates the --dbfilename / --df_snapshot_format flags (filename format, no directory
// separators, extension vs snapshot format). Returns false on an invalid configuration.
bool ValidateSnapshotFilenameFlags();

class CommandContext;
class CommandRegistry;
class DflyCmd;
class Replica;
class Service;
class ScriptMgr;
class RdbLoadContext;

struct ReplicaRoleInfo {
  std::string id;
  std::string address;
  uint32_t listening_port;
  std::string_view state;
  uint64_t lsn_lag;
};

// Contains the state of the last save operation.
// This object is immutable.
struct SaveInfoData {
  time_t save_time = 0;  // epoch time in seconds.
  uint32_t success_duration_sec = 0;
  std::string file_name;
  std::vector<std::pair<std::string_view, size_t>> freq_map;  // RDB_TYPE_xxx -> count mapping.

  // last error save info
  GenericError last_error;
  time_t last_error_time = 0;      // epoch time in seconds.
  time_t failed_duration_sec = 0;  // epoch time in seconds.

  // false if last attempt failed
  bool last_bgsave_status = true;
  bool bgsave_in_progress = false;
};

// A thread-safe wrapper for SaveInfoData using the Copy-on-Write pattern.
class ThreadSafeSaveInfo {
 public:
  // Returns a snapshot of the current save info.
  SaveInfoData Get() const {
    std::lock_guard<util::fb2::Mutex> lock(data_mutex_);
    return data_;
  }

  // The modifier function is called under a lock.
  void Update(std::function<void(SaveInfoData*)> modifier) {
    std::lock_guard<util::fb2::Mutex> lock(writer_mutex_);
    SaveInfoData new_data(Get());
    modifier(&new_data);
    UpdateData(new_data);
  }

 private:
  void UpdateData(const SaveInfoData& new_data) {
    std::lock_guard<util::fb2::Mutex> lock(data_mutex_);
    data_ = new_data;
  }

  mutable util::fb2::Mutex writer_mutex_;
  mutable util::fb2::Mutex data_mutex_;
  SaveInfoData data_;
};

struct SnapshotSpec {
  std::string hour_spec;
  std::string minute_spec;
};

struct ReplicaOffsetInfo {
  std::string sync_id;
  std::vector<uint64_t> flow_offsets;
};

struct SaveCmdOptions {
  // if new_version is true, saves DF specific, non redis compatible snapshot.
  bool new_version;
  // cloud storage URI
  std::string_view cloud_uri;
  // if basename is not empty it will override dbfilename flag
  std::string_view basename;
};

bool ReadProcStats(io::StatusData* sdata);
uint64_t GetDelayMs(uint64_t cycles_ts);

class ServerFamily {
  using SinkReplyBuilder = facade::SinkReplyBuilder;

 public:
  explicit ServerFamily(Service* service);
  ~ServerFamily();

  void Init(util::AcceptServer* acceptor, std::vector<facade::Listener*> listeners);
  void Register(CommandRegistry* registry);
  void Shutdown() ABSL_LOCKS_EXCLUDED(replicaof_mu_);

  // Public because is used by DflyCmd.
  void ShutdownCmd(facade::CmdArgParser parser, CommandContext* cmd_cntx);

  Service& service() {
    return service_;
  }

  void ResetStat(Namespace* ns);

  // Collects server metrics. See MetricsCollectOpts; a default-constructed value collects all.
  Metrics GetMetrics(Namespace* ns, const MetricsCollectOpts& opts) const;

  std::string FormatInfoMetrics(const Metrics& metrics, std::string_view section,
                                bool priveleged) const;

  ScriptMgr* script_mgr() {
    return script_mgr_.get();
  }

  const ScriptMgr* script_mgr() const {
    return script_mgr_.get();
  }

  void StatsMC(std::string_view section, CommandContext* cmd_ctx);

  GenericError DoSave(const SaveCmdOptions& save_cmd_opts, Transaction* transaction,
                      bool ignore_state = false);

  // Calls DoSave with a default generated transaction and with the format
  // specified in --df_snapshot_format
  GenericError DoSave(bool ignore_state = false);

  // Burns down and destroy all the data from the database.
  // if kDbAll is passed, burns all the databases to the ground.
  // `wait` makes it wait for all fibers to finish and decommit
  void Drakarys(Transaction* transaction, DbIndex db_ind, bool wait);

  SaveInfoData GetLastSaveInfo() const;

  void FlushAll(Namespace* ns);

  // Load snapshot from file (.rdb file or summary.dfs file) and return
  // future with error_code.
  enum class LoadExistingKeys : uint8_t { kFail, kOverride };
  std::optional<util::fb2::Future<GenericError>> Load(const std::string& file_name,
                                                      LoadExistingKeys existing_keys);

  bool TEST_IsSaving() const;

  void ConfigureMetrics(util::HttpListenerBase* listener);

  void PauseReplication(bool pause) ABSL_LOCKS_EXCLUDED(replicaof_mu_);
  std::optional<ReplicaOffsetInfo> GetReplicaOffsetInfo() ABSL_LOCKS_EXCLUDED(replicaof_mu_);

  // Investigation-only (DEBUG REPLDIAG). Remove once closed.
  std::optional<int> GetReplicaMasterSocketUnreadBytes() ABSL_LOCKS_EXCLUDED(replicaof_mu_);

  const std::string& master_replid() const {
    return master_replid_;
  }

  // The lineage root replication id of this node: its own replid if it is a true master, or the
  // ancestor id advertised by its master when it is itself a replica (cascaded replication).
  std::string GetLineageId() const ABSL_LOCKS_EXCLUDED(replicaof_mu_);

  DflyCmd* GetDflyCmd() const {
    return dfly_cmd_.get();
  }

  std::optional<LastMasterSyncData> GetLastMasterData() const {
    return last_master_data_;
  }

  absl::Span<facade::Listener* const> GetListeners() const {
    return listeners_;
  }

  std::vector<facade::Listener*> GetNonPriviligedListeners() const;

  // Replica-side method. Returns replication summary if this server is a replica,
  // nullopt otherwise.
  std::optional<Metrics::ReplicaInfo> GetReplicaSummary() const;

  struct MasterLinkClientInfo {
    uint32_t client_id;
    std::string info;
  };

  // One entry per attached outbound master link; empty if not replicating.
  std::vector<MasterLinkClientInfo> GetMasterLinkClientInfo() const
      ABSL_LOCKS_EXCLUDED(replicaof_mu_);

  bool IsMasterLinkClientId(uint32_t id) const ABSL_LOCKS_EXCLUDED(replicaof_mu_);

  void OnClose(ConnectionContext* cntx);

  void CancelBlockingOnThread(std::function<facade::OpStatus(facade::ArgSlice)> = {});

  // Sets the server to replicate another instance. Does not flush the database beforehand!
  void Replicate(std::string_view host, std::string_view port);

  void UpdateMemoryGlobalStats();

  // Return true if no replicas are registered or if all replicas reached stable sync
  // Used in debug populate to DCHECK insocsistent flows that violate transaction gurantees
  bool AreAllReplicasInStableSync() const;

 private:
  // Helper to safely get save controller copy
  std::shared_ptr<detail::SaveStagesController> GetSaveController() const {
    util::fb2::LockGuard lk{save_mu_};
    return save_controller_;
  }

  bool HasPrivilegedInterface();
  void JoinSnapshotSchedule();
  void LoadFromSnapshot() ABSL_LOCKS_EXCLUDED(loading_stats_mu_);

  uint32_t shard_count() const {
    return shard_set->size();
  }

  void Auth(facade::CmdArgParser parser, CommandContext* cmd_cntx);
  void Client(facade::CmdArgParser parser, CommandContext* cmd_cntx);
  void Config(facade::CmdArgParser parser, CommandContext* cmd_cntx);
  void DbSize(facade::CmdArgParser parser, CommandContext* cmd_cntx);
  void Debug(facade::CmdArgParser parser, CommandContext* cmd_cntx);
  void Dfly(facade::CmdArgParser parser, CommandContext* cmd_cntx);
  void Memory(facade::CmdArgParser parser, CommandContext* cmd_cntx);
  void Shrink(facade::CmdArgParser parser, CommandContext* cmd_cntx);
  void FlushDb(facade::CmdArgParser parser, CommandContext* cmd_cntx);
  void Info(facade::CmdArgParser parser, CommandContext* cmd_cntx)
      ABSL_LOCKS_EXCLUDED(replicaof_mu_);
  void Hello(facade::CmdArgParser parser, CommandContext* cmd_cntx);
  void LastSave(facade::CmdArgParser parser, CommandContext* cmd_cntx);
  void Latency(facade::CmdArgParser parser, CommandContext* cmd_cntx);
  void ReplicaOf(facade::CmdArgParser parser, CommandContext* cmd_cntx);
  void AddReplicaOf(facade::CmdArgParser parser, CommandContext* cmd_cntx);
  void ReplTakeOver(facade::CmdArgParser parser, CommandContext* cmd_cntx)
      ABSL_LOCKS_EXCLUDED(replicaof_mu_);
  void ReplConf(facade::CmdArgParser parser, CommandContext* cmd_cntx);
  void Wait(facade::CmdArgParser parser, CommandContext* cmd_cntx);
  void Role(facade::CmdArgParser parser, CommandContext* cmd_cntx)
      ABSL_LOCKS_EXCLUDED(replicaof_mu_);
  void Save(facade::CmdArgParser parser, CommandContext* cmd_cntx);
  void BgSave(facade::CmdArgParser parser, CommandContext* cmd_cntx);
  void Script(facade::CmdArgParser parser, CommandContext* cmd_cntx);
  void SlowLog(facade::CmdArgParser parser, CommandContext* cmd_cntx);
  void Module(facade::CmdArgParser parser, CommandContext* cmd_cntx);

  void SyncGeneric(std::string_view repl_master_id, uint64_t offs, ConnectionContext* cntx);

  enum ActionOnConnectionFail {
    kReturnOnError,        // if we fail to connect to master, return to err
    kContinueReplication,  // continue attempting to connect to master, regardless of initial
                           // failure
  };

  void ReplicaOfInternal(facade::ParsedArgs args, CommandContext* cmnd_cntx,
                         ActionOnConnectionFail on_error) ABSL_LOCKS_EXCLUDED(replicaof_mu_);

  void ReplicaOfNoOne(SinkReplyBuilder* builder) ABSL_LOCKS_EXCLUDED(replicaof_mu_);

  struct LoadOptions {
    std::string snapshot_id;
    uint32_t shard_count = 0;      // Shard count of the snapshot being loaded.
    uint64_t num_loaded_keys = 0;  // Number of keys loaded.
  };

  // Updates LoadOptions if successful. If snapshot_id and shard_count are passed in,
  // may use them for consistency checks.
  std::error_code LoadRdb(const std::string& rdb_file, LoadExistingKeys existing_keys,
                          LoadOptions* load_opts, RdbLoadContext* load_context,
                          detail::SnapshotStorage* storage);

  void SnapshotScheduling() ABSL_LOCKS_EXCLUDED(loading_stats_mu_);

  void SendInvalidationMessages() const;

  std::optional<SaveCmdOptions> GetSaveCmdOpts(facade::ParsedArgs args, CommandContext* cmd_cntx);

  void BgSaveFb(boost::intrusive_ptr<Transaction> trans);

  struct DoSaveCheckAndStartOpts {
    bool ignore_state = false;
    bool bg_save = false;
  };

  GenericError DoSaveCheckAndStart(const SaveCmdOptions& save_cmd_opts, Transaction* trans,
                                   DoSaveCheckAndStartOpts opts) ABSL_LOCKS_EXCLUDED(save_mu_);

  GenericError WaitUntilSaveFinished(Transaction* trans,
                                     bool ignore_state = false) ABSL_NO_THREAD_SAFETY_ANALYSIS;
  void StopAllClusterReplicas() ABSL_EXCLUSIVE_LOCKS_REQUIRED(replicaof_mu_);

  static bool DoAuth(ConnectionContext* cntx, std::string_view username, std::string_view password);

  void ClientPauseCmd(facade::CmdArgParser parser, CommandContext* cmd_cntx);
  void ClientUnPauseCmd(facade::ParsedArgs args, CommandContext* cmd_cntx);

  // Set accepting_connections_ and update listners according to it
  void ChangeConnectionAccept(bool accept);

  util::fb2::Fiber snapshot_schedule_fb_;
  util::fb2::Fiber load_fiber_;

  Service& service_;

  util::AcceptServer* acceptor_ = nullptr;
  std::vector<facade::Listener*> listeners_;
  bool accepting_connections_ = true;  // reject connections near oom
  util::ProactorBase* pb_task_ = nullptr;

  mutable util::fb2::Mutex replicaof_mu_, save_mu_;
  std::shared_ptr<Replica> replica_ ABSL_GUARDED_BY(replicaof_mu_);
  std::vector<std::unique_ptr<Replica>> cluster_replicas_
      ABSL_GUARDED_BY(replicaof_mu_);  // used to replicating multiple nodes to single dragonfly

  std::unique_ptr<ScriptMgr> script_mgr_;
  std::unique_ptr<DflyCmd> dfly_cmd_;

  std::string master_replid_;
  std::optional<LastMasterSyncData> last_master_data_;

  time_t start_time_ = 0;  // in seconds, epoch time.

  ThreadSafeSaveInfo thread_safe_save_info_;
  std::shared_ptr<detail::SaveStagesController> save_controller_ ABSL_GUARDED_BY(save_mu_);

  // Used to override save on shutdown behavior that is usually set
  // be --dbfilename.
  bool save_on_shutdown_{true};

  util::fb2::Done schedule_done_;
  std::unique_ptr<util::fb2::FiberQueueThreadPool> fq_threadpool_;
  std::shared_ptr<detail::SnapshotStorage> snapshot_storage_;

  std::atomic<bool> is_c_pause_in_progress_ = false;
  // We need this because if dragonfly shuts down during pause, ServerState will destruct
  // before the dettached fiber Pause() causing a seg fault.
  std::atomic<size_t> active_pauses_ = 0;
  util::fb2::EventCount client_pause_ec_;

  // protected by save_mu_
  util::fb2::Fiber bg_save_fb_;

  mutable util::fb2::Mutex peak_stats_mu_;
  mutable PeakStats peak_stats_;

  mutable util::fb2::Mutex loading_stats_mu_;
  LoadingStats loading_stats_ ABSL_GUARDED_BY(loading_stats_mu_);

  bool legacy_format_metrics_ = true;
};

// Reusable CLIENT PAUSE implementation that blocks while polling is_pause_in_progress
std::optional<util::fb2::Fiber> Pause(std::vector<facade::Listener*> listeners, Namespace* ns,
                                      facade::Connection* conn, ClientPause pause_state,
                                      std::function<bool()> is_pause_in_progress,
                                      std::function<void()> maybe_cleanup = {});

}  // namespace dfly
