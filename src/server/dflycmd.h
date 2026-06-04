// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/btree_map.h>

#include <algorithm>
#include <atomic>
#include <memory>

#include "server/conn_context.h"
#include "server/execution_state.h"
#include "server/synchronization.h"
#include "util/fibers/synchronization.h"

namespace facade {
class RedisReplyBuilder;
}  // namespace facade

namespace util {
class ListenerInterface;
}  // namespace util

namespace dfly {

class EngineShard;
class EngineShardSet;
class ServerFamily;
class RdbSaver;
class JournalStreamer;
struct ReplicaRoleInfo;
struct ReplicationMemoryStats;

// Stores information related to a single flow.
struct FlowInfo {
  FlowInfo();
  ~FlowInfo();

  // Shutdown associated socket if its still open.
  void TryShutdownSocket();

  facade::Connection* conn = nullptr;

  // Owned by the shard that this flow corresponds to; only that shard's proactor
  // ever reads or writes these pointers, so no synchronization is needed.
  std::unique_ptr<RdbSaver> saver;            // Saver for full sync phase.
  std::unique_ptr<JournalStreamer> streamer;  // Streamer for stable sync phase
  std::string eof_token;

  std::optional<LSN> start_partial_sync_at;
  // Written by REPLCONF ACK on the owner-shard proactor; all readers also run there.
  uint64_t last_acked_lsn = 0;

  std::function<void()> cleanup;  // Optional cleanup for cancellation.
};

// DflyCmd is responsible for managing replication. A master instance can be connected
// to many replica instances, what is more, each of them can open multiple connections.
// This is why its important to understand replica lifecycle management before making
// any crucial changes.
//
// A ReplicaInfo instance is responsible for managing a replica's state and is accessible by its
// sync_id. Each per-thread connection is called a Flow and is represented by the FlowInfo
// instance, accessible by its index.
//
// An important aspect is synchronization and efficient locking. Three patterns are used:
//  1. Global locking.
//    Member  mutex `mu_` is used for synchronizing operations connected with internal data
//    structures.
//  2. Per-replica locking
//    ReplicaInfo contains a separate mutex that is used for replica-only routines. It is held
//    during state transitions (start full sync, start stable state sync), cancellation and member
//    access.
//  3. Lock-free snapshot.
//    A copy of `replica_infos_` is published to a thread-local on every proactor via
//    `UpdateReplicaInfoCacheLocked()` (which must be called from each mutator of replica_infos_).
//    Readers (INFO REPLICATION, metrics) load this snapshot and access ReplicaInfo state via
//    its atomic getters (GetReplicaState, etc.) without taking any lock.
//
// Upon first connection from the replica, a new ReplicaInfo is created.
// It transitions through the following phases:
//  1. Preparation
//    During this start phase the "flows" are set up - one connection for every master thread. Those
//    connections registered by the FLOW command sent from each newly opened connection.
//  2. Full sync
//    This phase is initiated by the SYNC command. It makes sure all flows are connected and the
//    replica is in a valid state.
//  3. Stable state sync
//    After the replica has received confirmation, that each flow is ready to transition, it sends a
//    STARTSTABLE command. This transitions the replica into streaming journal changes.
//  4. Cancellation
//    This can happed due to an error at any phase or through a normal abort. For properly releasing
//    resources we need to run a multi-step cancellation procedure:
//    1. Transition state
//      We obtain the ReplicaInfo lock, transition into the cancelled state and cancel the context.
//    2. Joining tasks
//      Running tasks will stop on receiving the cancellation flag. Each FlowInfo has also an
//      optional cleanup handler, that is invoked after cancelling. This should allow recovering
//      from any state. The flows task will be awaited and joined if present.
//    3. Unlocking the mutex
//      Now that all tasks have finished and all cleanup handlers have run, we can safely release
//      the per-replica mutex, so that all OnClose handlers will unblock and  internal resources
//      will be released by dragonfly. Then the ReplicaInfo is removed from the global map.
//
//
class DflyCmd {
 public:
  // See class comments for state descriptions.
  enum class SyncState { PREPARATION, FULL_SYNC, STABLE_SYNC, CANCELLED };

  // Stores information related to a single replica.
  class ABSL_LOCKABLE ReplicaInfo {
   public:
    ReplicaInfo(unsigned flow_count, std::string address, uint32_t listening_port,
                ExecutionState::ErrHandler err_handler)
        : replica_state_{SyncState::PREPARATION},
          exec_st_{std::move(err_handler)},
          address_{std::move(address)},
          listening_port_(listening_port),
          flows_{flow_count} {
    }

    // Immutable after construction; safe to read without locking.
    const std::string& GetAddress() const {
      return address_;
    }
    uint32_t GetListeningPort() const {
      return listening_port_;
    }

    // Returns the replica ID, or an empty view if SetId has not been called.
    // Thread-safe: id is written at most once via SetId, with release/acquire
    // ordering enforced by id_set_.
    std::string_view GetId() const {
      if (id_set_.load(std::memory_order_relaxed)) {
        return id_;
      }
      return {};
    }
    // Sets the replica ID. Expected to be called at most once per ReplicaInfo
    // lifetime (from the REPLCONF CLIENT-ID handler on the owner thread).
    void SetId(std::string_view id) {
      DCHECK(!id_set_.load(std::memory_order_relaxed)) << "SetId called more than once";
      id_.assign(id);
      id_set_.store(true, std::memory_order_relaxed);
    }

    // Atomic to allow cross-thread access: SetDflyClientVersion writes from the
    // REPLCONF CLIENT-VERSION handler without locking, while Flow() reads under
    // an exclusive lock on mutex(). Relaxed ordering: version is independent of
    // other state.
    DflyVersion GetVersion() const {
      return version_.load(std::memory_order_relaxed);
    }
    void SetVersion(DflyVersion v) {
      version_.store(v, std::memory_order_relaxed);
    }

    // State machine field. Setter must hold GetMutex() exclusively — the value is
    // only written under the lock so transitions remain serialized. Readers do not
    // need any lock; the atomic load lets INFO/metrics observe state lock-free.
    SyncState GetReplicaState() const {
      return replica_state_.load(std::memory_order_relaxed);
    }
    void SetReplicaState(SyncState s) {
      replica_state_.store(s, std::memory_order_relaxed);
    }

    // Per-shard fibers receive &GetExecState() and pass it into StartFullSyncInThread
    // and friends. ExecutionState has its own internal synchronization.
    ExecutionState& GetExecState() {
      return exec_st_;
    }
    const ExecutionState& GetExecState() const {
      return exec_st_;
    }

    // Per-shard flow access; idx is the master shard index. The flows_ vector
    // is sized once at construction so addresses are stable for the ReplicaInfo's
    // lifetime. Default contract: caller must hold GetMutex(). Exception: the
    // saver/streamer/last_acked_lsn fields are touched only by the owner-shard
    // proactor (idx == that proactor's shard_id) and may be accessed lock-free
    // from that proactor — see the FlowInfo field comments.
    FlowInfo& GetFlow(size_t idx) {
      return flows_[idx];
    }
    const FlowInfo& GetFlow(size_t idx) const {
      return flows_[idx];
    }
    size_t GetFlowCount() const {
      return flows_.size();
    }

    // Returns true if every flow has a live connection. Caller must hold a lock.
    bool AllFlowsConnected() const {
      return std::ranges::all_of(flows_, [](const FlowInfo& flow) { return flow.conn != nullptr; });
    }

    // Returns the per-replica mutex. Callers acquire it via util::fb2::LockGuard
    // (exclusive), dfly::SharedLock (shared), or std::shared_lock(..., std::try_to_lock)
    // (try-shared). Returned by reference rather than a factory-returned scoped lock
    // because clang's -Wthread-safety-analysis cannot track ownership transfer
    // through a factory function's return value.
    util::fb2::SharedMutex& GetMutex() {
      return shared_mu_;
    }

    // Transition into cancelled state, run cleanup.
    void Cancel();

   private:
    // Transitions still serialized under shared_mu_; atomic so readers can load lock-free.
    std::atomic<SyncState> replica_state_;
    ExecutionState exec_st_;

    std::string id_;
    // Publication guard for id_: SetId writes id_ then sets this flag with
    // release; id() acquires this flag and reads id_ on true.
    std::atomic<bool> id_set_{false};
    std::string address_;
    uint32_t listening_port_;

    // We expect to update version_ during handshaking, for now we set it to
    // the oldest version to be safe.
    std::atomic<DflyVersion> version_{DflyVersion::VER1};

    std::vector<FlowInfo> flows_;

    util::fb2::SharedMutex shared_mu_;
  };

 public:
  DflyCmd(ServerFamily* server_family);

  void Run(CmdArgList args, CommandContext* cmd_cntx);

  void OnClose(unsigned sync_id);

  // Stop all background processes so we can exit in orderly manner.
  void CancelReplicas() ABSL_LOCKS_EXCLUDED(mu_);

  // Create new sync session. Returns (session_id, number of flows)
  std::pair<uint32_t, unsigned> CreateSyncSession(ConnectionState* state) ABSL_LOCKS_EXCLUDED(mu_);

  // Master side access method to replication info of that connection.
  std::shared_ptr<ReplicaInfo> GetReplicaInfoFromConnection(ConnectionState* state);

  // Master-side command. Provides Replica info.
  std::vector<ReplicaRoleInfo> GetReplicasRoleInfo() const ABSL_LOCKS_EXCLUDED(mu_);

  // Must be called on the given shard's thread (e.g. from the GetMetrics
  // fan-out). Lock-free: reads thread-local replica infos.
  static ReplicationMemoryStats GetReplicationMemoryStats(EngineShard* shard);

  // Sets metadata.
  void SetDflyClientVersion(ConnectionState* state, DflyVersion version);

  // Tries to break those flows that stuck on socket write for too long time.
  void BreakStalledFlowsInShard() ABSL_NO_THREAD_SAFETY_ANALYSIS;

  using ReplicaInfoMap = absl::btree_map<uint32_t, std::shared_ptr<ReplicaInfo>>;

 private:
  // JOURNAL [START/STOP]
  // Start or stop journaling.
  // void Journal(CmdArgList args, ConnectionContext* cntx);

  // THREAD [to_thread]
  // Return connection thread index or migrate to another thread.
  void Thread(CmdArgList args, CommandContext* cmd_cntx);

  // FLOW <masterid> <syncid> <flowid> [<seqid>]
  // Register connection as flow for sync session.
  // If seqid is given, it means the client wants to try partial sync.
  // If it is possible, return Ok and prepare for a partial sync, else
  // return error and ask the replica to execute FLOW again.
  void Flow(CmdArgList args, CommandContext* cmd_cntx);

  // SYNC <syncid>
  // Initiate full sync.
  void Sync(CmdArgList args, CommandContext* cmd_cntx);

  // STARTSTABLE <syncid>
  // Switch to stable state replication.
  void StartStable(CmdArgList args, CommandContext* cmd_cntx);
  // TAKEOVER <syncid>
  // Shut this master down atomically with replica promotion.
  void TakeOver(CmdArgList args, CommandContext* cmd_cntx);

  // EXPIRE
  // Check all keys for expiry.
  void Expire(CmdArgList args, CommandContext* cmd_cntx);

  // REPLICAOFFSET
  // Return journal records num sent for each flow of replication.
  void ReplicaOffset(CmdArgList args, CommandContext* cmd_cntx);

  void Load(CmdArgList args, CommandContext* cmd_cntx);

  // Start full sync in thread. Start FullSyncFb. Called for each flow.
  facade::OpStatus StartFullSyncInThread(DflyVersion version, FlowInfo* flow, ExecutionState* cntx,
                                         EngineShard* shard);

  // Stop full sync in thread. Run state switch cleanup.
  facade::OpStatus StopFullSyncInThread(FlowInfo* flow, ExecutionState* cntx, EngineShard* shard);

  // Start stable sync in thread. Called for each flow.
  void StartStableSyncInThread(FlowInfo* flow, ExecutionState* cntx, EngineShard* shard);

  // Get ReplicaInfo by sync_id.
  std::shared_ptr<ReplicaInfo> GetReplicaInfo(uint32_t sync_id) ABSL_LOCKS_EXCLUDED(mu_);

  // Find sync info by id or send error reply.
  std::pair<uint32_t, std::shared_ptr<ReplicaInfo>> GetReplicaInfoOrReply(std::string_view id,
                                                                          CommandContext* cmd_cntx)
      ABSL_LOCKS_EXCLUDED(mu_);

  // Check replica is in expected state and flows are set-up correctly.
  bool CheckReplicaStateOrReply(const ReplicaInfo& ri, SyncState expected,
                                CommandContext* cmd_cntx);

  // Main entrypoint for stopping replication.
  void StopReplication(uint32_t sync_id) ABSL_LOCKS_EXCLUDED(mu_);

  std::optional<LSN> ParseLsnVec(std::string_view lsn_vec, size_t last_journal_lsn_size,
                                 size_t flow_id, CommandContext* cmd_cntx);

  // Checks if LSN exists in the partial sync buffer. If not, also LOG that we can't
  // partial sync.
  bool IsLSNInPartialSyncBuffer(LSN lsn) const;

  // Return a map between replication ID to lag. lag is defined as the maximum of difference
  // between the master's LSN and the last acknowledged LSN in over all shards.
  std::map<uint32_t, LSN> ReplicationLags(const ReplicaInfoMap& replicas_local_cache) const;

  // Publishes a fresh copy of replica_infos_ to a thread-local on every proactor.
  // Caller must hold mu_. Readers (INFO/metrics) load this view lock-free.
  void UpdateReplicaInfoCacheLocked() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  ServerFamily* sf_;  // Not owned
  uint32_t next_sync_id_ = 1;

  ReplicaInfoMap replica_infos_ ABSL_GUARDED_BY(mu_);

  mutable util::fb2::Mutex mu_;  // Guard global operations. See header top for locking levels.
};

std::string_view SyncStateName(DflyCmd::SyncState sync_state);

}  // namespace dfly
