// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/btree_map.h>

#include <atomic>
#include <memory>

#include "server/conn_context.h"

namespace facade {
class RedisReplyBuilder;
}  // namespace facade

namespace util {
class ListenerInterface;
}  // namespace util

namespace dfly {

class EngineShardSet;
class ServerFamily;
class RdbSaver;
class JournalStreamer;
struct ReplicaRoleInfo;

// Stores information related to a single flow.
struct FlowInfo {
  FlowInfo();
  ~FlowInfo();
  // Shutdown associated socket if its still open.
  void TryShutdownSocket();

  facade::Connection* conn;

  Fiber full_sync_fb;               // Full sync fiber.
  std::unique_ptr<RdbSaver> saver;  // Saver used by the full sync phase.
  std::unique_ptr<JournalStreamer> streamer;
  std::string eof_token;

  uint64_t last_acked_lsn;

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
// An important aspect is synchronization and efficient locking. Two levels of locking are used:
//  1. Global locking.
//    Member  mutex `mu_` is used for synchronizing operations connected with internal data
//    structures.
//  2. Per-replica locking
//    ReplicaInfo contains a separate mutex that is used for replica-only routines. It is held
//    during state transitions (start full sync, start stable state sync), cancellation and member
//    access.
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
  struct ReplicaInfo {
    ReplicaInfo(unsigned flow_count, std::string address, uint32_t listening_port,
                Context::ErrHandler err_handler)
        : state{SyncState::PREPARATION}, cntx{std::move(err_handler)}, address{std::move(address)},
          listening_port(listening_port), flows{flow_count} {
    }

    std::atomic<SyncState> state;
    Context cntx;

    std::string address;
    uint32_t listening_port;

    std::vector<FlowInfo> flows;
    Mutex mu;  // See top of header for locking levels.
  };

 public:
  DflyCmd(ServerFamily* server_family);

  void Run(CmdArgList args, ConnectionContext* cntx);

  void OnClose(ConnectionContext* cntx);

  void BreakOnShutdown();

  // Stop all background processes so we can exit in orderly manner.
  void Shutdown();

  // Create new sync session.
  std::pair<uint32_t, std::shared_ptr<ReplicaInfo>> CreateSyncSession(ConnectionContext* cntx);

  std::vector<ReplicaRoleInfo> GetReplicasRoleInfo();

 private:
  // JOURNAL [START/STOP]
  // Start or stop journaling.
  void Journal(CmdArgList args, ConnectionContext* cntx);

  // THREAD [to_thread]
  // Return connection thread index or migrate to another thread.
  void Thread(CmdArgList args, ConnectionContext* cntx);

  // FLOW <masterid> <syncid> <flowid>
  // Register connection as flow for sync session.
  void Flow(CmdArgList args, ConnectionContext* cntx);

  // SYNC <syncid>
  // Initiate full sync.
  void Sync(CmdArgList args, ConnectionContext* cntx);

  // STARTSTABLE <syncid>
  // Switch to stable state replication.
  void StartStable(CmdArgList args, ConnectionContext* cntx);

  // TAKEOVER <syncid>
  // Shut this master down atomically with replica promotion.
  void TakeOver(CmdArgList args, ConnectionContext* cntx);

  // EXPIRE
  // Check all keys for expiry.
  void Expire(CmdArgList args, ConnectionContext* cntx);

  // REPLICAOFFSET
  // Return journal records num sent for each flow of replication.
  void ReplicaOffset(CmdArgList args, ConnectionContext* cntx);

  // Start full sync in thread. Start FullSyncFb. Called for each flow.
  facade::OpStatus StartFullSyncInThread(FlowInfo* flow, Context* cntx, EngineShard* shard);

  // Stop full sync in thread. Run state switch cleanup.
  void StopFullSyncInThread(FlowInfo* flow, EngineShard* shard);

  // Start stable sync in thread. Called for each flow.
  facade::OpStatus StartStableSyncInThread(FlowInfo* flow, Context* cntx, EngineShard* shard);

  // Fiber that runs full sync for each flow.
  void FullSyncFb(FlowInfo* flow, Context* cntx);

  // Main entrypoint for stopping replication.
  void StopReplication(uint32_t sync_id);

  // Transition into cancelled state, run cleanup.
  void CancelReplication(uint32_t sync_id, std::shared_ptr<ReplicaInfo> replica_info_ptr);

  // Get ReplicaInfo by sync_id.
  std::shared_ptr<ReplicaInfo> GetReplicaInfo(uint32_t sync_id);

  // Find sync info by id or send error reply.
  std::pair<uint32_t, std::shared_ptr<ReplicaInfo>> GetReplicaInfoOrReply(
      std::string_view id, facade::RedisReplyBuilder* rb);

  // Check replica is in expected state and flows are set-up correctly.
  bool CheckReplicaStateOrReply(const ReplicaInfo& ri, SyncState expected,
                                facade::RedisReplyBuilder* rb);

  // Return a map between replication ID to lag. lag is defined as the maximum of difference
  // between the master's LSN and the last acknowledged LSN in over all shards.
  std::map<uint32_t, LSN> ReplicationLags() const;

 private:
  ServerFamily* sf_;  // Not owned

  TxId journal_txid_ = 0;

  uint32_t next_sync_id_ = 1;

  using ReplicaInfoMap = absl::btree_map<uint32_t, std::shared_ptr<ReplicaInfo>>;
  ReplicaInfoMap replica_infos_;

  Mutex mu_;  // Guard global operations. See header top for locking levels.
};

}  // namespace dfly
