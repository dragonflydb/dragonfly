// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/btree_map.h>

#include <atomic>
#include <boost/fiber/fiber.hpp>
#include <boost/fiber/mutex.hpp>
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

namespace journal {
class Journal;
}  // namespace journal

// DflyCmd is responsible for managing replication. A master instance can be connected
// to many replica instances, what is more, each of them can open multiple connections.
// This is why its important to understand replica lifecycle management before making
// any crucial changes.
//
// A SyncInfo instance is responsible for managing a replica's state and is accessible by its
// sync_id. Each per-thread connection is called a Flow and is represented by the FlowInfo
// instance, accessible by its index.
//
// An important aspect is synchronization and efficient locking. Two levels of locking are used:
//  1. Global locking.
//    Member  mutex `mu_` is used for synchronizing operations connected with internal data
//    structures.
//  2. Per-replica locking
//    SyncInfo contains a separate mutex that is used for replica-only routines. It is held during
//    state transitions (start full sync, start stable state sync), cancellation and member access.
//
// Upon first connection from the replica, a new SyncInfo is created.
// It tranistions through the following phases:
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
//      We obtain the SyncInfo lock, transition into the cancelled state and cancel the context.
//    2. Joining tasks
//      Running tasks will stop on receiving the cancellation flag. Each FlowInfo has also an
//      optional cleanup handler, that is invoked after cancelling. This should allow recovering
//      from any state. The flows task will be awaited and joined if present.
//    3. Unlocking the mutex
//      Now that all tasks have finished and all cleanup handlers have run, we can safely release
//      the per-replica mutex, so that all OnClose handlers will unblock and  internal resources
//      will be released by dragonfly. Then the SyncInfo is removed from the global map.
//
//
class DflyCmd {
 public:
  // See header comments for state descriptions.
  enum class SyncState { PREPARATION, FULL_SYNC, STABLE_SYNC, CANCELLED };

  // Stores information related to a single flow.
  struct FlowInfo {
    FlowInfo() = default;
    FlowInfo(facade::Connection* conn, const std::string& eof_token)
        : conn{conn}, eof_token{eof_token} {};

    // Try closing socket for cleanup.
    void TryCloseSocket();

    facade::Connection* conn;

    std::unique_ptr<RdbSaver> saver;  // Saver used by the full sync phase.
    std::string eof_token;

    std::function<void()> cleanup;   // Optional cleanup for cancellation.
    ::boost::fibers::fiber task_fb;  // Current running task.
  };

  // Stores information related to a single replica.
  struct SyncInfo {
    SyncInfo(unsigned flow_count, Context::ErrHandler err_handler)
        : state{SyncState::PREPARATION}, cntx{std::move(err_handler)}, flows{flow_count} {
    }

    SyncState state;
    Context cntx;

    std::vector<FlowInfo> flows;
    ::boost::fibers::mutex mu;  // See top of header for locking levels.
  };

 public:
  DflyCmd(util::ListenerInterface* listener, ServerFamily* server_family);

  void Run(CmdArgList args, ConnectionContext* cntx);

  void OnClose(ConnectionContext* cntx);

  // Stop all background processes so we can exit in orderly manner.
  void BreakOnShutdown();

  // Create new sync session.
  uint32_t CreateSyncSession();

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

  // EXPIRE
  // Check all keys for expiry.
  void Expire(CmdArgList args, ConnectionContext* cntx);

  // Start full sync in thread. Start FullSyncFb. Called for each flow.
  facade::OpStatus StartFullSyncInThread(FlowInfo* flow, Context* cntx, EngineShard* shard);

  // Stop full sync in thread. Run state switch cleanup.
  void StopFullSyncInThread(FlowInfo* flow, EngineShard* shard);

  // Start stable sync in thread. Called for each flow.
  facade::OpStatus StartStableSyncInThread(FlowInfo* flow, EngineShard* shard);

  // Fiber that runs full sync for each flow.
  void FullSyncFb(FlowInfo* flow, Context* cntx);

  // Main entrypoint for stopping replication.
  void StopReplication(uint32_t sync_id);

  // Transition into cancelled state, run cleanup.
  void CancelSyncSession(uint32_t sync_id, std::shared_ptr<SyncInfo> sync_info);

  // Get SyncInfo by sync_id.
  std::shared_ptr<SyncInfo> GetSyncInfo(uint32_t sync_id);

  // Find sync info by id or send error reply.
  std::pair<uint32_t, std::shared_ptr<SyncInfo>> GetSyncInfoOrReply(std::string_view id,
                                                                    facade::RedisReplyBuilder* rb);

  bool CheckReplicaStateOrReply(const SyncInfo& si, SyncState expected,
                                facade::RedisReplyBuilder* rb);

 private:
  ServerFamily* sf_;

  util::ListenerInterface* listener_;
  TxId journal_txid_ = 0;

  uint32_t next_sync_id_ = 1;
  absl::btree_map<uint32_t, std::shared_ptr<SyncInfo>> sync_infos_;

  ::boost::fibers::mutex mu_;  // Guard global operations. See header top for locking levels.
};

}  // namespace dfly
