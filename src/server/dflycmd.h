// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/btree_map.h>

#include <optional>

#include "server/conn_context.h"
#include "server/snapshot.h"

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

class DflyCmd {
  enum class ReplicaState {
    PREPARATION, // Preparation for full sync.
    FULL_SYNC, // Full sync step.
    STABLE_SYNC, // Stable sync step.
  };

  // Stores per-flow data.
  struct ReplicateFlow {
    facade::Connection* conn;
    std::string eof_token;
    // Stores currently active flow fiber if present.
    // Either full (FullSyncFb) or stable sync (StableSyncFb).
    ::boost::fibers::fiber repl_fb;

    RdbSaver* saver;
  };

  // Stores sync info for one replica.
  struct SyncInfo {
    int64_t tx_id = 0;
    int64_t start_time_ns;

    ReplicaState state = ReplicaState::PREPARATION;
    // Generic fiber flag counter.
    // During ReplicaState::FULL_SYNC: how many fibers are still sending static data.
    std::atomic_uint16_t fiber_flag_counter;
    // Per-flow data.
    absl::flat_hash_map<uint32_t, ReplicateFlow> thread_map;
  };

 public:
  using SyncId = uint32_t;

  DflyCmd(util::ListenerInterface* listener, ServerFamily* server_family);

  // Main entrypoint of DlfyCmd.
  void Run(CmdArgList args, ConnectionContext* cntx);

  // Create entry in sync_info and generate id.
  SyncId AllocateSyncSession();

  void OnClose(ConnectionContext* cntx);

  // Stops all background processes so we could exit in orderly manner.
  void BreakOnShutdown();

 private:
  // THREAD [to_thread]
  // Migrate to thread or return current index.
  void Thread(CmdArgList args, ConnectionContext* cntx);

  // FLOW <masterid> <syncid> <threadid>
  // Handshake with flow and save connection data.
  // Sent from each replica flow.
  void Flow(CmdArgList args, ConnectionContext* cntx);

  // SYNC <syncid>
  // Start stable state replication.
  // Sent once from replica coordinator.
  void Sync(CmdArgList args, ConnectionContext* cntx);

  // WATCH <syncid>
  // Atomically switch from full sync replication to stable state.
  // Sent once from replica coordinator.
  void Switch(CmdArgList args, ConnectionContext* cntx);

  // JOURNAL <START/STOP>
  // Start or stop journal file writes.
  void HandleJournal(CmdArgList args, ConnectionContext* cntx);

  // Some threads are not shards but we still need context from them.
  void StartReplInThread(ReplicateFlow* flow);

  // Dispatch FullSyncFb (full sync fiber) and return status.
  facade::OpStatus StartFullSync(ReplicateFlow* flow, EngineShard* shard);

  // The fiber for full sync.
  void FullSyncFb(std::string eof_token, facade::Connection* conn, RdbSaver* saver);

  // Dispatch StableSyncFb (stable sync fiber) and return status.
  facade::OpStatus StartStableSync(ReplicateFlow* flow, EngineShard* shard);

  // The fiber for stable sync.
  void StableSyncFb(ReplicateFlow* flow);

  // Return sync info from argument or respond with error.
  std::optional<std::pair<SyncId, SyncInfo*>> GetSyncInfoOrRespond(std::string_view id,
                                                                   facade::RedisReplyBuilder* rb);

  util::ListenerInterface* listener_;
  ServerFamily* sf_;
  TxId journal_txid_ = 0;

  // Stores sync info per each replica.
  absl::btree_map<SyncId, SyncInfo*> sync_info_;
  SyncId next_sync_id_ = 1;

  ::boost::fibers::mutex mu_; // guards all sync_info_ ops
};

}  // namespace dfly
