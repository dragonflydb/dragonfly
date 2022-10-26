// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/btree_map.h>
#include <boost/fiber/fiber.hpp>

#include <memory.h>

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

class DflyCmd {
 public:
  enum class SyncState { PREPARATION, FULL_SYNC, CANCELLED };

  struct FlowInfo {
    FlowInfo() = default;
    FlowInfo(facade::Connection* conn, const std::string& eof_token)
        : conn(conn), eof_token(eof_token){};

    facade::Connection* conn;
    std::string eof_token;

    std::unique_ptr<RdbSaver> saver;

    ::boost::fibers::fiber fb;
  };

  struct SyncInfo {
    SyncState state = SyncState::PREPARATION;

    std::vector<FlowInfo> flows;

    ::boost::fibers::mutex mu; // guard operations on replica.
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

  // SYNC <masterid> <syncid> <flowid>
  // Migrate connection to required flow thread.
  // Stub: will be replcaed with full sync.
  void Sync(CmdArgList args, ConnectionContext* cntx);

  // EXPIRE
  // Check all keys for expiry.
  void Expire(CmdArgList args, ConnectionContext* cntx);

  // Start full sync in thread. Start FullSyncFb. Called for each flow.
  facade::OpStatus StartFullSyncInThread(FlowInfo* flow, EngineShard* shard);

  // Fiber that runs full sync for each flow.
  void FullSyncFb(FlowInfo* flow);

  // Unregister flow. Must be called when flow disconnects.
  void UnregisterFlow(FlowInfo*);

  // Delete sync session. Cleanup flows.
  void DeleteSyncSession(uint32_t sync_id);

  // Get SyncInfo by sync_id.
  std::shared_ptr<SyncInfo> GetSyncInfo(uint32_t sync_id);

  // Find sync info by id or send error reply.
  std::pair<uint32_t, std::shared_ptr<SyncInfo>> GetSyncInfoOrReply(std::string_view id,
                                                                    facade::RedisReplyBuilder* rb);

  ServerFamily* sf_;

  util::ListenerInterface* listener_;
  TxId journal_txid_ = 0;

  absl::btree_map<uint32_t, std::shared_ptr<SyncInfo>> sync_infos_;
  uint32_t next_sync_id_ = 1;

  ::boost::fibers::mutex mu_;  // guard sync info and journal operations.
};

}  // namespace dfly
