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

class DflyCmd {
 public:
  enum class SyncState { PREPARATION, FULL_SYNC, STABLE_SYNC, CANCELLED };

  struct FlowInfo {
    FlowInfo() = default;
    FlowInfo(facade::Connection* conn, const std::string& eof_token)
        : conn{conn}, eof_token{eof_token}, cleanup{} {};

    facade::Connection* conn;
    std::string eof_token;

    std::unique_ptr<RdbSaver> saver;

    std::function<void()> cleanup;
    ::boost::fibers::fiber fb;
  };

  struct SyncInfo {
    SyncInfo(unsigned flow_count, Context::ErrHandler err_handler)
        : state{SyncState::PREPARATION}, flows{flow_count}, cntx{std::move(err_handler)} {
    }

    SyncState state;
    std::vector<FlowInfo> flows;
    Context cntx;
    ::boost::fibers::mutex mu;  // guard operations on replica.
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
  struct OnHoldHandle : public ::boost::fibers::mutex {};

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

  // Start stable sync in thread. Called for each flow.
  facade::OpStatus StartStableSyncInThread(FlowInfo* flow, EngineShard* shard);

  // Fiber that runs full sync for each flow.
  void FullSyncFb(FlowInfo* flow, Context* cntx);

  void StopReplication(uint32_t sync_id, bool lock_mutex = true);

  // Delete sync session.
  std::pair<std::shared_ptr<SyncInfo>, std::function<void()>> TransferHoldSyncSession(uint32_t sync_id);

  // Cancel context and cleanup flows.
  void CancelSyncSession(std::shared_ptr<SyncInfo> sync_info);

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
  absl::btree_map<uint32_t, std::shared_ptr<OnHoldHandle>> connection_holds_;


  ::boost::fibers::mutex mu_;  // guard sync info and journal operations.
};

}  // namespace dfly
