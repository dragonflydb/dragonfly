// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/btree_map.h>

#include "server/conn_context.h"

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
  struct ReplicateFlow {
    facade::Connection* conn;
    ::boost::fibers::fiber repl_fb;
    std::string eof_token;
  };

  struct SyncInfo {
    int64_t tx_id = 0;
    int64_t start_time_ns;
    absl::flat_hash_map<uint32_t, ReplicateFlow> thread_map;

    // How many connections have still not finished the full sync phase.
    std::atomic_uint16_t full_sync_cnt;
  };

 public:
  DflyCmd(util::ListenerInterface* listener, ServerFamily* server_family);

  void Run(CmdArgList args, ConnectionContext* cntx);

  // Allocated a positive sync session id.
  uint32_t AllocateSyncSession();

  void OnClose(ConnectionContext* cntx);

  // stops all background processes so we could exit in orderly manner.
  void BreakOnShutdown();

 private:
  void HandleJournal(CmdArgList args, ConnectionContext* cntx);

  // This function kicks off the replication asynchronously and exits.
  facade::OpStatus FullSyncInShard(uint32_t syncid, Transaction* t, EngineShard* shard);

  // Some threads are not shards but we still need context from them.
  void StartReplInThread(uint32_t thread_id, uint32_t syncid);

  // The fiber for full sync process.
  void FullSyncFb(std::string eof_token, SyncInfo* si, facade::Connection* conn, RdbSaver* saver);

  util::ListenerInterface* listener_;
  ServerFamily* sf_;
  ::boost::fibers::mutex mu_;
  TxId journal_txid_ = 0;

  absl::btree_map<uint32_t, SyncInfo*> sync_info_;
  uint32_t next_sync_id_ = 1;
};

}  // namespace dfly
