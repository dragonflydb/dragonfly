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

namespace journal {
class Journal;
}  // namespace journal

class DflyCmd {
 public:
  DflyCmd(util::ListenerInterface* listener, ServerFamily* server_family);

  void Run(CmdArgList args, ConnectionContext* cntx);

  uint32_t AllocateSyncSession();

  void OnClose(ConnectionContext* cntx);

  // stops all background processes so we could exit in orderly manner.
  void BreakOnShutdown();

 private:
  void HandleJournal(CmdArgList args, ConnectionContext* cntx);

  util::ListenerInterface* listener_;
  ServerFamily* sf_;
  ::boost::fibers::mutex mu_;
  TxId journal_txid_ = 0;

  struct SyncInfo {
    int64_t tx_id = 0;
  };

  absl::btree_map<uint32_t, SyncInfo> sync_info_;
  uint32_t next_sync_id_ = 1;
};

}  // namespace dfly
