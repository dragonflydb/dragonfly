// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "server/common_types.h"
#include "server/reply_builder.h"

namespace dfly {

class Connection;
class EngineShardSet;

struct StoredCmd {
  const CommandId* descr;
  std::vector<std::string> cmd;

  StoredCmd(const CommandId* d = nullptr) : descr(d) {
  }
};

struct ConnectionState {
  DbIndex db_index = 0;

  enum ExecState { EXEC_INACTIVE, EXEC_COLLECT, EXEC_ERROR };

  ExecState exec_state = EXEC_INACTIVE;
  std::vector<StoredCmd> exec_body;

  enum Mask : uint32_t {
    ASYNC_DISPATCH = 1,  // whether a command is handled via async dispatch.
    CONN_CLOSING = 2,    // could be because of unrecoverable error or planned action.

    // Whether this connection belongs to replica, i.e. a dragonfly slave is connected to this
    // host (master) via this connection to sync from it.
    REPL_CONNECTION = 2,
  };

  uint32_t mask = 0;  // A bitmask of Mask values.

  bool IsClosing() const {
    return mask & CONN_CLOSING;
  }

  bool IsRunViaDispatch() const {
    return mask & ASYNC_DISPATCH;
  }
};

class ConnectionContext : public ReplyBuilder {
 public:
  ConnectionContext(::io::Sink* stream, Connection* owner);

  struct DebugInfo {
    uint32_t shards_count = 0;
    TxClock clock = 0;
    bool is_ooo = false;
  };

  DebugInfo last_command_debug;

  // TODO: to introduce proper accessors.
  Transaction* transaction = nullptr;
  const CommandId* cid = nullptr;
  EngineShardSet* shard_set = nullptr;

  Connection* owner() {
    return owner_;
  }

  Protocol protocol() const;

  DbIndex db_index() const {
    return conn_state.db_index;
  }

  ConnectionState conn_state;

 private:
  Connection* owner_;
};

}  // namespace dfly
