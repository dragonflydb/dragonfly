// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_set.h>

#include "facade/conn_context.h"
#include "server/common_types.h"

namespace dfly {

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

  enum MCGetMask {
    FETCH_CAS_VER = 1,
  };

  // used for memcache set/get commands.
  // For set op - it's the flag value we are storing along with the value.
  // For get op - we use it as a mask of MCGetMask values.
  uint32_t memcache_flag = 0;

  // Lua-script related data.
  struct Script {
    bool is_write = true;

    absl::flat_hash_set<std::string_view> keys;
  };
  std::optional<Script> script_info;
};

class ConnectionContext : public facade::ConnectionContext {
 public:
  ConnectionContext(::io::Sink* stream, facade::Connection* owner)
      : facade::ConnectionContext(stream, owner) {
  }
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
  ConnectionState conn_state;

  DbIndex db_index() const {
    return conn_state.db_index;
  }
};

}  // namespace dfly
