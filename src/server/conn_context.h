// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_set.h>

#include "facade/conn_context.h"
#include "server/common.h"
#include "util/fibers/fibers_ext.h"

namespace dfly {

class EngineShardSet;
class ConnectionContext;

struct StoredCmd {
  const CommandId* descr;

 private:
  std::unique_ptr<char[]> backing_storage_;
  CmdArgVec arg_vec_;
  CmdArgList arg_list_;

 public:
  StoredCmd(const CommandId* d, CmdArgList args);

  CmdArgList ArgList() const {
    return arg_list_;
  }

  void Invoke(ConnectionContext* ctx);
};

struct ConnectionState {
  // MULTI-EXEC transaction related data.
  struct ExecInfo {
    enum ExecState { EXEC_INACTIVE, EXEC_COLLECT, EXEC_ERROR };

    ExecInfo() = default;
    // ExecInfo is immovable due to being referenced from DbSlice.
    ExecInfo(ExecInfo&&) = delete;

    // Return true if ExecInfo is active (after MULTI)
    bool IsActive() {
      return state != EXEC_INACTIVE;
    }

    // Resets to blank state after EXEC or DISCARD
    void Clear();

    // Resets local watched keys info. Does not unregister the keys from DbSlices.
    void ClearWatched();

    ExecState state = EXEC_INACTIVE;
    std::vector<StoredCmd> body;
    // List of keys registered with WATCH
    std::vector<std::pair<DbIndex, std::string>> watched_keys;
    // Set if a watched key was changed before EXEC
    std::atomic_bool watched_dirty = false;
    // Number of times watch was called on an existing key.
    uint32_t watched_existed = 0;
  };

  // Lua-script related data.
  struct ScriptInfo {
    bool is_write = true;
    absl::flat_hash_set<std::string_view> keys;
  };

  // PUB-SUB messaging related data.
  struct SubscribeInfo {
    bool IsEmpty() const {
      return channels.empty() && patterns.empty();
    }

    unsigned SubscriptionCount() const {
      return channels.size() + patterns.size();
    }

    // TODO: to provide unique_strings across service. This will allow us to use string_view here.
    absl::flat_hash_set<std::string> channels;
    absl::flat_hash_set<std::string> patterns;

    util::fibers_ext::BlockingCounter borrow_token{0};
  };

  struct ReplicationInfo {
    // If this server is master, and this connection is from a secondary replica,
    // then it holds positive sync session id.
    uint32_t repl_session_id = 0;
    uint32_t repl_flow_id = kuint32max;
    uint32_t repl_listening_port = 0;
  };

  enum MCGetMask {
    FETCH_CAS_VER = 1,
  };

  DbIndex db_index = 0;

  // used for memcache set/get commands.
  // For set op - it's the flag value we are storing along with the value.
  // For get op - we use it as a mask of MCGetMask values.
  uint32_t memcache_flag = 0;

  ExecInfo exec_info;
  ReplicationInfo replicaiton_info;

  std::optional<ScriptInfo> script_info;
  std::unique_ptr<SubscribeInfo> subscribe_info;
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
  ConnectionState conn_state;

  DbIndex db_index() const {
    return conn_state.db_index;
  }

  void ChangeSubscription(bool to_add, bool to_reply, CmdArgList args);
  void ChangePSub(bool to_add, bool to_reply, CmdArgList args);
  void UnsubscribeAll(bool to_reply);
  void PUnsubscribeAll(bool to_reply);
  void ChangeMonitor(bool start);  // either start or stop monitor on a given connection

  bool is_replicating = false;
  bool monitor = false;  // when a monitor command is sent over a given connection, we need to aware
                         // of it as a state for the connection

 private:
  void EnableMonitoring(bool enable) {
    force_dispatch = enable;  // required to support the monitoring
    monitor = enable;
  }
  void SendSubscriptionChangedResponse(std::string_view action,
                                       std::optional<std::string_view> topic, unsigned count);
};

}  // namespace dfly
