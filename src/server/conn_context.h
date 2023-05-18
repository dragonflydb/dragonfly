// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/fixed_array.h>
#include <absl/container/flat_hash_set.h>

#include "core/fibers.h"
#include "facade/conn_context.h"
#include "facade/reply_capture.h"
#include "server/common.h"
#include "server/version.h"

namespace dfly {

class EngineShardSet;
class ConnectionContext;
class ChannelStore;
class FlowInfo;

// Stores command id and arguments for delayed invocation.
// Used for storing MULTI/EXEC commands.
class StoredCmd {
 public:
  StoredCmd(const CommandId* cid, CmdArgList args,
            facade::ReplyMode mode = facade::ReplyMode::FULL);

  // Create on top of already filled tightly-packed buffer.
  StoredCmd(std::string&& buffer, const CommandId* cid, CmdArgList args,
            facade::ReplyMode mode = facade::ReplyMode::FULL);

  size_t NumArgs() const;

  size_t UsedHeapMemory() const;

  // Fill the arg list with stored arguments, it should be at least of size NumArgs().
  // Between filling and invocation, cmd should NOT be moved.
  void Fill(CmdArgList args);

  void Fill(CmdArgVec* dest) {
    dest->resize(sizes_.size());
    Fill(absl::MakeSpan(*dest));
  }

  const CommandId* Cid() const;

  facade::ReplyMode ReplyMode() const;

 private:
  const CommandId* cid_;                 // underlying command
  std::string buffer_;                   // underlying buffer
  absl::FixedArray<uint32_t, 4> sizes_;  // sizes of arg part
  facade::ReplyMode reply_mode_;         // reply mode
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
    absl::flat_hash_set<std::string_view> keys;  // declared keys

    size_t async_cmds_heap_mem = 0;     // bytes used by async_cmds
    size_t async_cmds_heap_limit = 0;   // max bytes allowed for async_cmds
    std::vector<StoredCmd> async_cmds;  // aggregated by acall
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

    BlockingCounter borrow_token{0};
  };

  struct ReplicationInfo {
    // If this server is master, and this connection is from a secondary replica,
    // then it holds positive sync session id.
    uint32_t repl_session_id = 0;
    uint32_t repl_flow_id = UINT32_MAX;
    uint32_t repl_listening_port = 0;
    DflyVersion repl_version = DflyVersion::VER0;
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

  std::unique_ptr<ScriptInfo> script_info;
  std::unique_ptr<SubscribeInfo> subscribe_info;
};

class ConnectionContext : public facade::ConnectionContext {
 public:
  ConnectionContext(::io::Sink* stream, facade::Connection* owner)
      : facade::ConnectionContext(stream, owner) {
  }

  ConnectionContext(Transaction* tx, facade::CapturingReplyBuilder* crb)
      : facade::ConnectionContext(nullptr, nullptr), transaction{tx} {
    delete Inject(crb);  // deletes the previous reply builder.
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
  void ChangePSubscription(bool to_add, bool to_reply, CmdArgList args);
  void UnsubscribeAll(bool to_reply);
  void PUnsubscribeAll(bool to_reply);
  void ChangeMonitor(bool start);  // either start or stop monitor on a given connection

  // Whether this connection is a connection from a replica to its master.
  bool is_replicating = false;
  // Reference to a FlowInfo for this connection if from a master to a replica.
  FlowInfo* replication_flow;
  bool monitor = false;  // when a monitor command is sent over a given connection, we need to aware
                         // of it as a state for the connection

 private:
  void EnableMonitoring(bool enable) {
    subscriptions++;  // required to support the monitoring
    monitor = enable;
  }
  void SendSubscriptionChangedResponse(std::string_view action,
                                       std::optional<std::string_view> topic, unsigned count);
};

}  // namespace dfly
