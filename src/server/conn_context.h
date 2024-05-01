// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/fixed_array.h>
#include <absl/container/flat_hash_set.h>

#include "acl/acl_commands_def.h"
#include "facade/conn_context.h"
#include "facade/reply_capture.h"
#include "server/common.h"
#include "server/tx_base.h"
#include "server/version.h"

namespace dfly {

class EngineShardSet;
class ConnectionContext;
class ChannelStore;
class Interpreter;
struct FlowInfo;

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

  size_t UsedMemory() const;

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
    enum ExecState { EXEC_INACTIVE, EXEC_COLLECT, EXEC_RUNNING, EXEC_ERROR };

    ExecInfo() = default;
    // ExecInfo is immovable due to being referenced from DbSlice.
    ExecInfo(ExecInfo&&) = delete;

    bool IsCollecting() const {
      return state == EXEC_COLLECT;
    }

    bool IsRunning() const {
      return state == EXEC_RUNNING;
    }

    // Resets to blank state after EXEC or DISCARD
    void Clear();

    // Resets local watched keys info. Does not unregister the keys from DbSlices.
    void ClearWatched();

    size_t UsedMemory() const;

    ExecState state = EXEC_INACTIVE;
    std::vector<StoredCmd> body;
    bool is_write = false;

    std::vector<std::pair<DbIndex, std::string>> watched_keys;  // List of keys registered by WATCH
    std::atomic_bool watched_dirty = false;  // Set if a watched key was changed before EXEC
    uint32_t watched_existed = 0;            // Number of times watch was called on an existing key

    // If the transaction contains EVAL calls, preborrow an interpreter that will be used for all of
    // them. This has to be done to avoid potentially blocking when borrowing interpreters amid
    // executing the multi transaction, which can create deadlocks by blocking other transactions
    // that already borrowed all available interpreters but wait for keys to be unlocked.
    Interpreter* preborrowed_interpreter = nullptr;
  };

  // Lua-script related data.
  struct ScriptInfo {
    size_t UsedMemory() const;

    absl::flat_hash_set<LockTag> lock_tags;  // declared tags

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

    size_t UsedMemory() const;

    // TODO: to provide unique_strings across service. This will allow us to use string_view here.
    absl::flat_hash_set<std::string> channels;
    absl::flat_hash_set<std::string> patterns;
  };

  struct ReplicationInfo {
    // If this server is master, and this connection is from a secondary replica,
    // then it holds positive sync session id.
    uint32_t repl_session_id = 0;
    uint32_t repl_flow_id = UINT32_MAX;
    uint32_t repl_listening_port = 0;
    DflyVersion repl_version = DflyVersion::VER0;
  };

  struct SquashingInfo {
    // Pointer to the original underlying context of the base command.
    // Only const access it possible for reading from multiple threads,
    // each squashing thread has its own proxy context that contains this info.
    const ConnectionContext* owner = nullptr;
  };

  enum MCGetMask {
    FETCH_CAS_VER = 1,
  };

  size_t UsedMemory() const;

  // Client tracking is a per-connection state machine that adheres to the requirements
  // of the CLIENT TRACKING command. Note that the semantics described below are enforced
  // by the tests in server_family_test. The rules are:
  // 1. If CLIENT TRACKING is ON then each READ command must be tracked. Invalidation
  //    messages are sent `only once`. Subsequent changes of the same key require the
  //    client to re-read the key in order to receive the next invalidation message.
  // 2. CLIENT TRACKING ON OPTIN turns on optional tracking. Read commands are not
  //    tracked unless the client issues a CLIENT CACHING YES command which conditionally
  //    allows the tracking of the command that follows CACHING YES). For example:
  //    >> CLIENT TRACKING ON
  //    >> CLIENT CACHING YES
  //    >> GET foo  <--------------------- From now foo is being tracked
  //    However:
  //    >> CLIENT TRACKING ON
  //    >> CLIENT CACHING YES
  //    >> SET foo bar
  //    >> GET foo <--------------------- is *NOT* tracked since GET does not succeed CACHING
  //    Also, in the context of multi transactions, CLIENT CACHING YES is *STICKY*:
  //    >> CLIENT TRACKING ON
  //    >> CLIENT CACHING YES
  //    >> MULTI
  //    >>   GET foo
  //    >>   SET foo bar
  //    >>   GET brother_foo
  //    >> EXEC
  //    From this point onwards `foo` and `get` keys are tracked. Same aplies if CACHING YES
  //    is used within the MULTI/EXEC block.
  //
  // The state machine implements the above rules. We need to track two commands at each time:
  // 1. The command invoked previously.
  // 2. The command that is invoked now (via InvokeCmd).
  // Which is tracked by current_command_ and prev_command_ respectively. When CACHING YES
  // is invoked the current_command_ is set to true which is later moved to the prev_command_
  // when the next command is invoked. This is needed to keep track of the different rules
  // described above. Stickiness is covered similarly by the multi/exec/discard command which
  // when called sets the corresponding multi_ variable to true.
  class ClientTracking {
   public:
    // Sets to true when CLIENT TRACKING is ON
    void SetClientTracking(bool is_on);
    // Enable tracking on the client
    void TrackClientCaching();

    void UpdatePrevAndLastCommand();
    // Set if OPTIN subcommand is used in CLIENT TRACKING
    void SetOptin(bool optin);
    // When Multi command is invoked, it calls this to broadcast that we are on a multi
    // transaction.
    void SetMulti(bool multi);

    // Check if the keys should be tracked. Result adheres to the state machine described above.
    bool ShouldTrackKeys() const;
    // Check only if CLIENT TRACKING is ON
    bool IsTrackingOn() const;

    // Iterates over the active shards of the transaction. If a key satisfies
    // the tracking requirements, is is set for tracking.
    void Track(ConnectionContext* cntx, const CommandId* cid);

   private:
    // a flag indicating whether the client has turned on client tracking.
    bool tracking_enabled_ = false;
    bool optin_ = false;
    // remember if CLIENT CACHING TRUE was the last command
    // true if the previous command invoked is CLIENT CACHING TRUE
    bool prev_command_ = false;
    // true if the currently executing command is CLIENT CACHING TRUE
    bool executing_command_ = false;
    // true if we are in a multi transaction
    bool multi_ = false;
  };

 public:
  DbIndex db_index = 0;

  // used for memcache set/get commands.
  // For set op - it's the flag value we are storing along with the value.
  // For get op - we use it as a mask of MCGetMask values.
  uint32_t memcache_flag = 0;

  ExecInfo exec_info;
  ReplicationInfo replication_info;

  std::optional<SquashingInfo> squashing_info;
  std::unique_ptr<ScriptInfo> script_info;
  std::unique_ptr<SubscribeInfo> subscribe_info;
  ClientTracking tracking_info_;
};

class ConnectionContext : public facade::ConnectionContext {
 public:
  ConnectionContext(::io::Sink* stream, facade::Connection* owner);

  ConnectionContext(ConnectionContext* owner, Transaction* tx, facade::CapturingReplyBuilder* crb);

  struct DebugInfo {
    uint32_t shards_count = 0;
    TxClock clock = 0;

    // number of commands in the last exec body.
    unsigned exec_body_len = 0;
  };

  DebugInfo last_command_debug;

  // TODO: to introduce proper accessors.
  Transaction* transaction = nullptr;
  const CommandId* cid = nullptr;
  ConnectionContext* parent_cntx_ = nullptr;

  ConnectionState::ClientTracking& ClientTrackingInfo();

  ConnectionState conn_state;

  DbIndex db_index() const {
    return conn_state.db_index;
  }

  void ChangeSubscription(bool to_add, bool to_reply, CmdArgList args);
  void ChangePSubscription(bool to_add, bool to_reply, CmdArgList args);
  void UnsubscribeAll(bool to_reply);
  void PUnsubscribeAll(bool to_reply);
  void ChangeMonitor(bool start);  // either start or stop monitor on a given connection

  size_t UsedMemory() const override;

  // Whether this connection is a connection from a replica to its master.
  // This flag is true only on replica side, where we need to setup a special ConnectionContext
  // instance that helps applying commands coming from master.
  bool is_replicating = false;

  bool monitor = false;  // when a monitor command is sent over a given connection, we need to aware
                         // of it as a state for the connection

  // Reference to a FlowInfo for this connection if from a master to a replica.
  FlowInfo* replication_flow = nullptr;

 private:
  void EnableMonitoring(bool enable) {
    subscriptions++;  // required to support the monitoring
    monitor = enable;
  }

  void SendSubscriptionChangedResponse(std::string_view action,
                                       std::optional<std::string_view> topic, unsigned count);
};

}  // namespace dfly
