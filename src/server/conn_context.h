// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/fixed_array.h>
#include <absl/container/flat_hash_set.h>

#include "acl/acl_commands_def.h"
#include "facade/acl_commands_def.h"
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
  StoredCmd(const CommandId* cid, ArgSlice args, facade::ReplyMode mode = facade::ReplyMode::FULL);

  // Create on top of already filled tightly-packed buffer.
  StoredCmd(std::string&& buffer, const CommandId* cid, ArgSlice args,
            facade::ReplyMode mode = facade::ReplyMode::FULL);

  size_t NumArgs() const;

  size_t UsedMemory() const;

  // Fill the arg list with stored arguments, it should be at least of size NumArgs().
  // Between filling and invocation, cmd should NOT be moved.
  void Fill(absl::Span<std::string_view> args);

  void Fill(CmdArgVec* dest) {
    dest->resize(sizes_.size());
    Fill(absl::MakeSpan(*dest));
  }

  std::string FirstArg() const;

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
    bool read_only = false;

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
    std::string repl_ip_address;
    uint32_t repl_listening_port = 0;
    DflyVersion repl_version = DflyVersion::VER1;
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
  // The state machine implements the above rules. We need to track:
  // 1. If TRACKING is ON and OPTIN
  // 2. Stickiness of CACHING as described above
  //
  // We introduce a monotonic counter called sequence number which we increment only:
  // * On InvokeCmd when we are not Collecting (multi)
  // We introduce another counter called caching_seq_num which is set to seq_num
  // when the users sends a CLIENT CACHING YES command
  // If seq_num == caching_seq_num + 1 then we know that we should Track().
  class ClientTracking {
   public:
    enum Options : uint8_t {
      NONE,   // NO subcommand, that is no OPTIN and no OUTPUT was used when CLIENT TRACKING was
              // called. We track all keys of read commands.
      OPTIN,  // OPTIN was used with CLIENT TRACKING. We only track keys of read commands preceded
              // by CACHING TRUE command.
      OPTOUT  // OPTOUT was used with CLIENT TRACKING. We track all keys of read commands except the
              // ones preceded by a CACHING FALSE command.
    };

    // Sets to true when CLIENT TRACKING is ON
    void SetClientTracking(bool is_on) {
      tracking_enabled_ = is_on;
    }

    // Increment current sequence number
    void IncrementSequenceNumber() {
      ++seq_num_;
    }

    // Set if OPTIN/OPTOUT subcommand is used in CLIENT TRACKING
    void SetOption(Options option) {
      option_ = option;
    }

    void SetNoLoop(bool noloop) {
      noloop_ = noloop;
    }

    // Check if the keys should be tracked. Result adheres to the state machine described above.
    bool ShouldTrackKeys() const;

    // Check only if CLIENT TRACKING is ON
    bool IsTrackingOn() const {
      return tracking_enabled_;
    }

    // Called by CLIENT CACHING YES and caches the current seq_num_
    void SetCachingSequenceNumber(bool is_multi) {
      // We need -1 when we are in multi
      caching_seq_num_ = is_multi && seq_num_ != 0 ? seq_num_ - 1 : seq_num_;
    }

    void ResetCachingSequenceNumber() {
      caching_seq_num_ = 0;
    }

    bool HasOption(Options option) const {
      return option_ == option;
    }

   private:
    // a flag indicating whether the client has turned on client tracking.
    bool tracking_enabled_ = false;
    bool noloop_ = false;
    Options option_ = NONE;
    // sequence number
    size_t seq_num_ = 0;
    size_t caching_seq_num_ = 0;
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
  ConnectionContext(facade::Connection* owner, dfly::acl::UserCredentials cred);
  ConnectionContext(const ConnectionContext* owner, Transaction* tx);

  struct DebugInfo {
    uint32_t shards_count = 0;
    TxClock clock = 0;

    // number of commands in the last exec body.
    unsigned exec_body_len = 0;
  };

  DebugInfo last_command_debug;

  // TODO: to introduce proper accessors.
  Namespace* ns = nullptr;
  Transaction* transaction = nullptr;
  const CommandId* cid = nullptr;

  ConnectionState conn_state;

  DbIndex db_index() const {
    return conn_state.db_index;
  }

  void ChangeSubscription(bool to_add, bool to_reply, CmdArgList args,
                          facade::RedisReplyBuilder* rb);

  void ChangePSubscription(bool to_add, bool to_reply, CmdArgList args,
                           facade::RedisReplyBuilder* rb);
  void UnsubscribeAll(bool to_reply, facade::RedisReplyBuilder* rb);
  void PUnsubscribeAll(bool to_reply, facade::RedisReplyBuilder* rb);
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

  std::vector<unsigned> ChangeSubscriptions(CmdArgList channels, bool pattern, bool to_add,
                                            bool to_reply);
};

}  // namespace dfly
