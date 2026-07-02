// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_set.h>

#include "facade/conn_context.h"
#include "facade/parsed_command.h"
#include "facade/reply_mode.h"
#include "server/acl/acl_commands_def.h"
#include "server/common.h"
#include "server/tx_base.h"
#include "server/version.h"

namespace dfly {

class EngineShardSet;
class ChannelStore;
class Interpreter;
struct FlowInfo;

// Non-owning view of a command for the squasher. Can be constructed from StoredCmd or
// CommandContext.
struct CmdRef {
  const CommandId* cid = nullptr;
  facade::ParsedArgs args;
  facade::ReplyMode reply_mode = facade::ReplyMode::FULL;
  CommandContext* cmd_cntx = nullptr;

  bool IsValid() const {
    return cid != nullptr;
  }
};

// Stores command id and arguments for delayed invocation.
// Used for storing MULTI/EXEC commands.
class StoredCmd {
 public:
  // Deep copy of args, creates backing storage internally.
  StoredCmd(const CommandId* cid, const facade::ParsedArgs& args,
            facade::ReplyMode mode = facade::ReplyMode::FULL);

  // Moves args from src via swap. src's BackedArguments will be empty after this.
  // tail_index specifies how many leading args to skip (e.g., 1 to skip the command name).
  StoredCmd(const CommandId* cid, cmn::BackedArguments* src, uint8_t tail_index,
            facade::ReplyMode mode = facade::ReplyMode::FULL);

  size_t NumArgs() const {
    return args_.size();
  }

  size_t UsedMemory() const {
    return backed_ ? backed_->HeapMemory() + sizeof(*backed_) : 0;
  }

  const facade::ParsedArgs& Args() const {
    return args_;
  }
  std::string FirstArg() const;

  const CommandId* Cid() const {
    return cid_;
  }

  facade::ReplyMode ReplyMode() const {
    return reply_mode_;
  }

  CmdRef Ref() const;

 private:
  const CommandId* cid_;     // underlying command
  facade::ParsedArgs args_;  // arguments

  // TODO: we could optimize the storage further by introducing StoredCmdCollection and
  // keep the backing storage there. Then this class will only use shallow copies.
  std::unique_ptr<cmn::BackedArguments> backed_;
  facade::ReplyMode reply_mode_;  // reply mode
};

struct ConnectionState {
  // MULTI-EXEC transaction related data.
  struct ExecInfo {
    enum ExecState : uint8_t { EXEC_INACTIVE, EXEC_COLLECT, EXEC_RUNNING, EXEC_ERROR };

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

    // Empties the body vector and resets stored_cmd_bytes to 0. Returns the size before data was
    // cleared.
    size_t ClearStoredCmds();

    // Returns memory used by the body field without iterating over each stored command
    size_t GetStoredCmdBytes() const {
      return stored_cmd_bytes + body.capacity() * sizeof(StoredCmd);
    }

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

    // The total size of all stored commands kept in "body". Does not include memory allocated by
    // the "body" vector.
    size_t stored_cmd_bytes = 0;
  };

  // Lua-script related data.
  struct ScriptInfo {
    size_t UsedMemory() const;

    absl::flat_hash_set<LockTag> lock_tags;  // declared tags
    std::vector<std::string> key_backing;    // storage for keys provided from lua
    bool read_only = false;

    size_t async_cmds_heap_mem = 0;     // bytes used by async_cmds
    size_t async_cmds_heap_limit = 0;   // max bytes allowed for async_cmds
    std::vector<StoredCmd> async_cmds;  // aggregated by acall

    struct Stats {
      char sha[40];                            // sha of script
      unsigned num_commands = 0;               // total number of command executed
      std::atomic_uint32_t slow_commands = 0;  // commands that made it into slowlog

      uint8_t tx_mode = 0;     // value of Transaction::MultiMode
      unsigned tx_shards = 0;  // Number of shards on the transaction
    } stats;
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
  };

  struct SquashingInfo {
    // Pointer to the original underlying context of the base command.
    // Only const access it possible for reading from multiple threads,
    // each squashing thread has its own proxy context that contains this info.
    const ConnectionContext* owner = nullptr;
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
      caching_seq_num_ = 1;
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
    size_t caching_seq_num_ = 1;
  };

 public:
  DbIndex db_index = 0;

  ExecInfo exec_info;
  ReplicationInfo replication_info;

  std::unique_ptr<ScriptInfo> script_info;
  std::unique_ptr<SubscribeInfo> subscribe_info;
  ClientTracking tracking_info_;
};

class ConnectionContext : public facade::ConnectionContext {
 public:
  ConnectionContext(facade::Connection* owner, dfly::acl::UserCredentials cred);

  // Per-client introspection about the most recent command executed on this
  // connection (akin to the info Redis exposes via CLIENT INFO). Captured live
  // during command execution because the underlying Transaction is per-command
  // and freed once dispatch returns.
  struct LastCommandStats {
    uint32_t shards_count = 0;  // unique shards touched by the command's transaction
    TxClock clock = 0;          // transaction id (txid) of the command
  };

  LastCommandStats last_cmd_stats;

  // TODO: to introduce proper accessors.
  Namespace* ns = nullptr;
  Transaction* transaction = nullptr;

  ConnectionState conn_state;

  DbIndex db_index() const {
    return conn_state.db_index;
  }

  void ChangeSubscription(bool to_add, bool to_reply, bool sharded, const facade::ParsedArgs& args,
                          facade::RedisReplyBuilder* rb);

  void ChangePSubscription(bool to_add, bool to_reply, const facade::ParsedArgs& args,
                           facade::RedisReplyBuilder* rb);
  void UnsubscribeAll(bool to_reply, facade::RedisReplyBuilder* rb);
  void PUnsubscribeAll(bool to_reply, facade::RedisReplyBuilder* rb);
  void ChangeMonitor(bool start);  // either start or stop monitor on a given connection

  size_t UsedMemory() const override;

  void Unsubscribe(std::string_view channel) override;
  void OnSocketError(uint32_t epoll_mask) override;

  // Whether this connection is a connection from a replica to its master.
  // This flag is true only on replica side, where we need to setup a special ConnectionContext
  // instance that helps applying commands coming from master.
  bool is_replicating = false;

  bool monitor = false;  // when a monitor command is sent over a given connection, we need to aware
                         // of it as a state for the connection
  bool journal_emulated = false;  // whether it is used to dispatch journal commands

  // Reference to a master-side FlowInfo for this connection if it is a replication connection.
  FlowInfo* master_repl_flow = nullptr;

  // The related connection is bound to main listener or serves the memcached protocol
  bool has_main_or_memcache_listener = false;

  // ACLs.
  // The following variables represent the ACL rules of the context.
  // Each command, before run, is authorized against those rules by
  // IsUserAllowedToInvokeCmd(and variants) in validator.cc

  // Username
  std::string authed_username{"default"};

  // Each entry in the list is a bitfield representing a specific command family,
  // where each bit corresponds to an individual command within that family.
  // Together, these entries encode the user's full ACL to commands.
  // The index 'i' in 'acl_commands[i]' refers to the command family based on
  // its registration order at runtime. For more details, see acl_commands_def.h.
  std::vector<uint64_t> acl_commands;

  // Keyspace. Each key referenced in a command must match (any) of the rules (globs).
  dfly::acl::AclKeys keys;

  // Pub/sub channels. Each channel referenced in a command must match (any) of the rules (globs).
  dfly::acl::AclPubSub pub_sub;

  // db index, std::numeric_limits<size_t>::max for ALL db's. Dragonfly specific extension.
  size_t acl_db_idx = std::numeric_limits<size_t>::max();

  // Skip ACL validation, used by internal commands and commands run on admin port
  bool skip_acl_validation = false;

 private:
  void EnableMonitoring(bool enable) {
    subscriptions++;  // required to support the monitoring
    monitor = enable;
  }

  std::vector<unsigned> ChangeSubscriptions(facade::ParsedArgs channels, bool pattern, bool to_add,
                                            bool to_reply);
};

class CommandContext : public facade::ParsedCommand {
 public:
  CommandContext() = default;
  CommandContext(facade::SinkReplyBuilder* rb, facade::ConnectionContext* conn_cntx) {
    Init(rb, conn_cntx);
  }

  void SetupTx(const CommandId* cid, Transaction* tx) {
    cid_ = cid;
    tx_ = tx;
  }

  void UpdateCid(const CommandId* cid) {
    cid_ = cid;
  }

  virtual size_t GetSize() const override {
    return sizeof(CommandContext);
  }

  ConnectionContext* server_conn_cntx() const {
    return static_cast<ConnectionContext*>(conn_cntx_);
  }

  void RecordLatency(const facade::ParsedArgs& tail_args) const;

  facade::Connection* conn() const {
    return conn_cntx_->conn();
  }

  facade::SinkReplyBuilder* SwapReplier(facade::SinkReplyBuilder* new_rb) {
    return std::exchange(rb_, new_rb);
  }

  Transaction* tx() const {
    return tx_;
  }

  const CommandId* cid() const {
    return cid_;
  }

  void SetTailArgs(facade::ParsedArgs args) {
    tail_args_ = args;
  }

  const facade::ParsedArgs& tail_args() const {
    return tail_args_;
  }

  uint64_t start_time_usec = 0;

 protected:
  void ReuseInternal() final;

  // Command arguments without the command name. Points into BackedArguments owned by this
  // CommandContext (or StoredCmd for EXEC), so it survives async execution.
  facade::ParsedArgs tail_args_;

  Transaction* tx_ = nullptr;
  const CommandId* cid_ = nullptr;
};

// 320 is a mi_good_size boundary.
// The previous boundary of 256 would require making backed_args buffers much smaller
static_assert(sizeof(CommandContext) == 320);

}  // namespace dfly
