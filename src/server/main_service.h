// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "base/varz_value.h"
#include "core/interpreter.h"
#include "facade/service_interface.h"
#include "server/cluster/cluster_family.h"
#include "server/command_registry.h"
#include "server/config_registry.h"
#include "server/engine_shard_set.h"
#include "server/server_family.h"

namespace util {
class AcceptServer;
}  // namespace util

namespace dfly {

class Interpreter;
using facade::MemcacheParser;

class Service : public facade::ServiceInterface {
 public:
  struct InitOpts {
    bool disable_time_update;

    InitOpts() : disable_time_update{false} {
    }
  };

  explicit Service(util::ProactorPool* pp);
  ~Service();

  void Init(util::AcceptServer* acceptor, std::vector<facade::Listener*> listeners,
            const InitOpts& opts = InitOpts{});

  void Shutdown();

  // Prepare command execution, verify and execute, reply to context
  void DispatchCommand(CmdArgList args, facade::ConnectionContext* cntx) final;

  // Check VerifyCommandExecution and invoke command with args
  bool InvokeCmd(const CommandId* cid, CmdArgList tail_args, ConnectionContext* reply_cntx,
                 bool record_stats = false);

  // Verify command can be executed now (check out of memory), always called immediately before
  // execution
  std::optional<facade::ErrorReply> VerifyCommandExecution(const CommandId* cid);

  // Verify command prepares excution in correct state.
  // It's usually called before command execution. Only for multi/exec transactions it's checked
  // when the command is queued for execution, not before the execution itself.
  std::optional<facade::ErrorReply> VerifyCommandState(const CommandId* cid, CmdArgList tail_args,
                                                       const ConnectionContext& cntx);

  void DispatchMC(const MemcacheParser::Command& cmd, std::string_view value,
                  facade::ConnectionContext* cntx) final;

  facade::ConnectionContext* CreateContext(util::FiberSocketBase* peer,
                                           facade::Connection* owner) final;

  facade::ConnectionStats* GetThreadLocalConnectionStats() final;

  const CommandId* FindCmd(std::string_view cmd) const {
    return registry_.Find(cmd);
  }

  facade::ErrorReply ReportUnknownCmd(std::string_view cmd_name);

  // Returns: the new state.
  // if from equals the old state then the switch is performed "to" is returned.
  // Otherwise, does not switch and returns the current state in the system.
  // Upon switch, updates cached global state in threadlocal ServerState struct.
  GlobalState SwitchState(GlobalState from, GlobalState to);

  GlobalState GetGlobalState() const;

  void ConfigureHttpHandlers(util::HttpListenerBase* base) final;
  void OnClose(facade::ConnectionContext* cntx) final;
  std::string GetContextInfo(facade::ConnectionContext* cntx) final;

  uint32_t shard_count() const {
    return shard_set->size();
  }

  // Used by tests.
  bool IsLocked(DbIndex db_index, std::string_view key) const;
  bool IsShardSetLocked() const;

  util::ProactorPool& proactor_pool() {
    return pp_;
  }

  absl::flat_hash_map<std::string, unsigned> UknownCmdMap() const;

  ScriptMgr* script_mgr() {
    return server_family_.script_mgr();
  }

  ServerFamily& server_family() {
    return server_family_;
  }

 private:
  static void Quit(CmdArgList args, ConnectionContext* cntx);
  static void Multi(CmdArgList args, ConnectionContext* cntx);

  static void Watch(CmdArgList args, ConnectionContext* cntx);
  static void Unwatch(CmdArgList args, ConnectionContext* cntx);

  void Discard(CmdArgList args, ConnectionContext* cntx);
  void Eval(CmdArgList args, ConnectionContext* cntx);
  void EvalSha(CmdArgList args, ConnectionContext* cntx);
  void Exec(CmdArgList args, ConnectionContext* cntx);
  void Publish(CmdArgList args, ConnectionContext* cntx);
  void Subscribe(CmdArgList args, ConnectionContext* cntx);
  void Unsubscribe(CmdArgList args, ConnectionContext* cntx);
  void PSubscribe(CmdArgList args, ConnectionContext* cntx);
  void PUnsubscribe(CmdArgList args, ConnectionContext* cntx);
  void Function(CmdArgList args, ConnectionContext* cntx);
  void Monitor(CmdArgList args, ConnectionContext* cntx);
  void Pubsub(CmdArgList args, ConnectionContext* cntx);
  void Command(CmdArgList args, ConnectionContext* cntx);

  void PubsubChannels(std::string_view pattern, ConnectionContext* cntx);
  void PubsubPatterns(ConnectionContext* cntx);

  struct EvalArgs {
    std::string_view sha;  // only one of them is defined.
    CmdArgList keys, args;
  };

  // Return error if not all keys are owned by the server when running in cluster mode
  std::optional<facade::ErrorReply> CheckKeysOwnership(const CommandId* cid, CmdArgList args,
                                                       const ConnectionContext& dfly_cntx);

  const CommandId* FindCmd(CmdArgList args) const;

  void EvalInternal(const EvalArgs& eval_args, Interpreter* interpreter, ConnectionContext* cntx);

  // Return optional payload - first received error that occured when executing commands.
  std::optional<facade::CapturingReplyBuilder::Payload> FlushEvalAsyncCmds(ConnectionContext* cntx,
                                                                           bool force = false);

  void CallFromScript(ConnectionContext* cntx, Interpreter::CallArgs& args);

  void RegisterCommands();

  base::VarzValue::Map GetVarzStats();

 private:
  util::ProactorPool& pp_;

  ServerFamily server_family_;
  ClusterFamily cluster_family_;
  CommandRegistry registry_;
  absl::flat_hash_map<std::string, unsigned> unknown_cmds_;

  mutable Mutex mu_;
  GlobalState global_state_ = GlobalState::ACTIVE;  // protected by mu_;
};

uint64_t GetMaxMemoryFlag();
void SetMaxMemoryFlag(uint64_t value);

}  // namespace dfly
