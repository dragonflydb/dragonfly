// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <utility>

#include "base/varz_value.h"
#include "core/interpreter.h"
#include "facade/service_interface.h"
#include "server/acl/acl_commands_def.h"
#include "server/acl/acl_family.h"
#include "server/acl/user_registry.h"
#include "server/cluster/cluster_family.h"
#include "server/command_registry.h"
#include "server/config_registry.h"
#include "server/engine_shard_set.h"
#include "server/server_family.h"

namespace util {
class AcceptServer;
}  // namespace util

namespace dfly {

using facade::MemcacheParser;

class Service : public facade::ServiceInterface {
 public:
  explicit Service(util::ProactorPool* pp);
  ~Service();

  void Init(util::AcceptServer* acceptor, std::vector<facade::Listener*> listeners);

  void Shutdown();

  // Prepare command execution, verify and execute, reply to context
  void DispatchCommand(ArgSlice args, facade::SinkReplyBuilder* builder,
                       facade::ConnectionContext* cntx) final;

  // Execute multiple consecutive commands, possibly in parallel by squashing
  size_t DispatchManyCommands(absl::Span<ArgSlice> args_list, facade::SinkReplyBuilder* builder,
                              facade::ConnectionContext* cntx) final;

  // Check VerifyCommandExecution and invoke command with args
  bool InvokeCmd(const CommandId* cid, CmdArgList tail_args, facade::SinkReplyBuilder* builder,
                 ConnectionContext* reply_cntx);

  // Verify command can be executed now (check out of memory), always called immediately before
  // execution
  std::optional<facade::ErrorReply> VerifyCommandExecution(const CommandId* cid,
                                                           const ConnectionContext* cntx,
                                                           CmdArgList tail_args);

  // Verify command prepares excution in correct state.
  // It's usually called before command execution. Only for multi/exec transactions it's checked
  // when the command is queued for execution, not before the execution itself.
  std::optional<facade::ErrorReply> VerifyCommandState(const CommandId* cid, ArgSlice tail_args,
                                                       const ConnectionContext& cntx);

  void DispatchMC(const MemcacheParser::Command& cmd, std::string_view value,
                  facade::MCReplyBuilder* builder, facade::ConnectionContext* cntx) final;

  facade::ConnectionContext* CreateContext(facade::Connection* owner) final;

  const CommandId* FindCmd(std::string_view) const;

  CommandRegistry* mutable_registry() {
    return &registry_;
  }

  facade::ErrorReply ReportUnknownCmd(std::string_view cmd_name) ABSL_LOCKS_EXCLUDED(mu_);

  // Returns: the new state.
  // if from equals the old state then the switch is performed "to" is returned.
  // Otherwise, does not switch and returns the current state in the system.
  // Upon switch, updates cached global state in threadlocal ServerState struct.
  GlobalState SwitchState(GlobalState from, GlobalState to) ABSL_LOCKS_EXCLUDED(mu_);

  bool RequestLoadingState() ABSL_LOCKS_EXCLUDED(mu_);
  void RemoveLoadingState() ABSL_LOCKS_EXCLUDED(mu_);

  void ConfigureHttpHandlers(util::HttpListenerBase* base, bool is_privileged) final;
  void OnConnectionClose(facade::ConnectionContext* cntx) final;

  Service::ContextInfo GetContextInfo(facade::ConnectionContext* cntx) const final;

  uint32_t shard_count() const {
    return shard_set->size();
  }

  // Used by tests.
  bool IsLocked(Namespace* ns, DbIndex db_index, std::string_view key) const;
  bool IsShardSetLocked() const;

  util::ProactorPool& proactor_pool() {
    return pp_;
  }

  absl::flat_hash_map<std::string, unsigned> UknownCmdMap() const;

  ScriptMgr* script_mgr() {
    return server_family_.script_mgr();
  }

  const ScriptMgr* script_mgr() const {
    return server_family_.script_mgr();
  }

  ServerFamily& server_family() {
    return server_family_;
  }

  cluster::ClusterFamily& cluster_family() {
    return cluster_family_;
  }

  // Utility function used in unit tests
  // Do not use in production, only meant to be used by unit tests
  const acl::AclFamily* TestInit();

 private:
  using SinkReplyBuilder = facade::SinkReplyBuilder;

  static void Quit(CmdArgList args, const CommandContext& cmd_cntx);
  static void Multi(CmdArgList args, const CommandContext& cmd_cntx);

  static void Watch(CmdArgList args, const CommandContext& cmd_cntx);
  static void Unwatch(CmdArgList args, const CommandContext& cmd_cntx);

  void Discard(CmdArgList args, const CommandContext& cmd_cntx);
  void Eval(CmdArgList args, const CommandContext& cmd_cntx, bool read_only = false);
  void EvalRo(CmdArgList args, const CommandContext& cmd_cntx);
  void EvalSha(CmdArgList args, const CommandContext& cmd_cntx, bool read_only = false);
  void EvalShaRo(CmdArgList args, const CommandContext& cmd_cntx);
  void Exec(CmdArgList args, const CommandContext& cmd_cntx);
  void Publish(CmdArgList args, const CommandContext& cmd_cntx);
  void Subscribe(CmdArgList args, const CommandContext& cmd_cntx);
  void Unsubscribe(CmdArgList args, const CommandContext& cmd_cntx);
  void PSubscribe(CmdArgList args, const CommandContext& cmd_cntx);
  void PUnsubscribe(CmdArgList args, const CommandContext& cmd_cntx);
  void Function(CmdArgList args, const CommandContext& cmd_cntx);
  void Monitor(CmdArgList args, const CommandContext& cmd_cntx);
  void Pubsub(CmdArgList args, const CommandContext& cmd_cntx);
  void Command(CmdArgList args, const CommandContext& cmd_cntx);

  void PubsubChannels(std::string_view pattern, SinkReplyBuilder* builder);
  void PubsubPatterns(SinkReplyBuilder* builder);
  void PubsubNumSub(CmdArgList channels, SinkReplyBuilder* builder);

  struct EvalArgs {
    std::string_view sha;  // only one of them is defined.
    CmdArgList keys, args;
  };

  // Return error if not all keys are owned by the server when running in cluster mode
  std::optional<facade::ErrorReply> CheckKeysOwnership(const CommandId* cid, CmdArgList args,
                                                       const ConnectionContext& dfly_cntx);

  void EvalInternal(CmdArgList args, const EvalArgs& eval_args, Interpreter* interpreter,
                    SinkReplyBuilder* builder, ConnectionContext* cntx, bool read_only);
  void CallSHA(CmdArgList args, std::string_view sha, Interpreter* interpreter,
               SinkReplyBuilder* builder, ConnectionContext* cntx, bool read_only);

  // Return optional payload - first received error that occured when executing commands.
  std::optional<facade::CapturingReplyBuilder::Payload> FlushEvalAsyncCmds(ConnectionContext* cntx,
                                                                           bool force = false);

  void CallFromScript(ConnectionContext* cntx, Interpreter::CallArgs& args);

  void RegisterCommands();
  void Register(CommandRegistry* registry);

  base::VarzValue::Map GetVarzStats();

  util::ProactorPool& pp_;

  acl::UserRegistry user_registry_;
  acl::AclFamily acl_family_;
  ServerFamily server_family_;
  cluster::ClusterFamily cluster_family_;
  CommandRegistry registry_;
  absl::flat_hash_map<std::string, unsigned> unknown_cmds_;

  const CommandId* exec_cid_;  // command id of EXEC command for pipeline squashing

  mutable util::fb2::Mutex mu_;
  GlobalState global_state_ ABSL_GUARDED_BY(mu_) = GlobalState::ACTIVE;
  uint32_t loading_state_counter_ ABSL_GUARDED_BY(mu_) = 0;
};

uint64_t GetMaxMemoryFlag();
void SetMaxMemoryFlag(uint64_t value);

}  // namespace dfly
