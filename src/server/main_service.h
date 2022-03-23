// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "base/varz_value.h"
#include "facade/service_interface.h"
#include "server/command_registry.h"
#include "server/engine_shard_set.h"
#include "server/server_family.h"

namespace util {
class AcceptServer;
}  // namespace util

namespace dfly {

class Interpreter;
class ObjectExplorer;  // for Interpreter
using facade::MemcacheParser;

class Service : public facade::ServiceInterface {
 public:
  using error_code = std::error_code;

  struct InitOpts {
    bool disable_time_update;

    InitOpts() : disable_time_update{false} {
    }
  };

  explicit Service(util::ProactorPool* pp);
  ~Service();

  void Init(util::AcceptServer* acceptor, const InitOpts& opts = InitOpts{});

  void Shutdown();

  void DispatchCommand(CmdArgList args, facade::ConnectionContext* cntx) final;
  void DispatchMC(const MemcacheParser::Command& cmd, std::string_view value,
                  facade::ConnectionContext* cntx) final;

  facade::ConnectionContext* CreateContext(util::FiberSocketBase* peer,
                                           facade::Connection* owner) final;

  facade::ConnectionStats* GetThreadLocalConnectionStats() final;

  uint32_t shard_count() const {
    return shard_set_.size();
  }

  // Used by tests.
  bool IsLocked(DbIndex db_index, std::string_view key) const;
  bool IsShardSetLocked() const;

  EngineShardSet& shard_set() {
    return shard_set_;
  }

  util::ProactorPool& proactor_pool() {
    return pp_;
  }

  bool IsPassProtected() const;

  absl::flat_hash_map<std::string, unsigned> UknownCmdMap() const;

 private:
  static void Quit(CmdArgList args, ConnectionContext* cntx);
  static void Multi(CmdArgList args, ConnectionContext* cntx);
  static void Publish(CmdArgList args, ConnectionContext* cntx);
  static void Subscribe(CmdArgList args, ConnectionContext* cntx);

  void Eval(CmdArgList args, ConnectionContext* cntx);
  void EvalSha(CmdArgList args, ConnectionContext* cntx);
  void Exec(CmdArgList args, ConnectionContext* cntx);

  struct EvalArgs {
    std::string_view sha;  // only one of them is defined.
    CmdArgList keys, args;
  };

  void EvalInternal(const EvalArgs& eval_args, Interpreter* interpreter, ConnectionContext* cntx);

  void CallFromScript(CmdArgList args, ObjectExplorer* reply, ConnectionContext* cntx);

  void RegisterCommands();
  base::VarzValue::Map GetVarzStats();

  util::ProactorPool& pp_;

  EngineShardSet shard_set_;
  ServerFamily server_family_;
  CommandRegistry registry_;

  mutable ::boost::fibers::mutex stats_mu_;
  absl::flat_hash_map<std::string, unsigned> unknown_cmds_;
};

}  // namespace dfly
