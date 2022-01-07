// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "server/engine_shard_set.h"
#include "util/proactor_pool.h"

namespace util {
class AcceptServer;
}  // namespace util

namespace dfly {

class ConnectionContext;
class CommandRegistry;
class Service;

class ServerFamily {
 public:
  ServerFamily(Service* engine);
  ~ServerFamily();

  void Init(util::AcceptServer* acceptor);
  void Register(CommandRegistry* registry);
  void Shutdown();

 private:
  uint32_t shard_count() const {
    return ess_.size();
  }

  void Debug(CmdArgList args, ConnectionContext* cntx);
  void DbSize(CmdArgList args, ConnectionContext* cntx);
  void FlushDb(CmdArgList args, ConnectionContext* cntx);
  void FlushAll(CmdArgList args, ConnectionContext* cntx);
  void Info(CmdArgList args, ConnectionContext* cntx);
  void _Shutdown(CmdArgList args, ConnectionContext* cntx);

  Service& engine_;
  util::ProactorPool& pp_;
  EngineShardSet& ess_;

  util::AcceptServer* acceptor_ = nullptr;
};

}  // namespace dfly
