// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include "base/varz_value.h"
#include "server/command_registry.h"
#include "server/engine_shard_set.h"
#include "util/http/http_handler.h"
#include "server/memcache_parser.h"

namespace util {
class AcceptServer;
}  // namespace util

namespace dfly {

class Service {
 public:
  using error_code = std::error_code;

  explicit Service(util::ProactorPool* pp);
  ~Service();

  void RegisterHttp(util::HttpListenerBase* listener);

  void Init(util::AcceptServer* acceptor);

  void Shutdown();

  void DispatchCommand(CmdArgList args, ConnectionContext* cntx);
  void DispatchMC(const MemcacheParser::Command& cmd, std::string_view value,
                  ConnectionContext* cntx);

  uint32_t shard_count() const {
    return shard_set_.size();
  }

  EngineShardSet& shard_set() {
    return shard_set_;
  }

  util::ProactorPool& proactor_pool() {
    return pp_;
  }

 private:
  void Ping(CmdArgList args, ConnectionContext* cntx);
  void Set(CmdArgList args, ConnectionContext* cntx);
  void Get(CmdArgList args, ConnectionContext* cntx);

  void RegisterCommands();

  base::VarzValue::Map GetVarzStats();

  CommandRegistry registry_;
  EngineShardSet shard_set_;
  util::ProactorPool& pp_;
};

}  // namespace dfly
