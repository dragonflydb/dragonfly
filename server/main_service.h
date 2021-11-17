// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include "base/varz_value.h"
#include "util/http/http_handler.h"
#include "server/engine_shard_set.h"

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

  uint32_t shard_count() const {
    return shard_set_.size();
  }

  EngineShardSet& shard_set() {
    return shard_set_;
  }

  util::ProactorPool& proactor_pool() {
    return pp_;
  }

  void Set(std::string_view key, std::string_view val);
 private:

  base::VarzValue::Map GetVarzStats();

  EngineShardSet shard_set_;
  util::ProactorPool& pp_;
};

}  // namespace dfly
