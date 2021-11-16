// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include "base/varz_value.h"
#include "util/http/http_handler.h"

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

  util::ProactorPool& proactor_pool() {
    return pp_;
  }

 private:
  
  base::VarzValue::Map GetVarzStats();

  util::ProactorPool& pp_;
};

}  // namespace dfly
