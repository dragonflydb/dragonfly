// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include "util/listener_interface.h"

typedef struct ssl_ctx_st SSL_CTX;

namespace dfly {

class Service;

class Listener : public util::ListenerInterface {
 public:
  Listener(Service*);
  ~Listener();

 private:
  util::Connection* NewConnection(util::ProactorBase* proactor) final;
  util::ProactorBase* PickConnectionProactor(util::LinuxSocketBase* sock) final;

  void PreShutdown();

  void PostShutdown();

  Service* engine_;

  std::atomic_uint32_t next_id_{0};
  SSL_CTX* ctx_ = nullptr;
};

}  // namespace dfly
