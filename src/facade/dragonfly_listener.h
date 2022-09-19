// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "facade/facade_types.h"
#include "util/http/http_handler.h"
#include "util/listener_interface.h"

typedef struct ssl_ctx_st SSL_CTX;

namespace facade {

class ServiceInterface;

class Listener : public util::ListenerInterface {
 public:
  Listener(Protocol protocol, ServiceInterface*);
  ~Listener();

  std::error_code ConfigureServerSocket(int fd) final;

 private:
  util::Connection* NewConnection(util::ProactorBase* proactor) final;
  util::ProactorBase* PickConnectionProactor(util::LinuxSocketBase* sock) final;

  void PreShutdown() final;

  void PostShutdown() final;

  std::unique_ptr<util::HttpListenerBase> http_base_;

  ServiceInterface* service_;

  std::atomic_uint32_t next_id_{0};
  Protocol protocol_;
  SSL_CTX* ctx_ = nullptr;
};

}  // namespace facade
