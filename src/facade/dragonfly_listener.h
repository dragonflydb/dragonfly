// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/base/internal/spinlock.h>

#include "facade/facade_types.h"
#include "util/fibers/proactor_base.h"
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

  // Wait until all connections that pass the filter have stopped dispatching or until a timeout has
  // run out. Returns true if the all connections have stopped dispatching.
  bool AwaitDispatches(absl::Duration timeout,
                       const std::function<bool(util::Connection*)>& filter);

 private:
  util::Connection* NewConnection(ProactorBase* proactor) final;
  ProactorBase* PickConnectionProactor(util::LinuxSocketBase* sock) final;

  void OnConnectionStart(util::Connection* conn) final;
  void OnConnectionClose(util::Connection* conn) final;
  void PreAcceptLoop(ProactorBase* pb) final;

  void PreShutdown() final;
  void PostShutdown() final;

  std::unique_ptr<util::HttpListenerBase> http_base_;

  ServiceInterface* service_;

  struct PerThread {
    int32_t num_connections{0};
    unsigned napi_id = 0;
  };
  std::vector<PerThread> per_thread_;

  std::atomic_uint32_t next_id_{0};

  uint32_t conn_cnt_{0};
  uint32_t min_cnt_thread_id_{0};
  int32_t min_cnt_{0};
  absl::base_internal::SpinLock mutex_;

  Protocol protocol_;
  SSL_CTX* ctx_ = nullptr;
};

}  // namespace facade
