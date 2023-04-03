// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/base/internal/spinlock.h>

#include "facade/facade_types.h"
#include "util/http/http_handler.h"
#include "util/listener_interface.h"

typedef struct ssl_ctx_st SSL_CTX;

namespace facade {

class ServiceInterface;

namespace detail {

// maps keys to counts, where keys must be indices in range [0, N].
class CntMinHeap {
  std::vector<unsigned> cnts_;  // keys -> counts
  unsigned min_key_ = 0;

 public:
  using Key = unsigned;

  CntMinHeap() = default;

  void Init(unsigned N) {
    cnts_.resize(N);
  }

  void Inc(Key key);
  void Dec(Key key);

  Key MinKey() const {
    return min_key_;
  }

  unsigned MinCnt() const {
    return cnts_[min_key_];
  }

  unsigned operator[](Key key) const {
    return cnts_[key];
  }
};

}  // namespace detail

class Listener : public util::ListenerInterface {
 public:
  Listener(Protocol protocol, ServiceInterface*);
  ~Listener();

  std::error_code ConfigureServerSocket(int fd) final;

 private:
  util::Connection* NewConnection(util::ProactorBase* proactor) final;
  util::ProactorBase* PickConnectionProactor(util::LinuxSocketBase* sock) final;

  void OnConnectionStart(util::Connection* conn) final;
  void OnConnectionClose(util::Connection* conn) final;
  void PreAcceptLoop(util::ProactorBase* pb) final;

  void PreShutdown() final;

  void PostShutdown() final;

  std::unique_ptr<util::HttpListenerBase> http_base_;

  ServiceInterface* service_;

  std::atomic_uint32_t next_id_{0};
  detail::CntMinHeap cnt_heap_;
  std::atomic_uint32_t conn_cnt_{0};
  absl::base_internal::SpinLock mutex_;

  Protocol protocol_;
  SSL_CTX* ctx_ = nullptr;
};

}  // namespace facade
