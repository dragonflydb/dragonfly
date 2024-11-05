// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/base/internal/spinlock.h>
#include <absl/time/time.h>

#include <memory>
#include <system_error>
#include <utility>
#include <vector>

#include "facade/facade_types.h"
#include "util/fiber_socket_base.h"
#include "util/fibers/proactor_base.h"
#include "util/http/http_handler.h"
#include "util/listener_interface.h"

typedef struct ssl_ctx_st SSL_CTX;

namespace facade {

class ServiceInterface;
class Connection;

class Listener : public util::ListenerInterface {
 public:
  // The Role PRIVILEGED is for admin port/listener
  // The Role MAIN is for the main listener on main port
  // The Role OTHER is for all the other listeners
  enum class Role { PRIVILEGED, MAIN, OTHER };
  Listener(Protocol protocol, ServiceInterface*, Role role = Role::OTHER);
  ~Listener();

  std::error_code ConfigureServerSocket(int fd) final;

  // Wait until all command dispatches that are currently in progress finish,
  // ignore commands from issuer connection.
  bool AwaitCurrentDispatches(absl::Duration timeout, util::Connection* issuer);

  // ReconfigureTLS MUST be called from the same proactor as the listener.
  bool ReconfigureTLS();

  // Returns thread-local dynamic memory usage by TLS.
  static size_t TLSUsedMemoryThreadLocal();
  static uint64_t RefusedConnectionMaxClientsCount();

  bool IsPrivilegedInterface() const;
  bool IsMainInterface() const;

  Protocol protocol() const {
    return protocol_;
  }

 private:
  util::Connection* NewConnection(ProactorBase* proactor) final;
  ProactorBase* PickConnectionProactor(util::FiberSocketBase* sock) final;

  void OnConnectionStart(util::Connection* conn) final;
  void OnConnectionClose(util::Connection* conn) final;
  void OnMaxConnectionsReached(util::FiberSocketBase* sock) final;
  void PreAcceptLoop(ProactorBase* pb) final;

  void PreShutdown() final;
  void PostShutdown() final;

  std::unique_ptr<util::HttpListenerBase> http_base_;

  ServiceInterface* service_;

  std::atomic_uint32_t next_id_{0};

  Role role_;

  uint32_t conn_cnt_{0};

  Protocol protocol_;
  SSL_CTX* ctx_ = nullptr;
};

// Dispatch tracker allows tracking the dispatch state of connections and blocking until all
// detected busy connections finished dispatching. Ignores issuer connection.
//
// Mostly used to detect when global state changes (takeover, pause, cluster config update) are
// visible to all commands and no commands are still running according to the old state / config.
class DispatchTracker {
 public:
  DispatchTracker(absl::Span<facade::Listener* const>, facade::Connection* issuer = nullptr,
                  bool ignore_paused = false, bool ignore_blocked = false);

  void TrackAll();       // Track busy connection on all threads
  void TrackOnThread();  // Track busy connections on current thread

  // Wait until all tracked connections finished dispatching.
  // Returns true on success, false if timeout was reached.
  bool Wait(absl::Duration timeout);

 private:
  void Handle(unsigned thread_index, util::Connection* conn);

  std::vector<facade::Listener*> listeners_;
  facade::Connection* issuer_;
  util::fb2::BlockingCounter bc_{0};  // tracks number of pending checkpoints
  bool ignore_paused_;
  bool ignore_blocked_;
};

}  // namespace facade
