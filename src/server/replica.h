// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <boost/fiber/fiber.hpp>
#include <variant>

#include "base/io_buf.h"
#include "core/resp_expr.h"
#include "server/conn_context.h"
#include "util/fiber_socket_base.h"

namespace dfly {

class RedisParser;
class Service;

class Replica {
 public:
  Replica(std::string master_host, uint16_t port, Service* se);
  ~Replica();

  // Spawns a fiber that runs until link with master is broken or the replication is stopped.
  // Returns true if initial link with master has been established or
  // false if it has failed.
  bool Run(ConnectionContext* cntx);

  // Thread-safe
  void Stop();

  const std::string& master_host() const {
    return host_;
  }

  uint16_t port() {
    return port_;
  }

  struct Info {
    std::string host;
    uint16_t port;
    bool master_link_established;
    bool sync_in_progress;      // snapshot sync.
    time_t master_last_io_sec;  // monotonic clock.
  };

  // Threadsafe, fiber blocking.
  Info GetInfo() const;

 private:
  // The flow is : R_ENABLED -> R_TCP_CONNECTED -> (R_SYNCING) -> R_SYNC_OK.
  // SYNCING means that the initial ack succeeded. It may be optional if we can still load from
  // the journal offset.
  enum State {
    R_ENABLED = 1,   // Replication mode is enabled. Serves for signaling shutdown.
    R_TCP_CONNECTED = 2,
    R_SYNCING = 4,
    R_SYNC_OK = 8,
  };

  void ConnectFb();

  using ReplHeader = std::variant<std::string, size_t>;
  std::error_code ConnectSocket();
  std::error_code GreatAndSync();
  std::error_code ConsumeRedisStream();
  std::error_code ParseAndExecute(base::IoBuf* io_buf);

  Service& service_;
  std::string host_;
  uint16_t port_;

  ::boost::fibers::fiber sync_fb_;
  std::unique_ptr<util::LinuxSocketBase> sock_;

  // Where the sock_ is handled.
  util::ProactorBase* sock_thread_ = nullptr;
  std::unique_ptr<RedisParser> parser_;

  // repl_offs - till what offset we've already read from the master.
  // ack_offs_ last acknowledged offset.
  size_t repl_offs_ = 0, ack_offs_ = 0;
  uint64_t last_io_time_ = 0;  // in ns, monotonic clock.
  unsigned state_mask_ = 0;
};

}  // namespace dfly
