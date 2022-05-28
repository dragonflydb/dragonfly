// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <boost/fiber/fiber.hpp>
#include <variant>

#include "base/io_buf.h"
#include "facade/redis_parser.h"
#include "server/conn_context.h"
#include "util/fiber_socket_base.h"

namespace dfly {

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
  enum State : unsigned {
    R_ENABLED = 1,  // Replication mode is enabled. Serves for signaling shutdown.
    R_TCP_CONNECTED = 2,
    R_GREETED = 4,
    R_SYNCING = 8,
    R_SYNC_OK = 0x10,
  };

  void ReplicateFb();

  struct PSyncResponse {
    // string - end of sync token (diskless)
    // size_t - size of the full sync blob (disk-based).
    // if fullsync is 0, it means that master can continue with partial replication.
    std::variant<std::string, size_t> fullsync;
  };

  std::error_code ConnectSocket();
  std::error_code Greet();
  std::error_code InitiatePSync();

  std::error_code ParseReplicationHeader(base::IoBuf* io_buf, PSyncResponse* header);
  std::error_code ReadLine(base::IoBuf* io_buf, std::string_view* line);
  std::error_code ConsumeRedisStream();
  std::error_code ParseAndExecute(base::IoBuf* io_buf);

  Service& service_;
  std::string host_;
  std::string master_repl_id_;
  uint16_t port_;

  ::boost::fibers::fiber sync_fb_;
  std::unique_ptr<util::LinuxSocketBase> sock_;

  // Where the sock_ is handled.
  util::ProactorBase* sock_thread_ = nullptr;
  std::unique_ptr<facade::RedisParser> parser_;

  // repl_offs - till what offset we've already read from the master.
  // ack_offs_ last acknowledged offset.
  size_t repl_offs_ = 0, ack_offs_ = 0;
  uint64_t last_io_time_ = 0;  // in ns, monotonic clock.
  unsigned state_mask_ = 0;
};

}  // namespace dfly
