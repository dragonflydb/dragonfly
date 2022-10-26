// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <boost/fiber/fiber.hpp>
#include <variant>

#include "base/io_buf.h"
#include "facade/facade_types.h"
#include "facade/redis_parser.h"
#include "util/fiber_socket_base.h"

namespace dfly {

class Service;
class ConnectionContext;

class Replica {
  // The attributes of the master we are connecting to.
  struct MasterContext {
    std::string host;
    std::string master_repl_id;
    std::string dfly_session_id;  // for dragonfly replication
    boost::asio::ip::tcp::endpoint master_ep;

    uint16_t port;
    uint32_t flow_id = UINT32_MAX;  // Relevant if this replica is used to transfer a flow.
  };

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
    return master_context_.host;
  }

  uint16_t port() {
    return master_context_.port;
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
  void Pause(bool pause);

 private:
  // Used to initialize df replication flow.
  Replica(const MasterContext& context, uint32_t flow_id, Service* service);

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

  // This function uses parser_ and cmd_args_ in order to consume a single response
  // from the sock_. The output will reside in cmd_str_args_.
  std::error_code ReadRespReply(base::IoBuf* io_buf, uint32_t* consumed);

  std::error_code ConnectSocket();
  std::error_code Greet();
  std::error_code InitiatePSync();
  std::error_code InitiateDflySync();

  std::error_code ParseReplicationHeader(base::IoBuf* io_buf, PSyncResponse* header);
  std::error_code ReadLine(base::IoBuf* io_buf, std::string_view* line);
  std::error_code ConsumeRedisStream();
  std::error_code ConsumeDflyStream();
  std::error_code ParseAndExecute(base::IoBuf* io_buf);

  std::error_code StartFlow();

  void ReplicateDFFb(std::unique_ptr<base::IoBuf> io_buf, std::string eof_token);

  Service& service_;

  ::boost::fibers::fiber sync_fb_;
  std::unique_ptr<util::LinuxSocketBase> sock_;
  MasterContext master_context_;

  // Where the sock_ is handled.
  // util::ProactorBase* sock_thread_ = nullptr;
  std::unique_ptr<facade::RedisParser> parser_;
  facade::RespVec resp_args_;
  facade::CmdArgVec cmd_str_args_;

  // repl_offs - till what offset we've already read from the master.
  // ack_offs_ last acknowledged offset.
  size_t repl_offs_ = 0, ack_offs_ = 0;
  uint64_t last_io_time_ = 0;  // in ns, monotonic clock.
  unsigned state_mask_ = 0;
  unsigned num_df_flows_ = 0;

  bool is_paused_ = false;

  std::vector<std::unique_ptr<Replica>> shard_flows_;
};

}  // namespace dfly
