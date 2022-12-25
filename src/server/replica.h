// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <boost/fiber/barrier.hpp>
#include <boost/fiber/fiber.hpp>
#include <boost/fiber/mutex.hpp>
#include <variant>

#include "base/io_buf.h"
#include "facade/facade_types.h"
#include "facade/redis_parser.h"
#include "server/common.h"
#include "server/journal/types.h"
#include "util/fiber_socket_base.h"
#include "util/fibers/fibers_ext.h"

namespace facade {
class ReqSerializer;
};  // namespace facade

namespace dfly {

class Service;
class ConnectionContext;
class JournalExecutor;

class Replica {
 private:
  // The attributes of the master we are connecting to.
  struct MasterContext {
    std::string host;
    uint16_t port;
    boost::asio::ip::tcp::endpoint endpoint;

    std::string master_repl_id;
    std::string dfly_session_id;         // Sync session id for dfly sync.
    uint32_t dfly_flow_id = UINT32_MAX;  // Flow id if replica acts as a dfly flow.
  };

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

  struct MultiShardExecution {
    boost::fibers::mutex map_mu;

    struct TxExecutionSync {
      boost::fibers::barrier barrier;
      std::atomic_uint32_t counter;
      TxExecutionSync(uint32_t counter) : barrier(counter), counter(counter) {
      }
    };

    std::unordered_map<TxId, TxExecutionSync> tx_sync_execution;
  };

 public:
  Replica(std::string master_host, uint16_t port, Service* se);
  ~Replica();

  // Spawns a fiber that runs until link with master is broken or the replication is stopped.
  // Returns true if initial link with master has been established or
  // false if it has failed.
  bool Start(ConnectionContext* cntx);

  void Stop();  // thread-safe

  void Pause(bool pause);

 private: /* Main standalone mode functions */
  // Coordinate state transitions. Spawned by start.
  void MainReplicationFb();

  std::error_code ConnectSocket();  // Connect to master.
  std::error_code Greet();          // Send PING and REPLCONF.

  std::error_code InitiatePSync();     // Redis full sync.
  std::error_code InitiateDflySync();  // Dragonfly full sync.

  std::error_code ConsumeRedisStream();  // Redis stable state.
  std::error_code ConsumeDflyStream();   // Dragonfly stable state.

  void CloseAllSockets();  // Close all sockets.
  void JoinAllFlows();     // Join all flows if possible.

  std::error_code SendNextPhaseRequest();  // Send DFLY SYNC or DFLY STARTSTABLE.

  void DefaultErrorHandler(const GenericError& err);

 private: /* Main dlfly flow mode functions */
  // Initialize as single dfly flow.
  Replica(const MasterContext& context, uint32_t dfly_flow_id, Service* service,
          std::shared_ptr<MultiShardExecution> shared_exe_data);

  // Start replica initialized as dfly flow.
  std::error_code StartFullSyncFlow(util::fibers_ext::BlockingCounter block, Context* cntx);

  // Transition into stable state mode as dfly flow.
  std::error_code StartStableSyncFlow(Context* cntx);

  // Single flow full sync fiber spawned by StartFullSyncFlow.
  void FullSyncDflyFb(std::string eof_token, util::fibers_ext::BlockingCounter block,
                      Context* cntx);

  // Single flow stable state sync fiber spawned by StartStableSyncFlow.
  void StableSyncDflyFb(Context* cntx);

 private: /* Utility */
  struct PSyncResponse {
    // string - end of sync token (diskless)
    // size_t - size of the full sync blob (disk-based).
    // if fullsync is 0, it means that master can continue with partial replication.
    std::variant<std::string, size_t> fullsync;
  };

  // This function uses parser_ and cmd_args_ in order to consume a single response
  // from the sock_. The output will reside in cmd_str_args_.
  std::error_code ReadRespReply(base::IoBuf* io_buf, uint32_t* consumed);

  std::error_code ParseReplicationHeader(base::IoBuf* io_buf, PSyncResponse* header);
  std::error_code ReadLine(base::IoBuf* io_buf, std::string_view* line);

  std::error_code ParseAndExecute(base::IoBuf* io_buf);

  // Check if reps_args contains a simple reply.
  bool CheckRespIsSimpleReply(std::string_view reply) const;

  // Check resp_args contains the following types at front.
  bool CheckRespFirstTypes(std::initializer_list<facade::RespExpr::Type> types) const;

  // Send command, update last_io_time, return error.
  std::error_code SendCommand(std::string_view command, facade::ReqSerializer* serializer);

  void ExecuteEntry(JournalExecutor* executor, journal::ParsedEntry& entry);

 public: /* Utility */
  struct Info {
    std::string host;
    uint16_t port;
    bool master_link_established;
    bool sync_in_progress;      // snapshot sync.
    time_t master_last_io_sec;  // monotonic clock.
  };

  Info GetInfo() const;  // thread-safe, blocks fiber

  bool HasDflyMaster() const {
    return !master_context_.dfly_session_id.empty();
  }

  bool IsDflyFlow() const {
    return master_context_.dfly_flow_id != UINT32_MAX;
  }

  const std::string& MasterHost() const {
    return master_context_.host;
  }

  uint16_t Port() const {
    return master_context_.port;
  }

 private:
  Service& service_;
  MasterContext master_context_;
  std::unique_ptr<util::LinuxSocketBase> sock_;

  std::shared_ptr<MultiShardExecution> multi_shard_exe_;

  // MainReplicationFb in standalone mode, FullSyncDflyFb in flow mode.
  ::boost::fibers::fiber sync_fb_;
  std::vector<std::unique_ptr<Replica>> shard_flows_;

  std::unique_ptr<base::IoBuf> leftover_buf_;
  std::unique_ptr<facade::RedisParser> parser_;
  facade::RespVec resp_args_;
  facade::CmdArgVec cmd_str_args_;

  Context cntx_;  // context for tasks in replica.

  // repl_offs - till what offset we've already read from the master.
  // ack_offs_ last acknowledged offset.
  size_t repl_offs_ = 0, ack_offs_ = 0;
  uint64_t last_io_time_ = 0;  // in ns, monotonic clock.
  unsigned state_mask_ = 0;
  unsigned num_df_flows_ = 0;

  bool is_paused_ = false;
};

}  // namespace dfly
