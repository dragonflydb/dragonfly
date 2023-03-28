// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <boost/fiber/barrier.hpp>
#include <queue>
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
struct JournalReader;

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

  // This class holds the commands of transaction in single shard.
  // Once all commands recieved the command can be executed.
  struct TransactionData {
    // Update the data from ParsedEntry and return true if all shard transaction commands were
    // recieved.
    bool AddEntry(journal::ParsedEntry&& entry);

    bool IsGlobalCmd() const;

    static TransactionData FromSingle(journal::ParsedEntry&& entry);

    TxId txid{0};
    DbIndex dbid{0};
    uint32_t shard_cnt{0};
    std::vector<journal::ParsedEntry::CmdData> commands{0};
    uint32_t journal_rec_count{0};  // Count number of source entries to check offset.
  };

  // Utility for reading TransactionData from a journal reader.
  // The journal stream can contain interleaved data for multiple multi transactions,
  // expiries and out of order executed transactions that need to be grouped on the replica side.
  struct TransactionReader {
    std::optional<TransactionData> NextTxData(JournalReader* reader, Context* cntx);

   private:
    // Stores ongoing multi transaction data.
    absl::flat_hash_map<TxId, TransactionData> current_;
  };

  // Coorindator for multi shard execution.
  struct MultiShardExecution {
    util::fibers_ext::Mutex map_mu;

    struct TxExecutionSync {
      util::fibers_ext::Barrier barrier;
      std::atomic_uint32_t counter;
      util::fibers_ext::BlockingCounter block;

      TxExecutionSync(uint32_t counter) : barrier(counter), counter(counter), block(counter) {
      }
    };

    std::unordered_map<TxId, TxExecutionSync> tx_sync_execution;
  };

 public:
  Replica(std::string master_host, uint16_t port, Service* se, std::string_view id);
  ~Replica();

  // Spawns a fiber that runs until link with master is broken or the replication is stopped.
  // Returns true if initial link with master has been established or
  // false if it has failed.
  bool Start(ConnectionContext* cntx);

  void Stop();  // thread-safe

  void Pause(bool pause);

  std::string_view MasterId() const {
    return master_context_.master_repl_id;
  }

 private: /* Main standalone mode functions */
  // Coordinate state transitions. Spawned by start.
  void MainReplicationFb();

  std::error_code ResolveMasterDns();  // Resolve master dns
  std::error_code ConnectAndAuth();    // Connect to master and authenticate if needed.
  std::error_code Greet();             // Send PING and REPLCONF.

  std::error_code InitiatePSync();     // Redis full sync.
  std::error_code InitiateDflySync();  // Dragonfly full sync.

  std::error_code ConsumeRedisStream();  // Redis stable state.
  std::error_code ConsumeDflyStream();   // Dragonfly stable state.

  void CloseSocket();                 // Close replica sockets.
  void JoinAllFlows();                // Join all flows if possible.
  void SetShardStates(bool replica);  // Call SetReplica(replica) on all shards.

  // Send DFLY SYNC or DFLY STARTSTABLE if stable is true.
  std::error_code SendNextPhaseRequest(bool stable);

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
  void StableSyncDflyReadFb(Context* cntx);

  void StableSyncDflyExecFb(Context* cntx);

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

  std::error_code ParseAndExecute(base::IoBuf* io_buf, ConnectionContext* cntx);

  // Check if reps_args contains a simple reply.
  bool CheckRespIsSimpleReply(std::string_view reply) const;

  // Check resp_args contains the following types at front.
  bool CheckRespFirstTypes(std::initializer_list<facade::RespExpr::Type> types) const;

  // Send command, update last_io_time, return error.
  std::error_code SendCommand(std::string_view command, facade::ReqSerializer* serializer);

  void ExecuteTx(TransactionData&& tx_data, bool inserted_by_me, Context* cntx);
  void InsertTxDataToShardResource(TransactionData&& tx_data);
  void ExecuteTxWithNoShardSync(TransactionData&& tx_data, Context* cntx);
  bool InsertTxToSharedMap(const TransactionData& tx_data);

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

  std::vector<uint64_t> GetReplicaOffset() const;
  std::string GetSyncId() const;

 private:
  Service& service_;
  MasterContext master_context_;
  std::unique_ptr<util::LinuxSocketBase> sock_;

  std::shared_ptr<MultiShardExecution> multi_shard_exe_;

  std::queue<std::pair<TransactionData, bool>> trans_data_queue_;
  static constexpr size_t kYieldAfterItemsInQueue = 50;
  ::util::fibers_ext::EventCount waker_;  // waker for trans_data_queue_
  bool use_multi_shard_exe_sync_;

  std::unique_ptr<JournalExecutor> executor_;
  // Count the number of journal records executed in specific flow
  std::atomic_uint64_t journal_rec_executed_ = 0;

  // MainReplicationFb in standalone mode, FullSyncDflyFb in flow mode.
  ::util::fibers_ext::Fiber sync_fb_;
  ::util::fibers_ext::Fiber execution_fb_;

  std::vector<std::unique_ptr<Replica>> shard_flows_;

  // Guard operations where flows might be in a mixed state (transition/setup)
  util::fibers_ext::Mutex flows_op_mu_;

  std::optional<base::IoBuf> leftover_buf_;
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
  std::string id_;
};

}  // namespace dfly
