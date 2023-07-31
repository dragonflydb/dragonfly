// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <absl/container/inlined_vector.h>

#include <boost/fiber/barrier.hpp>
#include <queue>
#include <variant>

#include "base/io_buf.h"
#include "facade/facade_types.h"
#include "facade/redis_parser.h"
#include "server/common.h"
#include "server/journal/types.h"
#include "server/protocol_client.h"
#include "server/version.h"
#include "util/fiber_socket_base.h"

namespace facade {
class ReqSerializer;
};  // namespace facade

namespace dfly {

class Service;
class ConnectionContext;
class JournalExecutor;
struct JournalReader;
class DflyShardReplica;

// Coordinator for multi shard execution.
struct MultiShardExecution {
  Mutex map_mu;

  struct TxExecutionSync {
    Barrier barrier;
    std::atomic_uint32_t counter;
    BlockingCounter block;

    explicit TxExecutionSync(uint32_t counter)
        : barrier(counter), counter(counter), block(counter) {
    }
  };

  std::unordered_map<TxId, TxExecutionSync> tx_sync_execution;
};

// The attributes of the master we are connecting to.
struct MasterContext {
  std::string master_repl_id;
  std::string dfly_session_id;  // Sync session id for dfly sync.
  DflyVersion version = DflyVersion::VER0;
};

// This class manages replication from both Dragonfly and Redis masters.
class Replica : ProtocolClient {
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

 public:
  Replica(std::string master_host, uint16_t port, Service* se, std::string_view id);
  ~Replica();

  // Spawns a fiber that runs until link with master is broken or the replication is stopped.
  // Returns true if initial link with master has been established or
  // false if it has failed.
  std::error_code Start(ConnectionContext* cntx);

  // Sets the server state to have replication enabled.
  // It is like Start(), but does not attempt to establish
  // a connection right-away, but instead lets MainReplicationFb do the work.
  std::error_code EnableReplication(ConnectionContext* cntx);

  void Stop();  // thread-safe

  void Pause(bool pause);

  std::error_code TakeOver(std::string_view timeout);

  std::string_view MasterId() const {
    return master_context_.master_repl_id;
  }

 private: /* Main standalone mode functions */
  // Coordinate state transitions. Spawned by start.
  void MainReplicationFb();

  std::error_code Greet();  // Send PING and REPLCONF.

  std::error_code HandleCapaDflyResp();
  std::error_code ConfigureDflyMaster();

  std::error_code InitiatePSync();     // Redis full sync.
  std::error_code InitiateDflySync();  // Dragonfly full sync.

  std::error_code ConsumeRedisStream();  // Redis stable state.
  std::error_code ConsumeDflyStream();   // Dragonfly stable state.

  void RedisStreamAcksFb();

  void JoinAllFlows();                // Join all flows if possible.
  void SetShardStates(bool replica);  // Call SetReplica(replica) on all shards.

  // Send DFLY ${kind} to the master instance.
  std::error_code SendNextPhaseRequest(std::string_view kind);

  void DefaultErrorHandler(const GenericError& err);

 private: /* Utility */
  struct PSyncResponse {
    // string - end of sync token (diskless)
    // size_t - size of the full sync blob (disk-based).
    // if fullsync is 0, it means that master can continue with partial replication.
    std::variant<std::string, size_t> fullsync;
  };

  std::error_code ParseReplicationHeader(base::IoBuf* io_buf, PSyncResponse* dest);

 public: /* Utility */
  struct Info {
    std::string host;
    uint16_t port;
    bool master_link_established;
    bool full_sync_in_progress;
    bool full_sync_done;
    time_t master_last_io_sec;  // monotonic clock.
  };

  Info GetInfo() const;  // thread-safe, blocks fiber

  bool HasDflyMaster() const {
    return !master_context_.dfly_session_id.empty();
  }

  const std::string& MasterHost() const {
    return server().host;
  }

  uint16_t Port() const {
    return server().port;
  }

  std::vector<uint64_t> GetReplicaOffset() const;
  std::string GetSyncId() const;

 private:
  Service& service_;
  MasterContext master_context_;

  // In redis replication mode.
  Fiber sync_fb_;
  Fiber acks_fb_;
  EventCount waker_;

  std::vector<std::unique_ptr<DflyShardReplica>> shard_flows_;
  std::shared_ptr<MultiShardExecution> multi_shard_exe_;

  // Guard operations where flows might be in a mixed state (transition/setup)
  Mutex flows_op_mu_;

  // repl_offs - till what offset we've already read from the master.
  // ack_offs_ last acknowledged offset.
  size_t repl_offs_ = 0, ack_offs_ = 0;
  std::atomic<unsigned> state_mask_ = 0;
  unsigned num_df_flows_ = 0;

  bool is_paused_ = false;
  std::string id_;
};

// This class implements a single shard replication flow from a Dragonfly master instance.
// Multiple DflyShardReplica objects are managed by a Replica object.
class DflyShardReplica : public ProtocolClient {
 public:
  DflyShardReplica(ServerContext server_context, MasterContext master_context, uint32_t flow_id,
                   Service* service, std::shared_ptr<MultiShardExecution> multi_shard_exe);
  ~DflyShardReplica();

  // This class holds the commands of transaction in single shard.
  // Once all commands were received, the transaction can be executed.
  struct TransactionData {
    // Update the data from ParsedEntry and return true if all shard transaction commands were
    // received.
    bool AddEntry(journal::ParsedEntry&& entry);

    bool IsGlobalCmd() const;

    static TransactionData FromSingle(journal::ParsedEntry&& entry);

    TxId txid{0};
    DbIndex dbid{0};
    uint32_t shard_cnt{0};
    absl::InlinedVector<journal::ParsedEntry::CmdData, 1> commands{0};
    uint32_t journal_rec_count{0};  // Count number of source entries to check offset.
    bool is_ping = false;           // For Op::PING entries.
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

  void Cancel();
  void JoinFlow();

  // Start replica initialized as dfly flow.
  std::error_code StartFullSyncFlow(BlockingCounter block, Context* cntx);

  // Transition into stable state mode as dfly flow.
  std::error_code StartStableSyncFlow(Context* cntx);

  // Single flow full sync fiber spawned by StartFullSyncFlow.
  void FullSyncDflyFb(const std::string& eof_token, BlockingCounter block, Context* cntx);

  // Single flow stable state sync fiber spawned by StartStableSyncFlow.
  void StableSyncDflyReadFb(Context* cntx);

  void StableSyncDflyAcksFb(Context* cntx);

  void StableSyncDflyExecFb(Context* cntx);

  void ExecuteTx(TransactionData&& tx_data, bool inserted_by_me, Context* cntx);
  void InsertTxDataToShardResource(TransactionData&& tx_data);
  void ExecuteTxWithNoShardSync(TransactionData&& tx_data, Context* cntx);
  bool InsertTxToSharedMap(const TransactionData& tx_data);

  uint32_t FlowId() const;

  uint64_t JournalExecutedCount() const;

 private:
  Service& service_;
  MasterContext master_context_;

  std::optional<base::IoBuf> leftover_buf_;

  std::queue<std::pair<TransactionData, bool>> trans_data_queue_;
  static constexpr size_t kYieldAfterItemsInQueue = 50;
  EventCount waker_;  // waker for trans_data_queue_
  bool use_multi_shard_exe_sync_;

  std::unique_ptr<JournalExecutor> executor_;

  // The master instance has a LSN for each journal record. This counts
  // the number of journal records executed in this flow plus the initial
  // journal offset that we received in the transition from full sync
  // to stable sync.
  // Note: This is not 1-to-1 the LSN in the master, because this counts
  // **executed** records, which might be received interleaved when commands
  // run out-of-order on the master instance.
  std::atomic_uint64_t journal_rec_executed_ = 0;

  Fiber sync_fb_;

  Fiber acks_fb_;
  size_t ack_offs_ = 0;

  bool force_ping_ = false;
  Fiber execution_fb_;

  std::shared_ptr<MultiShardExecution> multi_shard_exe_;
  uint32_t flow_id_ = UINT32_MAX;  // Flow id if replica acts as a dfly flow.
};

}  // namespace dfly
