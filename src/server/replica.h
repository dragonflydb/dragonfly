// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <absl/container/inlined_vector.h>

#include <boost/fiber/barrier.hpp>
#include <queue>
#include <variant>

#include "facade/facade_types.h"
#include "facade/redis_parser.h"
#include "io/io_buf.h"
#include "server/cluster/cluster_defs.h"
#include "server/common.h"
#include "server/journal/tx_executor.h"
#include "server/journal/types.h"
#include "server/protocol_client.h"
#include "server/version.h"
#include "util/fiber_socket_base.h"

namespace dfly {

class Service;
class ConnectionContext;
class JournalExecutor;
struct JournalReader;
class DflyShardReplica;

// The attributes of the master we are connecting to.
struct MasterContext {
  std::string master_repl_id;
  std::string dfly_session_id;  // Sync session id for dfly sync.
  unsigned num_flows = 0;
  DflyVersion version = DflyVersion::VER1;
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
  Replica(std::string master_host, uint16_t port, Service* se, std::string_view id,
          std::optional<cluster::SlotRange> slot_range);
  ~Replica();

  // Spawns a fiber that runs until link with master is broken or the replication is stopped.
  // Returns true if initial link with master has been established or
  // false if it has failed.
  std::error_code Start(facade::SinkReplyBuilder* builder);

  // Sets the server state to have replication enabled.
  // It is like Start(), but does not attempt to establish
  // a connection right-away, but instead lets MainReplicationFb do the work.
  void EnableReplication(facade::SinkReplyBuilder* builder);

  void Stop();  // thread-safe

  void Pause(bool pause);

  std::error_code TakeOver(std::string_view timeout, bool save_flag);

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

  // Joins all the flows when doing sharded replication. This is called in two
  // places: Once at the end of full sync to join the full sync fibers, and twice
  // if a stable sync is interrupted to join the cancelled stable sync fibers.
  void JoinDflyFlows();
  void SetShardStates(bool replica);  // Call SetReplica(replica) on all shards.

  // Send DFLY ${kind} to the master instance.
  std::error_code SendNextPhaseRequest(std::string_view kind);

 private: /* Utility */
  struct PSyncResponse {
    // string - end of sync token (diskless)
    // size_t - size of the full sync blob (disk-based).
    // if fullsync is 0, it means that master can continue with partial replication.
    std::variant<std::string, size_t> fullsync;
  };

  std::error_code ParseReplicationHeader(base::IoBuf* io_buf, PSyncResponse* dest);

 public: /* Utility */
  struct Summary {
    std::string host;
    uint16_t port;
    bool master_link_established;
    bool full_sync_in_progress;
    bool full_sync_done;
    time_t master_last_io_sec;  // monotonic clock.
    std::string master_id;
    uint32_t reconnect_count;

    // sum of the offsets on all the flows.
    uint64_t repl_offset_sum;
  };

  Summary GetSummary() const;  // thread-safe, blocks fiber, makes a hop.

  bool HasDflyMaster() const {
    return !master_context_.dfly_session_id.empty();
  }

  std::vector<uint64_t> GetReplicaOffset() const;
  std::string GetSyncId() const;

 private:
  util::fb2::ProactorBase* proactor_ = nullptr;
  Service& service_;
  MasterContext master_context_;

  // In redis replication mode.
  util::fb2::Fiber sync_fb_;
  util::fb2::Fiber acks_fb_;
  util::fb2::EventCount replica_waker_;

  std::vector<std::unique_ptr<DflyShardReplica>> shard_flows_;
  std::vector<std::vector<unsigned>> thread_flow_map_;  // a map from proactor id to flow list.

  // A vector of the last executer LSNs when a replication is interrupted.
  // Allows partial sync on reconnects.
  std::optional<std::vector<LSN>> last_journal_LSNs_;
  std::shared_ptr<MultiShardExecution> multi_shard_exe_;

  // Guard operations where flows might be in a mixed state (transition/setup)
  util::fb2::Mutex flows_op_mu_;

  // repl_offs - till what offset we've already read from the master.
  // ack_offs_ last acknowledged offset.
  size_t repl_offs_ = 0, ack_offs_ = 0;
  std::atomic<unsigned> state_mask_ = 0;

  bool is_paused_ = false;
  std::string id_;

  std::optional<cluster::SlotRange> slot_range_;

  uint32_t reconnect_count_ = 0;
};

class RdbLoader;
// This class implements a single shard replication flow from a Dragonfly master instance.
// Multiple DflyShardReplica objects are managed by a Replica object.
class DflyShardReplica : public ProtocolClient {
 public:
  DflyShardReplica(ServerContext server_context, MasterContext master_context, uint32_t flow_id,
                   Service* service, std::shared_ptr<MultiShardExecution> multi_shard_exe);
  ~DflyShardReplica();

  void Cancel();
  void JoinFlow();

  // Start replica initialized as dfly flow.
  // Sets is_full_sync when successful.
  io::Result<bool> StartSyncFlow(util::fb2::BlockingCounter block, Context* cntx,
                                 std::optional<LSN>);

  // Transition into stable state mode as dfly flow.
  std::error_code StartStableSyncFlow(Context* cntx);

  // Single flow full sync fiber spawned by StartFullSyncFlow.
  void FullSyncDflyFb(std::string eof_token, util::fb2::BlockingCounter block, Context* cntx);

  // Single flow stable state sync fiber spawned by StartStableSyncFlow.
  void StableSyncDflyReadFb(Context* cntx);

  void StableSyncDflyAcksFb(Context* cntx);

  void ExecuteTx(TransactionData&& tx_data, Context* cntx);

  uint32_t FlowId() const;

  uint64_t JournalExecutedCount() const {
    return journal_rec_executed_.load(std::memory_order_relaxed);
  }

  // Can be called from any thread.
  void Pause(bool pause);

 private:
  Service& service_;
  MasterContext master_context_;

  std::optional<base::IoBuf> leftover_buf_;

  util::fb2::EventCount shard_replica_waker_;  // waker for trans_data_queue_

  std::unique_ptr<JournalExecutor> executor_;
  std::unique_ptr<RdbLoader> rdb_loader_;

  // The master instance has a LSN for each journal record. This counts
  // the number of journal records executed in this flow plus the initial
  // journal offset that we received in the transition from full sync
  // to stable sync.
  // Note: This is not 1-to-1 the LSN in the master, because this counts
  // **executed** records, which might be received interleaved when commands
  // run out-of-order on the master instance.
  // Atomic, because JournalExecutedCount() can be called from any thread.
  std::atomic_uint64_t journal_rec_executed_ = 0;

  util::fb2::Fiber sync_fb_, acks_fb_;
  size_t ack_offs_ = 0;
  int proactor_index_ = -1;
  bool force_ping_ = false;

  std::shared_ptr<MultiShardExecution> multi_shard_exe_;
  uint32_t flow_id_ = UINT32_MAX;  // Flow id if replica acts as a dfly flow.
};

}  // namespace dfly
