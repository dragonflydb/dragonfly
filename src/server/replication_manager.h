// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "server/cluster/cluster_defs.h"
#include "server/execution_state.h"
#include "server/metrics.h"
#include "server/replica_types.h"
#include "util/fibers/synchronization.h"

namespace dfly {

class Replica;
class Service;

struct ReplicaOffsetInfo {
  std::string sync_id;
  std::vector<uint64_t> flow_offsets;
};

// Owns the replica-side role of this node and its transitions to and from master.
// The thread-local is_master flag is flipped together with replica_ under mu_.
class ReplicationManager {
 public:
  enum ActionOnConnectionFail {
    kReturnOnError,        // if we fail to connect to master, return to err
    kContinueReplication,  // continue attempting to connect to master, regardless of initial
                           // failure
  };

  struct MasterLinkClientInfo {
    uint32_t client_id;
    std::string info;
  };

  ReplicationManager(Service* service, std::string master_replid,
                     std::function<void()> unblock_all_clients);
  ~ReplicationManager();

  // Replaces the current replica (if any) with a new one replicating host:port.
  GenericError StartReplication(const std::string& host, uint16_t port,
                                std::optional<cluster::SlotRange> slot_range,
                                ActionOnConnectionFail on_error) ABSL_LOCKS_EXCLUDED(mu_);

  // REPLICAOF NO ONE.
  void PromoteToMaster() ABSL_LOCKS_EXCLUDED(mu_);

  // REPLTAKEOVER. Returns no error if already master.
  GenericError TakeOver(int timeout_sec, bool save_flag) ABSL_LOCKS_EXCLUDED(mu_);

  // ADDREPLICAOF.
  GenericError AddClusterReplica(const std::string& host, uint16_t port,
                                 std::optional<cluster::SlotRange> slot_range)
      ABSL_LOCKS_EXCLUDED(mu_);

  // Stops all replicas without changing the role.
  void Shutdown() ABSL_LOCKS_EXCLUDED(mu_);

  // Checks the role under mu_, so it is consistent with a concurrent role transition.
  bool IsReplica() const ABSL_LOCKS_EXCLUDED(mu_);

  void Pause(bool pause) ABSL_LOCKS_EXCLUDED(mu_);
  std::optional<ReplicaOffsetInfo> GetOffsetInfo() const ABSL_LOCKS_EXCLUDED(mu_);
  std::optional<int> GetMasterSocketUnreadBytes() const ABSL_LOCKS_EXCLUDED(mu_);

  // Returns replication summary if this server is a replica, nullopt otherwise.
  std::optional<Metrics::ReplicaInfo> GetSummary() const ABSL_LOCKS_EXCLUDED(mu_);

  std::vector<MasterLinkClientInfo> GetMasterLinkClientInfo() const ABSL_LOCKS_EXCLUDED(mu_);
  bool IsMasterLinkClientId(uint32_t id) const ABSL_LOCKS_EXCLUDED(mu_);

  std::string GetLineageId() const ABSL_LOCKS_EXCLUDED(mu_);

  std::optional<LastMasterSyncData> GetLastMasterData() const {
    return last_master_data_;
  }

  const std::string& master_replid() const {
    return master_replid_;
  }

 private:
  void StopAllClusterReplicas() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  Service* service_;
  const std::string master_replid_;
  std::function<void()> unblock_all_clients_;

  mutable util::fb2::Mutex mu_;
  std::shared_ptr<Replica> replica_ ABSL_GUARDED_BY(mu_);

  // used to replicating multiple nodes to single dragonfly
  std::vector<std::unique_ptr<Replica>> cluster_replicas_ ABSL_GUARDED_BY(mu_);

  std::optional<LastMasterSyncData> last_master_data_;
};

}  // namespace dfly
