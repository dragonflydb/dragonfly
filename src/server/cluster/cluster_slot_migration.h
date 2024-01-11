// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include "server/cluster/cluster_shard_migration.h"
#include "server/protocol_client.h"

namespace dfly {

class Service;

// The main entity on the target side that manage slots migration process
// Creates initial connection between the target and source node,
// manage migration process state and data
class ClusterSlotMigration : ProtocolClient {
 public:
  enum State : uint8_t { C_NO_STATE, C_CONNECTING, C_FULL_SYNC, C_STABLE_SYNC };

  struct Info {
    std::string host;
    uint16_t port;
    State state;
  };

  ClusterSlotMigration(std::string host_ip, uint16_t port, Service* se,
                       std::vector<ClusterConfig::SlotRange> slots);
  ~ClusterSlotMigration();

  // Initiate connection with source node and create migration fiber
  std::error_code Start(ConnectionContext* cntx);
  Info GetInfo() const;
  uint32_t getSyncId() const {
    return sync_id_;
  }
  bool trySetStableSync(uint32_t flow);

 private:
  // Send DFLYMIGRATE CONF to the source and get info about migration process
  std::error_code Greet();
  void MainMigrationFb();
  // Creates flows, one per shard on the source node and manage migration process
  std::error_code InitiateSlotsMigration();

 private:
  Service& service_;
  Mutex flows_op_mu_;
  std::vector<std::unique_ptr<ClusterShardMigration>> shard_flows_;
  std::vector<ClusterConfig::SlotRange> slots_;
  uint32_t source_shards_num_ = 0;
  uint32_t sync_id_ = 0;
  State state_ = C_NO_STATE;
  std::shared_ptr<MultiShardExecution> multi_shard_exe_;

  Fiber sync_fb_;
};

}  // namespace dfly
