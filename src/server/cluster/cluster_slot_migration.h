// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include "server/protocol_client.h"

namespace dfly {
class ClusterShardMigration;

class Service;
class ClusterFamily;

// The main entity on the target side that manage slots migration process
// Creates initial connection between the target and source node,
// manage migration process state and data
class ClusterSlotMigration : private ProtocolClient {
 public:
  struct Info {
    std::string host;
    uint16_t port;
  };

  ClusterSlotMigration(ClusterFamily* cl_fm, std::string host_ip, uint16_t port, Service* se,
                       std::vector<ClusterConfig::SlotRange> slots);
  ~ClusterSlotMigration();

  // Initiate connection with source node and create migration fiber
  std::error_code Start(ConnectionContext* cntx);
  Info GetInfo() const;
  uint32_t GetSyncId() const {
    return sync_id_;
  }

  // Remote sync ids uniquely identifies a sync *remotely*. However, multiple remote sources can
  // use the same id, so we need a local id as well.
  uint32_t GetLocalSyncId() const {
    return local_sync_id_;
  }

  MigrationState GetState() const {
    return state_;
  }

  void SetStableSyncForFlow(uint32_t flow);

  void Stop();

  const std::vector<ClusterConfig::SlotRange>& GetSlots() const {
    return slots_;
  }

 private:
  // Send DFLYMIGRATE CONF to the source and get info about migration process
  std::error_code Greet();
  void MainMigrationFb();
  // Creates flows, one per shard on the source node and manage migration process
  std::error_code InitiateSlotsMigration();

  // may be called after we finish all flows
  bool IsFinalized() const;

 private:
  ClusterFamily* cluster_family_;
  Service& service_;
  Mutex flows_op_mu_;
  std::vector<std::unique_ptr<ClusterShardMigration>> shard_flows_;
  std::vector<ClusterConfig::SlotRange> slots_;
  uint32_t source_shards_num_ = 0;
  uint32_t sync_id_ = 0;
  uint32_t local_sync_id_ = 0;
  MigrationState state_ = MigrationState::C_NO_STATE;

  Fiber sync_fb_;
};

}  // namespace dfly
