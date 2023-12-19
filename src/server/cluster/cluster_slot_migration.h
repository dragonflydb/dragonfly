// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include "server/cluster/cluster_shard_migration.h"
#include "server/protocol_client.h"

namespace dfly {

class ClusterSlotMigration : ProtocolClient {
 public:
  enum State : uint8_t { C_NO_STATE, C_CONNECTING, C_FULL_SYNC, C_STABLE_SYNC };

  struct Info {
    std::string host;
    uint16_t port;
    State state;
  };

  ClusterSlotMigration(std::string host_ip, uint16_t port,
                       std::vector<ClusterConfig::SlotRange> slots);
  ~ClusterSlotMigration();

  std::error_code Start(ConnectionContext* cntx);
  Info GetInfo() const;

 private:
  void MainMigrationFb();
  std::error_code InitiateDflyFullSync();

 private:
  Mutex flows_op_mu_;
  std::vector<std::unique_ptr<ClusterShardMigration>> shard_flows_;
  std::error_code Greet();
  std::vector<ClusterConfig::SlotRange> slots_;
  uint32_t source_shards_num_ = 0;
  uint32_t sync_id_ = 0;
  State state_ = C_NO_STATE;

  Fiber sync_fb_;
};

}  // namespace dfly
