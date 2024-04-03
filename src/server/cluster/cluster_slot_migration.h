// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include "helio/io/io.h"
#include "server/cluster/cluster_config.h"

namespace dfly {
class ClusterShardMigration;

class Service;

// The main entity on the target side that manage slots migration process
// Creates initial connection between the target and source node,
// manage migration process state and data
class ClusterSlotMigration {
 public:
  ClusterSlotMigration(std::string source_id, Service* se, SlotRanges slots, uint32_t shards_num);
  ~ClusterSlotMigration();

  void StartFlow(uint32_t shard, io::Source* source);

  // Remote sync ids uniquely identifies a sync *remotely*. However, multiple remote sources can
  // use the same id, so we need a local id as well.
  uint32_t GetLocalSyncId() const {
    return local_sync_id_;
  }

  MigrationState GetState() const {
    return state_;
  }

  void Join();

  const SlotRanges& GetSlots() const {
    return slots_;
  }

  const std::string GetSourceID() const {
    return source_id_;
  }

 private:
  std::string source_id_;
  Service& service_;
  std::vector<std::unique_ptr<ClusterShardMigration>> shard_flows_;
  SlotRanges slots_;
  uint32_t local_sync_id_ = 0;
  MigrationState state_ = MigrationState::C_NO_STATE;
  Context cntx_;
  std::vector<std::vector<unsigned>> partitions_;

  util::fb2::BlockingCounter bc_;
  util::fb2::Fiber sync_fb_;
};

}  // namespace dfly
