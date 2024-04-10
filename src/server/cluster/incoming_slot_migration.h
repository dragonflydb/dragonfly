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
// Manage connections between the target and source node,
// manage migration process state and data
class IncomingSlotMigration {
 public:
  IncomingSlotMigration(std::string source_id, Service* se, SlotRanges slots, uint32_t shards_num);
  ~IncomingSlotMigration();

  // process data from FDLYMIGRATE FLOW cmd
  void StartFlow(uint32_t shard, io::Source* source);
  // wait untill all flows are got FIN opcode
  void Join();

  MigrationState GetState() const {
    return state_;
  }

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
  MigrationState state_ = MigrationState::C_NO_STATE;
  Context cntx_;

  util::fb2::BlockingCounter bc_;
  util::fb2::Fiber sync_fb_;
};

}  // namespace dfly
