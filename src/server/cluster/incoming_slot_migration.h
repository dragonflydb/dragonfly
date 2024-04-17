// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include "helio/io/io.h"
#include "helio/util/fiber_socket_base.h"
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
  // executes until Cancel called or connection closed
  void StartFlow(uint32_t shard, util::FiberSocketBase* source);

  // wait untill all flows are got FIN opcode
  // Join can be called several times and connection isn't closed
  // Connection can be closed automatically, or using Cancel
  // After Join we still can get data due to error situation
  void Join();

  void Cancel();

  MigrationState GetState() const {
    return state_.load();
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
  std::atomic<MigrationState> state_ = MigrationState::C_NO_STATE;
  Context cntx_;

  util::fb2::BlockingCounter bc_;
};

}  // namespace dfly
