// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include "helio/io/io.h"
#include "helio/util/fiber_socket_base.h"
#include "server/cluster/cluster_defs.h"
#include "server/common.h"

namespace dfly {
class Service;
}

namespace dfly::cluster {
class ClusterShardMigration;

// The main entity on the target side that manage slots migration process
// Manage connections between the target and source node,
// manage migration process state and data
class IncomingSlotMigration {
 public:
  IncomingSlotMigration(std::string source_id, Service* se, SlotRanges slots, uint32_t shards_num);
  ~IncomingSlotMigration();

  // process data from FDLYMIGRATE FLOW cmd
  // executes until Stop called or connection closed
  void StartFlow(uint32_t shard, util::FiberSocketBase* source);

  // Waits until all flows got FIN opcode.
  // returns true if we joined false if timeout is readed
  // After Join we still can get data due to error situation
  [[nodiscard]] bool Join(long attempt);

  // Stop and join the migration, can be called even after migration is finished
  void Stop();

  MigrationState GetState() const {
    return state_.load();
  }

  const SlotRanges& GetSlots() const {
    return slots_;
  }

  const std::string& GetSourceID() const {
    return source_id_;
  }

  void ReportError(dfly::GenericError err) ABSL_LOCKS_EXCLUDED(error_mu_) {
    util::fb2::LockGuard lk(error_mu_);
    last_error_ = std::move(err);
  }

  std::string GetErrorStr() const ABSL_LOCKS_EXCLUDED(error_mu_) {
    util::fb2::LockGuard lk(error_mu_);
    return last_error_.Format();
  }

  size_t GetKeyCount() const;

  void Pause(bool pause);

 private:
  std::string source_id_;
  Service& service_;
  std::vector<std::unique_ptr<ClusterShardMigration>> shard_flows_;
  SlotRanges slots_;
  std::atomic<MigrationState> state_ = MigrationState::C_CONNECTING;
  Context cntx_;
  mutable util::fb2::Mutex error_mu_;
  dfly::GenericError last_error_ ABSL_GUARDED_BY(error_mu_);

  // when migration is finished we need to store number of migrated keys
  // because new request can add or remove keys and we get incorrect statistic
  size_t keys_number_ = 0;

  util::fb2::BlockingCounter bc_;
};

}  // namespace dfly::cluster
