// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include "io/io.h"
#include "server/cluster/cluster_config.h"
#include "server/common.h"
#include "server/protocol_client.h"

namespace dfly {

namespace journal {
class Journal;
}

class DbSlice;
class ServerFamily;
class ClusterFamily;

// Whole outgoing slots migration manager
class OutgoingMigration : private ProtocolClient {
 public:
  OutgoingMigration(MigrationInfo info, ClusterFamily* cf, Context::ErrHandler err_handler,
                    ServerFamily* sf);
  ~OutgoingMigration();

  // start migration process, sends INIT command to the target node
  std::error_code Start(ConnectionContext* cntx);

  // should be run for all shards
  void StartFlow(journal::Journal* journal, io::Sink* dest);

  void Cancel();

  MigrationState GetState() const;

  const std::string& GetHostIp() const {
    return server().host;
  };

  uint16_t GetPort() const {
    return server().port;
  };

  const SlotRanges& GetSlots() const {
    return migration_info_.slot_ranges;
  }

  const MigrationInfo GetMigrationInfo() const {
    return migration_info_;
  }

  static constexpr long kInvalidAttempt = -1;

 private:
  MigrationState GetStateImpl() const;
  // SliceSlotMigration manages state and data transfering for the corresponding shard
  class SliceSlotMigration;

  void SyncFb();
  bool FinishMigration(long attempt);

 private:
  MigrationInfo migration_info_;
  Context cntx_;
  mutable util::fb2::Mutex flows_mu_;
  std::vector<std::unique_ptr<SliceSlotMigration>> slot_migrations_ ABSL_GUARDED_BY(flows_mu_);
  ServerFamily* server_family_;
  ClusterFamily* cf_;

  util::fb2::Fiber main_sync_fb_;

  // Atomic only for simple read operation, writes - from the same thread, reads - from any thread
  std::atomic<MigrationState> state_ = MigrationState::C_NO_STATE;
};

}  // namespace dfly
