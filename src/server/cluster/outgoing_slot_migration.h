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

// Whole outgoing slots migration manager
class OutgoingMigration : private ProtocolClient {
 public:
  OutgoingMigration() = default;
  ~OutgoingMigration();
  OutgoingMigration(std::string ip, uint16_t port, SlotRanges slots, uint32_t sync_id,
                    Context::ErrHandler, ServerFamily* sf);

  std::error_code Start(ConnectionContext* cntx);

  // should be run for all shards
  void StartFlow(journal::Journal* journal, io::Sink* dest);

  void Finalize(uint32_t shard_id);
  void Cancel(uint32_t shard_id);

  MigrationState GetState() const;

  const std::string& GetHostIp() const {
    return host_ip_;
  };

  uint16_t GetPort() const {
    return port_;
  };

  const SlotRanges& GetSlots() const {
    return slots_;
  }

 private:
  MigrationState GetStateImpl() const;
  // SliceSlotMigration manages state and data transfering for the corresponding shard
  class SliceSlotMigration;

  void SyncFb();

 private:
  std::string host_ip_;
  uint16_t port_;
  uint32_t sync_id_;
  SlotRanges slots_;
  Context cntx_;
  mutable util::fb2::Mutex flows_mu_;
  std::vector<std::unique_ptr<SliceSlotMigration>> slot_migrations_ ABSL_GUARDED_BY(flows_mu_);
  ServerFamily* server_family_;

  util::fb2::Fiber main_sync_fb_;
};

}  // namespace dfly
