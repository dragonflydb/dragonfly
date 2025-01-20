// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/btree_map.h>

#include <string>

#include "facade/conn_context.h"
#include "server/cluster/cluster_config.h"
#include "server/cluster/incoming_slot_migration.h"
#include "server/cluster/outgoing_slot_migration.h"
#include "server/common.h"

namespace facade {
class SinkReplyBuilder;
}  // namespace facade

namespace dfly {
class ServerFamily;
class CommandRegistry;
class ConnectionContext;
}  // namespace dfly

namespace dfly::cluster {

class ClusterFamily {
 public:
  explicit ClusterFamily(ServerFamily* server_family);

  void Register(CommandRegistry* registry);

  void Shutdown() ABSL_LOCKS_EXCLUDED(set_config_mu);

  // Returns a thread-local pointer.
  static ClusterConfig* cluster_config();

  void ApplyMigrationSlotRangeToConfig(std::string_view node_id, const SlotRanges& slots,
                                       bool is_outgoing);

  const std::string& MyID() const {
    return id_;
  }

  // Only for debug purpose. Pause/Resume all incoming migrations
  void PauseAllIncomingMigrations(bool pause);

 private:
  using SinkReplyBuilder = facade::SinkReplyBuilder;

  // Cluster commands compatible with Redis
  void Cluster(CmdArgList args, const CommandContext& cmd_cntx);
  void ClusterHelp(SinkReplyBuilder* builder);
  void ClusterShards(SinkReplyBuilder* builder, ConnectionContext* cntx);
  void ClusterSlots(SinkReplyBuilder* builder, ConnectionContext* cntx);
  void ClusterNodes(SinkReplyBuilder* builder, ConnectionContext* cntx);
  void ClusterInfo(SinkReplyBuilder* builder, ConnectionContext* cntx);
  void ClusterMyId(SinkReplyBuilder* builder);

  void KeySlot(CmdArgList args, SinkReplyBuilder* builder);

  void ReadOnly(CmdArgList args, const CommandContext& cmd_cntx);
  void ReadWrite(CmdArgList args, const CommandContext& cmd_cntx);

  // Custom Dragonfly commands for cluster management
  void DflyCluster(CmdArgList args, const CommandContext& cmd_cntx);
  void DflyClusterConfig(CmdArgList args, SinkReplyBuilder* builder, ConnectionContext* cntx);

  void DflyClusterGetSlotInfo(CmdArgList args, SinkReplyBuilder* builder)
      ABSL_LOCKS_EXCLUDED(migration_mu_);
  void DflyClusterFlushSlots(CmdArgList args, SinkReplyBuilder* builder);

 private:  // Slots migration section
  void DflySlotMigrationStatus(CmdArgList args, SinkReplyBuilder* builder)
      ABSL_LOCKS_EXCLUDED(migration_mu_);

  // DFLYMIGRATE is internal command defines several steps in slots migrations process
  void DflyMigrate(CmdArgList args, const CommandContext& cmd_cntx);

  // DFLYMIGRATE INIT is internal command to create incoming migration object
  void InitMigration(CmdArgList args, SinkReplyBuilder* builder) ABSL_LOCKS_EXCLUDED(migration_mu_);

  // DFLYMIGRATE FLOW initiate second step in slots migration procedure
  // this request should be done for every shard on the target node
  // this method assocciate connection and shard that will be the data
  // source for migration
  void DflyMigrateFlow(CmdArgList args, SinkReplyBuilder* builder, ConnectionContext* cntx);

  void DflyMigrateAck(CmdArgList args, SinkReplyBuilder* builder);

  std::shared_ptr<IncomingSlotMigration> GetIncomingMigration(std::string_view source_id)
      ABSL_LOCKS_EXCLUDED(migration_mu_);

  void StartSlotMigrations(std::vector<MigrationInfo> migrations);

  // must be destroyed excluded set_config_mu and migration_mu_ locks
  struct PreparedToRemoveOutgoingMigrations {
    std::vector<std::shared_ptr<OutgoingMigration>> migrations;
    SlotRanges slot_ranges;
    ~PreparedToRemoveOutgoingMigrations() ABSL_LOCKS_EXCLUDED(migration_mu_, set_config_mu);
  };

  [[nodiscard]] PreparedToRemoveOutgoingMigrations TakeOutOutgoingMigrations(
      std::shared_ptr<ClusterConfig> new_config, std::shared_ptr<ClusterConfig> old_config)
      ABSL_LOCKS_EXCLUDED(migration_mu_);
  void RemoveIncomingMigrations(const std::vector<MigrationInfo>& migrations)
      ABSL_LOCKS_EXCLUDED(migration_mu_);

  // store info about migration and create unique session id
  std::shared_ptr<OutgoingMigration> CreateOutgoingMigration(MigrationInfo info)
      ABSL_LOCKS_EXCLUDED(migration_mu_);

  mutable util::fb2::Mutex migration_mu_;  // guard migrations operations
  // holds all incoming slots migrations that are currently in progress.
  std::vector<std::shared_ptr<IncomingSlotMigration>> incoming_migrations_jobs_
      ABSL_GUARDED_BY(migration_mu_);

  // holds all outgoing slots migrations that are currently in progress
  std::vector<std::shared_ptr<OutgoingMigration>> outgoing_migration_jobs_
      ABSL_GUARDED_BY(migration_mu_);

 private:
  std::optional<ClusterShardInfos> GetShardInfos(ConnectionContext* cntx) const;

  ClusterShardInfo GetEmulatedShardInfo(ConnectionContext* cntx) const;

  // Guards set configuration, so that we won't handle 2 in parallel.
  mutable util::fb2::Mutex set_config_mu;

  std::string id_;

  ServerFamily* server_family_ = nullptr;
};

}  // namespace dfly::cluster
