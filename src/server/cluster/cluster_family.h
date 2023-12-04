// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <string>

#include "facade/conn_context.h"
#include "server/cluster/cluster_config.h"
#include "server/cluster/cluster_slot_migration.h"
#include "server/common.h"

namespace dfly {
class CommandRegistry;
class ConnectionContext;
class ServerFamily;
class DflyCmd;

class ClusterFamily {
 public:
  explicit ClusterFamily(ServerFamily* server_family);

  void Register(CommandRegistry* registry);

  // Returns a thread-local pointer.
  ClusterConfig* cluster_config();

 private:
  // Cluster commands compatible with Redis
  void Cluster(CmdArgList args, ConnectionContext* cntx);
  void ClusterHelp(ConnectionContext* cntx);
  void ClusterShards(ConnectionContext* cntx);
  void ClusterSlots(ConnectionContext* cntx);
  void ClusterNodes(ConnectionContext* cntx);
  void ClusterInfo(ConnectionContext* cntx);

  void KeySlot(CmdArgList args, ConnectionContext* cntx);

  void ReadOnly(CmdArgList args, ConnectionContext* cntx);
  void ReadWrite(CmdArgList args, ConnectionContext* cntx);

  // Custom Dragonfly commands for cluster management
  void DflyCluster(CmdArgList args, ConnectionContext* cntx);
  void DflyClusterConfig(CmdArgList args, ConnectionContext* cntx);
  void DflyClusterGetSlotInfo(CmdArgList args, ConnectionContext* cntx);
  void DflyClusterMyId(CmdArgList args, ConnectionContext* cntx);
  void DflyClusterFlushSlots(CmdArgList args, ConnectionContext* cntx);
  void DflyClusterStartSlotMigration(CmdArgList args, ConnectionContext* cntx);
  void DflySlotMigrationStatus(CmdArgList args, ConnectionContext* cntx);
  void DflyMigrate(CmdArgList args, ConnectionContext* cntx);

  void MigrationConf(CmdArgList args, ConnectionContext* cntx);
  ClusterSlotMigration* AddMigration(std::string host_ip, uint16_t port,
                                     std::vector<ClusterConfig::SlotRange> slots);

  ClusterConfig::ClusterShard GetEmulatedShardInfo(ConnectionContext* cntx) const;

  ServerFamily* server_family_ = nullptr;

  mutable Mutex migrations_jobs_mu_;
  // holds all slot migrations that are currently in progress.
  std::vector<std::unique_ptr<ClusterSlotMigration>> migrations_jobs_
      ABSL_GUARDED_BY(migrations_jobs_mu_);
};

}  // namespace dfly
