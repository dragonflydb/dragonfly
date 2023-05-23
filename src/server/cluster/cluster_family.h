// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <memory>
#include <string>

#include "facade/conn_context.h"
#include "server/cluster/cluster_config.h"
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

  bool IsEnabledOrEmulated() const;

  ClusterConfig* cluster_config() {
    return cluster_config_.get();
  }

 private:
  // Cluster commands compatible with Redis
  void Cluster(CmdArgList args, ConnectionContext* cntx);
  void ClusterHelp(ConnectionContext* cntx);
  void ClusterSlots(ConnectionContext* cntx);
  void ClusterNodes(ConnectionContext* cntx);
  void ClusterInfo(ConnectionContext* cntx);

  void ReadOnly(CmdArgList args, ConnectionContext* cntx);
  void ReadWrite(CmdArgList args, ConnectionContext* cntx);

  // Custom Dragonfly commands for cluster management
  void DflyCluster(CmdArgList args, ConnectionContext* cntx);
  void DflyClusterConfig(CmdArgList args, ConnectionContext* cntx);
  void DflyClusterGetSlotInfo(CmdArgList args, ConnectionContext* cntx);
  void DflyClusterMyId(CmdArgList args, ConnectionContext* cntx);

  std::string BuildClusterNodeReply(ConnectionContext* cntx) const;

  bool is_emulated_cluster_ = false;
  ServerFamily* server_family_ = nullptr;

  std::unique_ptr<ClusterConfig> cluster_config_;
};

}  // namespace dfly
