// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include "server/cluster/cluster_defs.h"
#include "server/protocol_client.h"

namespace dfly::cluster {

// Coordinator needs to create and manage connections between nodes in the cluster for cross shard
// commands. All cross-shard commands are dispatched through the Coordinator.
// It can be used to exeute commands on all shards or specific shards.
class Coordinator {
 public:
  using RespCB = std::function<void(const facade::RespVec&)>;

  static Coordinator& Current();
  void DispatchAll(std::string_view command, RespCB cb);

 private:
  Coordinator() = default;
  class CrossShardClient;
};

}  // namespace dfly::cluster
