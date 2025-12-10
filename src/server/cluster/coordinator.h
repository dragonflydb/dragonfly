// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include "server/cluster/cluster_defs.h"
#include "server/protocol_client.h"
#include "util/fibers/future.h"

namespace dfly::cluster {

// Coordinator needs to create and manage connections between nodes in the cluster for cross shard
// commands. All cross-shard commands are dispatched through the Coordinator.
// It can be used to exeute commands on all shards or specific shards.
class Coordinator {
 public:
  using RespCB = std::function<void(const OwnedRespExpr::Vec&)>;  // TODO add error.

  static Coordinator& Current();
  [[nodiscard]] util::fb2::Future<GenericError> DispatchAll(std::string command, RespCB cb);

  void Shutdown() {
    // TODO add proper shutdown logic. We need to prevent new clients creation. Maybe we need to
    // wait destroying of existing clients.
    clients_.clear();
  }

 private:
  Coordinator() = default;
  class CrossShardClient;
  class CrossShardRequest;
  using CrossShardRequestPtr = std::shared_ptr<Coordinator::CrossShardRequest>;
  std::shared_ptr<CrossShardClient> GetClient(const std::string& host, uint16_t port);
  std::vector<std::shared_ptr<CrossShardClient>> clients_;
};

}  // namespace dfly::cluster
