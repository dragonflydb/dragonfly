// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include "server/cluster/cluster_defs.h"
#include "server/protocol_client.h"

namespace dfly::cluster {

class Coordinator {
 public:
  static Coordinator& Current();
  void DispatchAll(std::string_view command);

 private:
  class CrossShardClient;
};

}  // namespace dfly::cluster
