// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include "server/protocol_client.h"

namespace dfly {

class ClusterSlotMigration : ProtocolClient {
 public:
  enum State : uint8_t { C_NO_STATE, C_CONNECTING, C_FULL_SYNC, C_STABLE_SYNC };

  struct Info {
    std::string host;
    uint16_t port;
    State state;
  };

  ClusterSlotMigration(std::string host_ip, uint16_t port,
                       std::vector<ClusterConfig::SlotRange> slots);
  ~ClusterSlotMigration();

  std::error_code Start(ConnectionContext* cntx);
  Info GetInfo() const;

 private:
  std::error_code Greet();
  std::vector<ClusterConfig::SlotRange> slots_;
  size_t souce_shards_num_ = 0;
  State state_ = C_NO_STATE;
};

}  // namespace dfly
