// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include "base/io_buf.h"
#include "server/protocol_client.h"

namespace dfly {

class ClusterShardMigration : public ProtocolClient {
 public:
  ClusterShardMigration(ServerContext server_context, uint32_t shard_id, uint32_t sync_id);
  ~ClusterShardMigration();

  std::error_code StartSyncFlow(Context* cntx);
  void Cancel();

 private:
  void FullSyncShardFb(Context* cntx);
  void JoinFlow();

 private:
  uint32_t source_shard_id_;
  uint32_t sync_id_;
  std::optional<base::IoBuf> leftover_buf_;

  Fiber sync_fb_;
};

}  // namespace dfly
