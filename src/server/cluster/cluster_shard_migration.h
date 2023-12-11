// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include "base/io_buf.h"
#include "server/protocol_client.h"

namespace dfly {

class ClusterShardMigration : public ProtocolClient {
 public:
  ClusterShardMigration(ServerContext server_context, uint32_t shard_id);
  ~ClusterShardMigration();

  std::error_code StartSyncFlow(BlockingCounter sb, Context* cntx);
  void Cancel();

 private:
  uint32_t source_shard_id_;
  std::optional<base::IoBuf> leftover_buf_;
};

}  // namespace dfly
