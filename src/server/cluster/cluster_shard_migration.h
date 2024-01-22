// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include "base/io_buf.h"
#include "server/journal/executor.h"
#include "server/protocol_client.h"

namespace dfly {

class Service;
class TransactionData;
class MultiShardExecution;

// ClusterShardMigration manage data receiving in slots migration process.
// It is created per shard on the target node to initiate FLOW step.
class ClusterShardMigration : public ProtocolClient {
 public:
  ClusterShardMigration(ServerContext server_context, uint32_t shard_id, uint32_t sync_id,
                        Service* service);
  ~ClusterShardMigration();

  std::error_code StartSyncFlow(Context* cntx);
  void Cancel();

  void SetStableSync() {
    is_stable_sync_.store(true);
  }
  bool IsStableSync() {
    return is_stable_sync_.load();
  }

 private:
  void FullSyncShardFb(Context* cntx);
  void JoinFlow();

  void ExecuteTx(TransactionData&& tx_data, bool inserted_by_me, Context* cntx);
  void ExecuteTxWithNoShardSync(TransactionData&& tx_data, Context* cntx);

 private:
  uint32_t source_shard_id_;
  uint32_t sync_id_;
  std::optional<base::IoBuf> leftover_buf_;
  std::unique_ptr<JournalExecutor> executor_;
  Fiber sync_fb_;
  std::atomic_bool is_stable_sync_ = false;
};

}  // namespace dfly
