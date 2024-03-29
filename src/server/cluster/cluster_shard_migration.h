// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include "base/io_buf.h"
#include "server/journal/executor.h"

namespace dfly {

class Service;
struct TransactionData;
class MultiShardExecution;

// ClusterShardMigration manage data receiving in slots migration process.
// It is created per shard on the target node to initiate FLOW step.
class ClusterShardMigration {
 public:
  ClusterShardMigration(uint32_t local_sync_id, uint32_t shard_id, Service* service);
  ~ClusterShardMigration();

  void Start(Context* cntx, io::Source* source);

 private:
  void FullSyncShardFb(Context* cntx, io::Source* source);

  void ExecuteTx(TransactionData&& tx_data, bool inserted_by_me, Context* cntx);
  void ExecuteTxWithNoShardSync(TransactionData&& tx_data, Context* cntx);

 private:
  uint32_t source_shard_id_;
  std::optional<base::IoBuf> leftover_buf_;
  std::unique_ptr<JournalExecutor> executor_;
};

}  // namespace dfly
