// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <unordered_map>

#include "server/common.h"

namespace dfly {

class Service;

// Coordinator for multi shard execution.
struct MultiShardExecution {
  bool InsertTxToSharedMap(TxId txid, uint32_t shard_cnt);

  Mutex map_mu;

  struct TxExecutionSync {
    Barrier barrier;
    std::atomic_uint32_t counter;
    BlockingCounter block;

    explicit TxExecutionSync(uint32_t counter)
        : barrier(counter), counter(counter), block(counter) {
    }
  };

  std::unordered_map<TxId, TxExecutionSync> tx_sync_execution;
};

}  // namespace dfly
