// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "tx_executor.h"

#include "base/logging.h"

using namespace std;

namespace dfly {

bool MultiShardExecution::InsertTxToSharedMap(TxId txid, uint32_t shard_cnt) {
  std::unique_lock lk(map_mu);
  auto [it, was_insert] = tx_sync_execution.emplace(txid, shard_cnt);
  lk.unlock();

  VLOG(2) << "txid: " << txid << " unique_shard_cnt_: " << shard_cnt
          << " was_insert: " << was_insert;
  it->second.block.Dec();

  return was_insert;
}

}  // namespace dfly
