// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/btree_map.h>
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>

#include "base/string_view_sso.h"
#include "server/common.h"
#include "server/tx_base.h"

namespace dfly {

class Transaction;
class Namespace;

// Used for tracking keys of blocking transactions and properly notifying them.
// First, keys are marked as watched and associated with an owner transaction. A mutating
// transaction marks them as touched, and once it concludes, the watching transactions are notified.
class BlockingController {
 public:
  explicit BlockingController(EngineShard* owner, Namespace* ns);
  ~BlockingController();

  using Keys = ShardArgs;

  bool HasAwakedTransaction() const {
    return !awakened_transactions_.empty();
  }

  const auto& awakened_transactions() const {
    return awakened_transactions_;
  }

  // Associate given keys with transaction, checked via the krc checker
  void AddWatched(Keys watch_keys, KeyReadyChecker krc, Transaction* me);

  // Remove transaction from watching these keys
  void RemovedWatched(Keys keys, Transaction* tx);

  // Mark given key as touched. Called by commands mutating this key.
  void Touch(DbIndex db_index, std::string_view key);

  // Notify transactions of touched keys
  void NotifyPending();

  // Used in tests and debugging functions.
  size_t NumWatched(DbIndex db_indx) const;
  std::vector<std::string> GetWatchedKeys(DbIndex db_indx) const;

 private:
  struct WatchQueue;
  struct DbWatchTable;

  void NotifyWatchQueue(std::string_view key, WatchQueue* wqm, const DbContext& context);

  EngineShard* owner_;
  Namespace* ns_;

  absl::flat_hash_map<DbIndex, DbWatchTable> watched_dbs_;
  absl::flat_hash_set<DbIndex> awakened_indices_;  // watched_dbs_ with awakened keys

  // tracks currently notified and awaked transactions.
  // There can be multiple transactions like this because a transaction
  // could awaken arbitrary number of keys.
  absl::flat_hash_set<Transaction*> awakened_transactions_;
};
}  // namespace dfly
