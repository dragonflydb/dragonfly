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

class BlockingController {
 public:
  explicit BlockingController(EngineShard* owner, Namespace* ns);
  ~BlockingController();

  using Keys = std::variant<ShardArgs, ArgSlice>;

  bool HasAwakedTransaction() const {
    return !awakened_transactions_.empty();
  }

  const auto& awakened_transactions() const {
    return awakened_transactions_;
  }

  void FinalizeWatched(Keys keys, Transaction* tx);

  // go over potential wakened keys, verify them and activate watch queues.
  void NotifyPending();

  // Blocking API
  // TODO: consider moving all watched functions to
  // EngineShard with separate per db map.
  //! AddWatched adds a transaction to the blocking queue.
  void AddWatched(Keys watch_keys, KeyReadyChecker krc, Transaction* me);

  // Called from operations that create keys like lpush, rename etc.
  void AwakeWatched(DbIndex db_index, std::string_view db_key);

  // Used in tests and debugging functions.
  size_t NumWatched(DbIndex db_indx) const;
  std::vector<std::string> GetWatchedKeys(DbIndex db_indx) const;

 private:
  struct WatchQueue;
  struct DbWatchTable;

  using WatchQueueMap = absl::flat_hash_map<std::string, std::unique_ptr<WatchQueue>>;

  void NotifyWatchQueue(std::string_view key, WatchQueue* wqm, const DbContext& context);

  // void NotifyConvergence(Transaction* tx);

  EngineShard* owner_;
  Namespace* ns_;

  absl::flat_hash_map<DbIndex, std::unique_ptr<DbWatchTable>> watched_dbs_;

  // serves as a temporary queue that aggregates all the possible awakened dbs.
  // flushed by RunStep().
  absl::flat_hash_set<DbIndex> awakened_indices_;

  // tracks currently notified and awaked transactions.
  // There can be multiple transactions like this because a transaction
  // could awaken arbitrary number of keys.
  absl::flat_hash_set<Transaction*> awakened_transactions_;
};
}  // namespace dfly
