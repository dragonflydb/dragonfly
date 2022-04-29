// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/btree_map.h>
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>

#include "base/string_view_sso.h"
#include "server/common.h"

namespace dfly {

class Transaction;

class BlockingController {
 public:
  explicit BlockingController(EngineShard* owner);
  ~BlockingController();

  bool HasAwakedTransaction() const {
    return !awakened_transactions_.empty();
  }

  // Iterates over awakened key candidates in each db and moves verified ones into
  // global verified_awakened_ array.
  // Returns true if there are active awakened keys, false otherwise.
  // It has 2 responsibilities.
  // 1: to go over potential wakened keys, verify them and activate watch queues.
  // 2: if t is awaked and finished running - to remove it from the head
  //    of the queue and notify the next one.
  //    If t is null then second part is omitted.
  void RunStep(Transaction* t);

  // Blocking API
  // TODO: consider moving all watched functions to
  // EngineShard with separate per db map.
  //! AddWatched adds a transaction to the blocking queue.
  void AddWatched(Transaction* me);
  void RemoveWatched(Transaction* me);

  // Called from operations that create keys like lpush, rename etc.
  void AwakeWatched(DbIndex db_index, std::string_view db_key);

  // void OnTxFinish();

  // void RegisterAwaitForConverge(Transaction* t);

  size_t NumWatched(DbIndex db_indx) const;

 private:
  struct WatchQueue;
  struct DbWatchTable;

  using WatchQueueMap = absl::flat_hash_map<std::string, std::unique_ptr<WatchQueue>>;

  /// Returns the notified transaction,
  /// or null if all transactions in the queue have expired..
  void NotifyWatchQueue(WatchQueue* wq);

  // void NotifyConvergence(Transaction* tx);

  EngineShard* owner_;

  absl::flat_hash_map<DbIndex, std::unique_ptr<DbWatchTable>> watched_dbs_;

  // serves as a temporary queue that aggregates all the possible awakened dbs.
  // flushed by RunStep().
  absl::flat_hash_set<DbIndex> awakened_indices_;

  // tracks currently notified and awaked transactions.
  // There can be multiple transactions like this because a transaction
  // could awaken arbitrary number of keys.
  absl::flat_hash_set<Transaction*> awakened_transactions_;

  // absl::btree_multimap<TxId, Transaction*> waiting_convergence_;
};
}  // namespace dfly
