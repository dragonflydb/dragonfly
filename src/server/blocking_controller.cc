// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/blocking_controller.h"

#include <absl/container/inlined_vector.h>

#include <boost/smart_ptr/intrusive_ptr.hpp>

#include "base/logging.h"
#include "server/engine_shard_set.h"
#include "server/namespaces.h"
#include "server/transaction.h"

namespace dfly {

using namespace std;

struct WatchItem {
  Transaction* trans;
  KeyReadyChecker key_ready_checker;

  Transaction* get() const {
    return trans;
  }

  WatchItem(Transaction* t, KeyReadyChecker krc) : trans(t), key_ready_checker(std::move(krc)) {
  }
};

struct BlockingController::WatchQueue {
  deque<WatchItem> items;
  TxId notify_txid = UINT64_MAX;

  // Updated  by both coordinator and shard threads but at different times.
  enum State { SUSPENDED, ACTIVE } state = SUSPENDED;

  void Suspend() {
    state = SUSPENDED;
    notify_txid = UINT64_MAX;
  }

  auto Find(Transaction* tx) const {
    return find_if(items.begin(), items.end(),
                   [tx](const WatchItem& wi) { return wi.get() == tx; });
  }
};

// Watch state per db.
struct BlockingController::DbWatchTable {
  WatchQueueMap queue_map;

  // awakened keys point to blocked keys that can potentially be unblocked.
  absl::flat_hash_set<std::string> awakened_keys;

  // returns true if awake event was added.
  // Requires that the key queue be in the required state.
  bool AddAwakeEvent(string_view key);

  // Returns true if awakened tx was removed from the queue.
  bool UnwatchTx(string_view key, Transaction* tx);
};

bool BlockingController::DbWatchTable::UnwatchTx(string_view key, Transaction* tx) {
  auto wq_it = queue_map.find(key);

  // With multiple same keys we may have misses because the first iteration
  // on the same key could remove the queue.
  if (wq_it == queue_map.end())
    return false;

  WatchQueue* wq = wq_it->second.get();
  DCHECK(!wq->items.empty());

  bool res = false;
  if (wq->state == WatchQueue::ACTIVE && wq->items.front().get() == tx) {
    wq->items.pop_front();

    // We suspend the queue and add keys to re-verification.
    // If they are still present, this queue will be reactivated below.
    wq->state = WatchQueue::SUSPENDED;

    if (!wq->items.empty())
      awakened_keys.insert(wq_it->first);  // send for further validation.
    res = true;
  } else {
    // tx can be is_awakened == true because of some other key and this queue would be
    // in suspended and we still need to clean it up.
    // the suspended item does not have to be the first one in the queue.
    // This shard has not been awakened and in case this transaction in the queue
    // we must clean it up.
    if (auto it = wq->Find(tx); it != wq->items.end()) {
      wq->items.erase(it);
    }
  }

  if (wq->items.empty()) {
    DVLOG(1) << "queue_map.erase";
    awakened_keys.erase(wq_it->first);
    queue_map.erase(wq_it);
  }
  return res;
}

BlockingController::BlockingController(EngineShard* owner, Namespace* ns) : owner_(owner), ns_(ns) {
}

BlockingController::~BlockingController() {
}

bool BlockingController::DbWatchTable::AddAwakeEvent(string_view key) {
  auto it = queue_map.find(key);

  if (it == queue_map.end() || it->second->state != WatchQueue::SUSPENDED)
    return false;  /// nobody watches this key or state does not match.

  return awakened_keys.insert(it->first).second;
}

// Removes tx from its watch queues if tx appears there.
void BlockingController::FinalizeWatched(Keys keys, Transaction* tx) {
  DCHECK(tx);
  VLOG(1) << "FinalizeBlocking [" << owner_->shard_id() << "]" << tx->DebugId();

  bool removed = awakened_transactions_.erase(tx);
  DCHECK(!removed || (tx->DEBUG_GetLocalMask(owner_->shard_id()) & Transaction::AWAKED_Q));

  auto dbit = watched_dbs_.find(tx->GetDbIndex());

  // Can happen if it was the only transaction in the queue and it was notified and removed.
  if (dbit == watched_dbs_.end())
    return;

  DbWatchTable& wt = *dbit->second;

  // Add keys of processed transaction so we could awake the next one in the queue
  // in case those keys still exist.
  for (string_view key : base::it::Wrap(facade::kToSV, keys)) {
    bool removed_awakened = wt.UnwatchTx(key, tx);
    CHECK(!removed_awakened || removed)
        << tx->DebugId() << " " << key << " " << tx->DEBUG_GetLocalMask(owner_->shard_id());
  }

  if (wt.queue_map.empty()) {
    watched_dbs_.erase(dbit);
  }
  awakened_indices_.emplace(tx->GetDbIndex());
}

// Runs on the shard thread.
void BlockingController::NotifyPending() {
  const Transaction* tx = owner_->GetContTx();
  CHECK(tx == nullptr) << tx->DebugId();

  DbContext context;
  context.ns = ns_;
  context.time_now_ms = GetCurrentTimeMs();

  for (DbIndex index : awakened_indices_) {
    auto dbit = watched_dbs_.find(index);
    if (dbit == watched_dbs_.end())
      continue;

    context.db_index = index;
    DbWatchTable& wt = *dbit->second;
    for (const auto& key : wt.awakened_keys) {
      string_view sv_key = key;
      DVLOG(1) << "Processing awakened key " << sv_key;
      auto w_it = wt.queue_map.find(sv_key);
      if (w_it == wt.queue_map.end()) {
        // This should not happen because we remove keys from awakened_keys every type we remove
        // the entry from queue_map. TODO: to make it a CHECK after Dec 2024
        LOG(ERROR) << "Internal error: Key " << sv_key
                   << " was not found in the watch queue, wt.awakened_keys len is "
                   << wt.awakened_keys.size() << " wt.queue_map len is " << wt.queue_map.size();
        for (const auto& item : wt.awakened_keys) {
          LOG(ERROR) << "Awakened key: " << item;
        }

        continue;
      }

      CHECK(w_it != wt.queue_map.end());
      DVLOG(1) << "Notify WQ: [" << owner_->shard_id() << "] " << key;
      WatchQueue* wq = w_it->second.get();
      NotifyWatchQueue(sv_key, wq, context);
      if (wq->items.empty()) {
        // we erase awakened_keys right after this loop finishes running.
        wt.queue_map.erase(w_it);
      }
    }
    wt.awakened_keys.clear();

    if (wt.queue_map.empty()) {
      watched_dbs_.erase(dbit);
    }
  }
  awakened_indices_.clear();
}

void BlockingController::AddWatched(Keys watch_keys, KeyReadyChecker krc, Transaction* trans) {
  auto [dbit, added] = watched_dbs_.emplace(trans->GetDbIndex(), nullptr);
  if (added) {
    dbit->second.reset(new DbWatchTable);
  }

  DbWatchTable& wt = *dbit->second;

  for (auto key : base::it::Wrap(facade::kToSV, watch_keys)) {
    auto [res, inserted] = wt.queue_map.emplace(key, nullptr);
    if (inserted) {
      res->second.reset(new WatchQueue);
    }

    if (!res->second->items.empty()) {
      Transaction* last = res->second->items.back().get();
      DCHECK_GT(last->GetUseCount(), 0u);

      // Duplicate keys case. We push only once per key.
      if (last == trans)
        continue;
    }
    DVLOG(2) << "Emplace " << trans->DebugId() << " to watch " << key;
    res->second->items.emplace_back(trans, krc);
  }
}

// Called from commands like lpush.
void BlockingController::AwakeWatched(DbIndex db_index, string_view db_key) {
  auto it = watched_dbs_.find(db_index);
  if (it == watched_dbs_.end())
    return;

  DbWatchTable& wt = *it->second;
  DCHECK(!wt.queue_map.empty());

  if (wt.AddAwakeEvent(db_key)) {
    VLOG(1) << "AwakeWatched: db(" << db_index << ") " << db_key;

    awakened_indices_.insert(db_index);
  }
}

// Marks the queue as active and notifies the first transaction in the queue.
void BlockingController::NotifyWatchQueue(std::string_view key, WatchQueue* wq,
                                          const DbContext& context) {
  DCHECK_EQ(wq->state, WatchQueue::SUSPENDED);

  auto& queue = wq->items;
  ShardId sid = owner_->shard_id();

  // In the most cases we shouldn't have skipped elements at all
  absl::InlinedVector<dfly::WatchItem, 4> skipped;
  while (!queue.empty()) {
    auto& wi = queue.front();
    Transaction* head = wi.get();
    // We check may the transaction be notified otherwise move it to the end of the queue
    if (wi.key_ready_checker(owner_, context, head, key)) {
      DVLOG(2) << "WQ-Pop " << head->DebugId() << " from key " << key;
      if (head->NotifySuspended(owner_->committed_txid(), sid, key)) {
        wq->state = WatchQueue::ACTIVE;
        // We deliberately keep the notified transaction in the queue to know which queue
        // must handled when this transaction finished.
        wq->notify_txid = owner_->committed_txid();
        awakened_transactions_.insert(head);
        break;
      }
    } else {
      skipped.push_back(std::move(wi));
    }

    queue.pop_front();
  }
  std::move(skipped.begin(), skipped.end(), std::back_inserter(queue));
}

size_t BlockingController::NumWatched(DbIndex db_indx) const {
  auto it = watched_dbs_.find(db_indx);
  if (it == watched_dbs_.end())
    return 0;

  return it->second->queue_map.size();
}

vector<string> BlockingController::GetWatchedKeys(DbIndex db_indx) const {
  vector<string> res;
  auto it = watched_dbs_.find(db_indx);

  if (it != watched_dbs_.end()) {
    for (const auto& k_v : it->second->queue_map) {
      res.push_back(k_v.first);
    }
  }

  return res;
}

}  // namespace dfly
