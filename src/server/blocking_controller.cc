// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/blocking_controller.h"

#include <boost/smart_ptr/intrusive_ptr.hpp>

extern "C" {
#include "redis/object.h"
}

#include "base/logging.h"
#include "server/engine_shard_set.h"
#include "server/transaction.h"

namespace dfly {

using namespace std;

struct WatchItem {
  Transaction* trans;

  Transaction* get() const {
    return trans;
  }

  WatchItem(Transaction* t) : trans(t) {
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
  // they reference key objects in queue_map.
  absl::flat_hash_set<base::string_view_sso> awakened_keys;

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
    queue_map.erase(wq_it);
  }
  return res;
}

BlockingController::BlockingController(EngineShard* owner) : owner_(owner) {
}

BlockingController::~BlockingController() {
}

bool BlockingController::DbWatchTable::AddAwakeEvent(string_view key) {
  auto it = queue_map.find(key);

  if (it == queue_map.end() || it->second->state != WatchQueue::SUSPENDED)
    return false;  /// nobody watches this key or state does not match.

  string_view dbkey = it->first;

  return awakened_keys.insert(dbkey).second;
}

// Optionally removes tx from the front of the watch queues.
void BlockingController::FinalizeWatched(KeyLockArgs lock_args, Transaction* tx) {
  DCHECK(tx);

  ShardId sid = owner_->shard_id();

  uint16_t local_mask = tx->GetLocalMask(sid);
  VLOG(1) << "FinalizeBlocking [" << sid << "]" << tx->DebugId() << " " << local_mask;

  bool is_awakened = local_mask & Transaction::AWAKED_Q;

  if (is_awakened)
    awakened_transactions_.erase(tx);

  auto dbit = watched_dbs_.find(tx->GetDbIndex());

  // Can happen if it was the only transaction in the queue and it was notified and removed.
  if (dbit == watched_dbs_.end())
    return;

  DbWatchTable& wt = *dbit->second;

  // Add keys of processed transaction so we could awake the next one in the queue
  // in case those keys still exist.
  for (size_t i = 0; i < lock_args.args.size(); i += lock_args.key_step) {
    string_view key = lock_args.args[i];
    bool removed_awakened = wt.UnwatchTx(key, tx);
    if (removed_awakened) {
      CHECK(is_awakened) << tx->DebugId() << " " << key << " " << local_mask;
    }
  }

  if (wt.queue_map.empty()) {
    watched_dbs_.erase(dbit);
  }
  awakened_indices_.emplace(tx->GetDbIndex());
}

// Similar function but with ArgSlice. TODO: to fix the duplication.
void BlockingController::FinalizeWatched(ArgSlice args, Transaction* tx) {
  DCHECK(tx);

  ShardId sid = owner_->shard_id();

  VLOG(1) << "FinalizeBlocking [" << sid << "]" << tx->DebugId();

  uint16_t local_mask = tx->GetLocalMask(sid);
  bool is_awakened = local_mask & Transaction::AWAKED_Q;

  if (is_awakened)
    awakened_transactions_.erase(tx);

  auto dbit = watched_dbs_.find(tx->GetDbIndex());

  // Can happen if it was the only transaction in the queue and it was notified and removed.
  if (dbit == watched_dbs_.end())
    return;

  DbWatchTable& wt = *dbit->second;

  // Add keys of processed transaction so we could awake the next one in the queue
  // in case those keys still exist.
  for (string_view key : args) {
    bool removed_awakened = wt.UnwatchTx(key, tx);
    if (removed_awakened) {
      CHECK(is_awakened) << tx->DebugId() << " " << key << " " << local_mask;
    }
  }

  if (wt.queue_map.empty()) {
    watched_dbs_.erase(dbit);
  }
  awakened_indices_.emplace(tx->GetDbIndex());
}

void BlockingController::NotifyPending() {
  DbContext context;
  context.time_now_ms = GetCurrentTimeMs();

  for (DbIndex index : awakened_indices_) {
    auto dbit = watched_dbs_.find(index);
    if (dbit == watched_dbs_.end())
      continue;

    context.db_index = index;
    DbWatchTable& wt = *dbit->second;
    for (auto key : wt.awakened_keys) {
      string_view sv_key = static_cast<string_view>(key);
      DVLOG(1) << "Processing awakened key " << sv_key;

      // Double verify we still got the item.
      auto [it, exp_it] = owner_->db_slice().FindExt(context, sv_key);
      if (!IsValid(it) ||
          !(it->second.ObjType() == OBJ_LIST ||
            it->second.ObjType() == OBJ_ZSET))  // Only LIST and ZSET are allowed to block.
        continue;

      NotifyWatchQueue(sv_key, &wt.queue_map);
    }
    wt.awakened_keys.clear();

    if (wt.queue_map.empty()) {
      watched_dbs_.erase(dbit);
    }
  }
  awakened_indices_.clear();
}

void BlockingController::AddWatched(ArgSlice keys, Transaction* trans) {
  auto [dbit, added] = watched_dbs_.emplace(trans->GetDbIndex(), nullptr);
  if (added) {
    dbit->second.reset(new DbWatchTable);
  }

  DbWatchTable& wt = *dbit->second;

  for (auto key : keys) {
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
    res->second->items.emplace_back(trans);
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
void BlockingController::NotifyWatchQueue(std::string_view key, WatchQueueMap* wqm) {
  auto w_it = wqm->find(key);
  CHECK(w_it != wqm->end());
  DVLOG(1) << "Notify WQ: [" << owner_->shard_id() << "] " << key;
  WatchQueue* wq = w_it->second.get();

  DCHECK_EQ(wq->state, WatchQueue::SUSPENDED);
  wq->state = WatchQueue::ACTIVE;

  auto& queue = wq->items;
  ShardId sid = owner_->shard_id();

  do {
    WatchItem& wi = queue.front();
    Transaction* head = wi.get();
    DVLOG(2) << "WQ-Pop " << head->DebugId() << " from key " << key;

    if (head->NotifySuspended(owner_->committed_txid(), sid, key)) {
      // We deliberately keep the notified transaction in the queue to know which queue
      // must handled when this transaction finished.
      wq->notify_txid = owner_->committed_txid();
      awakened_transactions_.insert(head);
      break;
    }

    queue.pop_front();
  } while (!queue.empty());

  if (wq->items.empty()) {
    wqm->erase(w_it);
  }
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
