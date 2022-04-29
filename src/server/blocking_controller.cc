// Copyright 2022, Roman Gershman.  All rights reserved.
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

  Transaction* get() {
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
};

// Watch state per db.
struct BlockingController::DbWatchTable {
  WatchQueueMap queue_map;

  // awakened keys point to blocked keys that can potentially be unblocked.
  // they reference key objects in queue_map.
  absl::flat_hash_set<base::string_view_sso> awakened_keys;

  void RemoveEntry(WatchQueueMap::iterator it);

  // returns true if awake event was added.
  // Requires that the key queue be in the required state.
  bool AddAwakeEvent(WatchQueue::State cur_state, string_view key);
};

BlockingController::BlockingController(EngineShard* owner) : owner_(owner) {
}

BlockingController::~BlockingController() {
}

void BlockingController::DbWatchTable::RemoveEntry(WatchQueueMap::iterator it) {
  DVLOG(1) << "Erasing watchqueue key " << it->first;

  awakened_keys.erase(it->first);
  queue_map.erase(it);
}

bool BlockingController::DbWatchTable::AddAwakeEvent(WatchQueue::State cur_state, string_view key) {
  auto it = queue_map.find(key);

  if (it == queue_map.end() || it->second->state != cur_state)
    return false;  /// nobody watches this key or state does not match.

  string_view dbkey = it->first;

  return awakened_keys.insert(dbkey).second;
}

// Processes potentially awakened keys and verifies that these are indeed
// awakened to eliminate false positives.
// In addition, optionally removes completed_t from the front of the watch queues.
void BlockingController::RunStep(Transaction* completed_t) {
  VLOG(1) << "RunStep [" << owner_->shard_id() << "] " << completed_t;

  if (completed_t) {
    awakened_transactions_.erase(completed_t);

    auto dbit = watched_dbs_.find(completed_t->db_index());
    if (dbit != watched_dbs_.end()) {
      DbWatchTable& wt = *dbit->second;

      ShardId sid = owner_->shard_id();
      KeyLockArgs lock_args = completed_t->GetLockArgs(sid);

      for (size_t i = 0; i < lock_args.args.size(); i += lock_args.key_step) {
        string_view key = lock_args.args[i];
        if (wt.AddAwakeEvent(WatchQueue::ACTIVE, key)) {
          awakened_indices_.emplace(completed_t->db_index());
        }
      }
    }
  }

  for (DbIndex index : awakened_indices_) {
    auto dbit = watched_dbs_.find(index);
    if (dbit == watched_dbs_.end())
      continue;

    DbWatchTable& wt = *dbit->second;
    for (auto key : wt.awakened_keys) {
      string_view sv_key = static_cast<string_view>(key);

      // Double verify we still got the item.
      auto [it, exp_it] = owner_->db_slice().FindExt(index, sv_key);
      if (!IsValid(it) || it->second.ObjType() != OBJ_LIST)  // Only LIST is allowed to block.
        continue;

      auto w_it = wt.queue_map.find(sv_key);
      CHECK(w_it != wt.queue_map.end());
      DVLOG(1) << "NotifyWatchQueue " << key;
      WatchQueue* wq = w_it->second.get();
      NotifyWatchQueue(wq);
      if (wq->items.empty()) {
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

void BlockingController::AddWatched(Transaction* trans) {
  VLOG(1) << "AddWatched [" << owner_->shard_id() << "] " << trans->DebugId();

  auto [dbit, added] = watched_dbs_.emplace(trans->db_index(), nullptr);
  if (added) {
    dbit->second.reset(new DbWatchTable);
  }

  DbWatchTable& wt = *dbit->second;

  auto args = trans->ShardArgsInShard(owner_->shard_id());
  for (auto key : args) {
    auto [res, inserted] = wt.queue_map.emplace(key, nullptr);
    if (inserted) {
      res->second.reset(new WatchQueue);
    }

    res->second->items.emplace_back(trans);
  }
}

// Runs in O(N) complexity.
void BlockingController::RemoveWatched(Transaction* trans) {
  VLOG(1) << "RemoveWatched [" << owner_->shard_id() << "] " << trans->DebugId();

  auto dbit = watched_dbs_.find(trans->db_index());
  CHECK(dbit != watched_dbs_.end());

  DbWatchTable& wt = *dbit->second;
  auto args = trans->ShardArgsInShard(owner_->shard_id());
  for (auto key : args) {
    auto watch_it = wt.queue_map.find(key);
    CHECK(watch_it != wt.queue_map.end());

    WatchQueue& wq = *watch_it->second;
    bool erased = false;
    for (auto items_it = wq.items.begin(); items_it != wq.items.end(); ++items_it) {
      if (items_it->trans == trans) {
        wq.items.erase(items_it);
        erased = true;
        break;
      }
    }
    CHECK(erased);

    if (wq.items.empty()) {
      wt.RemoveEntry(watch_it);
    }
  }

  if (wt.queue_map.empty()) {
    watched_dbs_.erase(dbit);
  }

  awakened_transactions_.erase(trans);
}

// Called from commands like lpush.
void BlockingController::AwakeWatched(DbIndex db_index, string_view db_key) {
  auto it = watched_dbs_.find(db_index);
  if (it == watched_dbs_.end())
    return;

  VLOG(1) << "AwakeWatched: db(" << db_index << ") " << db_key;

  DbWatchTable& wt = *it->second;
  DCHECK(!wt.queue_map.empty());

  if (wt.AddAwakeEvent(WatchQueue::SUSPENDED, db_key)) {
    awakened_indices_.insert(db_index);
  }
}

// Internal function called from ProcessAwakened().
// Marks the queue as active and notifies the first transaction in the queue.
void BlockingController::NotifyWatchQueue(WatchQueue* wq) {
  VLOG(1) << "Notify WQ: [" << owner_->shard_id() << "]";

  wq->state = WatchQueue::ACTIVE;

  auto& queue = wq->items;
  ShardId sid = owner_->shard_id();

  do {
    WatchItem& wi = queue.front();
    Transaction* head = wi.get();

    queue.pop_front();

    if (head->NotifySuspended(owner_->committed_txid(), sid)) {
      wq->notify_txid = owner_->committed_txid();
      awakened_transactions_.insert(head);
      break;
    }
  } while (!queue.empty());
}

#if 0

void BlockingController::OnTxFinish() {
  VLOG(1) << "OnTxFinish [" << owner_->shard_id() << "]";

  if (waiting_convergence_.empty())
    return;

  TxQueue* txq = owner_->txq();
  if (txq->Empty()) {
    for (const auto& k_v : waiting_convergence_) {
      NotifyConvergence(k_v.second);
    }
    waiting_convergence_.clear();
    return;
  }

  TxId txq_score = txq->HeadScore();
  do {
    auto tx_waiting = waiting_convergence_.begin();
    Transaction* trans = tx_waiting->second;

    // Instead of taking the map key, we use upto date notify_txid
    // which could meanwhile improve (decrease). Not important though.
    TxId notifyid = trans->notify_txid();
    if (owner_->committed_txid() < notifyid && txq_score <= notifyid)
      break;  // we can not converge for notifyid so we can not converge for larger ts as well.

    waiting_convergence_.erase(tx_waiting);
    NotifyConvergence(trans);
  } while (!waiting_convergence_.empty());
}


void BlockingController::RegisterAwaitForConverge(Transaction* t) {
  TxId notify_id = t->notify_txid();

  DVLOG(1) << "RegisterForConverge " << t->DebugId() << " at notify " << notify_id;

  // t->notify_txid might improve in parallel. it does not matter since convergence
  // will happen even with stale notify_id.
  waiting_convergence_.emplace(notify_id, t);
}
#endif

size_t BlockingController::NumWatched(DbIndex db_indx) const {
  auto it = watched_dbs_.find(db_indx);
  if (it == watched_dbs_.end())
    return 0;

  return it->second->queue_map.size();
}

}  // namespace dfly
