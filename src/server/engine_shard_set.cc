// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/engine_shard_set.h"

extern "C" {
#include "redis/zmalloc.h"
}

#include "base/logging.h"
#include "server/transaction.h"
#include "util/fiber_sched_algo.h"
#include "util/varz.h"

namespace dfly {

using namespace std;
using namespace util;
namespace this_fiber = ::boost::this_fiber;
namespace fibers = ::boost::fibers;

namespace {

vector<EngineShardSet::CachedStats> cached_stats;  // initialized in EngineShardSet::Init

}  // namespace

thread_local EngineShard* EngineShard::shard_ = nullptr;
constexpr size_t kQueueLen = 64;

struct WatchItem {
  ::boost::intrusive_ptr<Transaction> trans;

  WatchItem(Transaction* t) : trans(t) {
  }
};

struct EngineShard::WatchQueue {
  deque<WatchItem> items;
  TxId notify_txid = UINT64_MAX;

  // Updated  by both coordinator and shard threads but at different times.
  enum State { SUSPENDED, ACTIVE } state = SUSPENDED;

  void Suspend() {
    state = SUSPENDED;
    notify_txid = UINT64_MAX;
  }
};

bool EngineShard::DbWatchTable::RemoveEntry(WatchQueueMap::iterator it) {
  DVLOG(1) << "Erasing watchqueue key " << it->first;

  awakened_keys.erase(it->first);
  queue_map.erase(it);

  return queue_map.empty();
}

EngineShard::EngineShard(util::ProactorBase* pb, bool update_db_time, mi_heap_t* heap)
    : queue_(kQueueLen), txq_([](const Transaction* t) { return t->txid(); }), mi_resource_(heap),
      db_slice_(pb->GetIndex(), this) {
  fiber_q_ = fibers::fiber([this, index = pb->GetIndex()] {
    this_fiber::properties<FiberProps>().set_name(absl::StrCat("shard_queue", index));
    queue_.Run();
  });

  if (update_db_time) {
    constexpr uint32_t kClockCycleMs = 1;

    periodic_task_ = pb->AddPeriodic(kClockCycleMs, [this] {
      // absl::GetCurrentTimeNanos() returns current time since the Unix Epoch.
      db_slice().UpdateExpireClock(absl::GetCurrentTimeNanos() / 1000000);

      if (task_iters_++ % 8 == 0) {
        CacheStats();
      }
    });
  }

  tmp_str1 = sdsempty();
  tmp_str2 = sdsempty();
  db_slice_.UpdateExpireBase(absl::GetCurrentTimeNanos() / 1000000, 0);
}

EngineShard::~EngineShard() {
  queue_.Shutdown();
  fiber_q_.join();
  sdsfree(tmp_str1);
  sdsfree(tmp_str2);

  if (periodic_task_) {
    ProactorBase::me()->CancelPeriodic(periodic_task_);
  }
}

void EngineShard::InitThreadLocal(ProactorBase* pb, bool update_db_time) {
  CHECK(shard_ == nullptr) << pb->GetIndex();
  CHECK(mi_heap_get_backing() == mi_heap_get_default());

  mi_heap_t* tlh = mi_heap_new();
  init_zmalloc_threadlocal(tlh);

  void* ptr = mi_heap_malloc_aligned(tlh, sizeof(EngineShard), alignof(EngineShard));
  shard_ = new (ptr) EngineShard(pb, update_db_time, tlh);

  CompactObj::InitThreadLocal(shard_->memory_resource());
  SmallString::InitThreadLocal(tlh);
}

void EngineShard::DestroyThreadLocal() {
  if (!shard_)
    return;

  uint32_t index = shard_->db_slice_.shard_id();
  mi_heap_t* tlh = shard_->mi_resource_.heap();
  shard_->~EngineShard();
  mi_free(shard_);
  shard_ = nullptr;
  CompactObj::InitThreadLocal(nullptr);
  mi_heap_delete(tlh);
  VLOG(1) << "Shard reset " << index;
}

// Is called by Transaction::ExecuteAsync in order to run transaction tasks.
// Only runs in its own thread.
void EngineShard::PollExecution(const char* context, Transaction* trans) {
  DVLOG(1) << "PollExecution " << context << " " << (trans ? trans->DebugId() : "");
  ShardId sid = shard_id();

  uint16_t trans_mask = trans ? trans->GetLocalMask(sid) : 0;
  if (trans_mask & Transaction::AWAKED_Q) {
    DCHECK(continuation_trans_ == nullptr);

    CHECK_EQ(committed_txid_, trans->notify_txid()) << "TBD";
    bool keep = trans->RunInShard(this);
    if (keep)
      return;
  }

  if (continuation_trans_) {
    if (trans == continuation_trans_)
      trans = nullptr;

    if (continuation_trans_->IsArmedInShard(sid)) {
      bool to_keep = continuation_trans_->RunInShard(this);
      DVLOG(1) << "RunContTrans: " << continuation_trans_->DebugId() << " keep: " << to_keep;
      if (!to_keep) {
        continuation_trans_ = nullptr;
        OnTxFinish();
      }
    }
  }

  bool has_awaked_trans = HasAwakedTransaction();
  Transaction* head = nullptr;
  string dbg_id;

  if (continuation_trans_ == nullptr && !has_awaked_trans) {
    while (!txq_.Empty()) {
      auto val = txq_.Front();
      head = absl::get<Transaction*>(val);

      // The fact that Tx is in the queue, already means that coordinator fiber will not progress,
      // hence here it's enough to test for run_count and check local_mask.
      bool is_armed = head->IsArmedInShard(sid);
      if (!is_armed)
        break;

      // It could be that head is processed and unblocks multi-hop transaction .
      // The transaction will schedule again and will arm another callback.
      // Then we will reach invalid state by running trans after this loop,
      // which is not what we want.
      // This function should not process 2 different callbacks for the same transaction.
      // Hence we make sure to reset trans if it has been processed via tx-queue.
      if (head == trans)
        trans = nullptr;
      TxId txid = head->txid();

      // committed_txid_ is strictly increasing when processed via TxQueue.
      DCHECK_LT(committed_txid_, txid);

      // We update committed_txid_ before calling RunInShard() to avoid cases where
      // a transaction stalls the execution with IO while another fiber queries this shard for
      // committed_txid_ (for example during the scheduling).
      committed_txid_ = txid;
      if (VLOG_IS_ON(2)) {
        dbg_id = head->DebugId();
      }

      bool keep = head->RunInShard(this);
      // We should not access head from this point since RunInShard callback decrements refcount.
      DLOG_IF(INFO, !dbg_id.empty()) << "RunHead " << dbg_id << ", keep " << keep;

      if (keep) {
        continuation_trans_ = head;
        break;
      }

      OnTxFinish();
    }       // while(!txq_.Empty())
  } else {  // if (continuation_trans_ == nullptr && !has_awaked_trans)
    DVLOG(1) << "Skipped TxQueue " << continuation_trans_ << " " << has_awaked_trans;
  }

  // For SUSPENDED_Q - if transaction has not been notified, it will still be
  // in the watch queue. We need to unlock an Execute by running a noop.
  if (trans_mask & Transaction::SUSPENDED_Q) {
    TxId notify_txid = trans->notify_txid();
    DCHECK(HasResultConverged(notify_txid));
    trans->RunNoop(this);
    return;
  }

  // If trans is out of order, i.e. locks keys that previous transactions have not locked.
  // It may be that there are other transactions that touch those keys but they necessary ordered
  // after trans in the queue, hence it's safe to run trans out of order.
  if (trans && trans_mask & Transaction::OUT_OF_ORDER) {
    DCHECK(trans != head);
    DCHECK(!trans->IsMulti());  // multi, global transactions can not be OOO.
    DCHECK(trans_mask & Transaction::ARMED);

    dbg_id.clear();

    if (VLOG_IS_ON(1)) {
      dbg_id = trans->DebugId();
    }
    ++stats_.ooo_runs;

    bool keep = trans->RunInShard(this);
    DLOG_IF(INFO, !dbg_id.empty()) << "Eager run " << sid << ", " << dbg_id << ", keep " << keep;

    // Should be enforced via Schedule(). TODO: to remove the check once the code is mature.
    CHECK(!keep) << "multi-hop transactions can not be OOO.";
  }
}

// Internal function called from ProcessAwakened().
// Marks the queue as active and notifies the first transaction in the queue.
Transaction* EngineShard::NotifyWatchQueue(WatchQueue* wq) {
  wq->state = WatchQueue::ACTIVE;

  auto& q = wq->items;
  ShardId sid = shard_id();

  do {
    const WatchItem& wi = q.front();
    Transaction* head = wi.trans.get();

    if (head->NotifySuspended(committed_txid_, sid)) {
      wq->notify_txid = committed_txid_;
      return head;
    }

    q.pop_front();
  } while (!q.empty());

  return nullptr;
}

// Processes potentially awakened keys and verifies that these are indeed
// awakened to eliminate false positives.
// In addition, optionally removes completed_t from the watch queues.
void EngineShard::ProcessAwakened(Transaction* completed_t) {
  for (DbIndex index : awakened_indices_) {
    DbWatchTable& wt = watched_dbs_[index];

    for (auto key : wt.awakened_keys) {
      string_view sv_key = static_cast<string_view>(key);
      auto [it, exp_it] = db_slice_.FindExt(index, sv_key);  // Double verify we still got the item.
      if (!IsValid(it))
        continue;

      auto w_it = wt.queue_map.find(sv_key);
      CHECK(w_it != wt.queue_map.end());
      DVLOG(1) << "NotifyWatchQueue " << key;
      Transaction* t2 = NotifyWatchQueue(w_it->second.get());
      if (t2) {
        awakened_transactions_.insert(t2);
      }
    }
    wt.awakened_keys.clear();
  }
  awakened_indices_.clear();

  if (!completed_t)
    return;

  auto dbit = watched_dbs_.find(completed_t->db_index());
  if (dbit == watched_dbs_.end())
    return;

  DbWatchTable& wt = dbit->second;
  KeyLockArgs lock_args = completed_t->GetLockArgs(shard_id());

  for (size_t i = 0; i < lock_args.args.size(); i += lock_args.key_step) {
    string_view key = lock_args.args[i];
    auto w_it = wt.queue_map.find(key);

    if (w_it == wt.queue_map.end() || w_it->second->state != WatchQueue::ACTIVE)
      continue;

    WatchQueue& wq = *w_it->second;

    DCHECK_LE(wq.notify_txid, committed_txid_);

    auto& queue = wq.items;
    DCHECK(!queue.empty());  // since it's active

    if (queue.front().trans == completed_t) {
      queue.pop_front();

      while (!queue.empty()) {
        const WatchItem& bi = queue.front();
        Transaction* head = bi.trans.get();

        if (head->NotifySuspended(wq.notify_txid, shard_id()))
          break;
        queue.pop_front();
      }

      if (queue.empty()) {
        wt.RemoveEntry(w_it);
      }
    }
  }

  if (wt.queue_map.empty()) {
    watched_dbs_.erase(dbit);
  }
  awakened_transactions_.erase(completed_t);
}

void EngineShard::AddWatched(string_view key, Transaction* me) {
  DbWatchTable& wt = watched_dbs_[me->db_index()];
  auto [res, inserted] = wt.queue_map.emplace(key, nullptr);
  if (inserted) {
    res->second.reset(new WatchQueue);
  }

  res->second->items.emplace_back(me);
}

// Runs in O(N) complexity.
bool EngineShard::RemovedWatched(string_view key, Transaction* me) {
  auto dbit = watched_dbs_.find(me->db_index());
  CHECK(dbit != watched_dbs_.end());

  DbWatchTable& wt = dbit->second;
  auto watch_it = wt.queue_map.find(key);
  CHECK(watch_it != wt.queue_map.end());

  WatchQueue& wq = *watch_it->second;
  for (auto j = wq.items.begin(); j != wq.items.end(); ++j) {
    if (j->trans == me) {
      wq.items.erase(j);
      if (wq.items.empty()) {
        if (wt.RemoveEntry(watch_it)) {
          watched_dbs_.erase(dbit);
        }
      }
      return true;
    }
  }

  LOG(FATAL) << "should not happen";

  return false;
}

void EngineShard::GCWatched(const KeyLockArgs& largs) {
  auto dbit = watched_dbs_.find(largs.db_index);
  CHECK(dbit != watched_dbs_.end());

  DbWatchTable& wt = dbit->second;

  for (size_t i = 0; i < largs.args.size(); i += largs.key_step) {
    string_view key = largs.args[i];
    auto watch_it = wt.queue_map.find(key);
    CHECK(watch_it != wt.queue_map.end());

    WatchQueue& wq = *watch_it->second;
    DCHECK(!wq.items.empty());
    do {
      auto local_mask = wq.items.front().trans->GetLocalMask(shard_id());
      if ((local_mask & Transaction::EXPIRED_Q) == 0) {
        break;
      }
      wq.items.pop_front();
    } while (!wq.items.empty());

    if (wq.items.empty()) {
      if (wt.RemoveEntry(watch_it)) {
        watched_dbs_.erase(dbit);
        return;
      }
    }
  }
}

// Called from commands like lpush.
void EngineShard::AwakeWatched(DbIndex db_index, string_view db_key) {
  auto it = watched_dbs_.find(db_index);
  if (it == watched_dbs_.end())
    return;

  DbWatchTable& wt = it->second;
  DCHECK(!wt.queue_map.empty());

  auto wit = wt.queue_map.find(db_key);

  if (wit == wt.queue_map.end())
    return;  /// Similarly, nobody watches this key.

  string_view key = wit->first;

  // Already awakened this key.
  if (wt.awakened_keys.find(key) != wt.awakened_keys.end())
    return;

  wt.awakened_keys.insert(wit->first);
  awakened_indices_.insert(db_index);
}

void EngineShard::ShutdownMulti(Transaction* multi) {
  if (continuation_trans_ == multi) {
    continuation_trans_ = nullptr;
  }
  OnTxFinish();
}

void EngineShard::WaitForConvergence(TxId notifyid, Transaction* t) {
  DVLOG(1) << "ConvergeNotification " << t->DebugId() << " at notify " << notifyid;
  waiting_convergence_.emplace(notifyid, t);
}

void EngineShard::OnTxFinish() {
  DCHECK(continuation_trans_ == nullptr);  // By definition of OnTxFinish.

  if (waiting_convergence_.empty())
    return;

  if (txq_.Empty()) {
    for (const auto& k_v : waiting_convergence_) {
      NotifyConvergence(k_v.second);
    }
    waiting_convergence_.clear();
    return;
  }

  TxId txq_score = txq_.HeadScore();
  do {
    auto tx_waiting = waiting_convergence_.begin();

    // Instead of taking the map key, we use upto date notify_txid
    // That could meanwhile improve. Not important though.
    TxId notifyid = tx_waiting->second->notify_txid();
    if (notifyid > committed_txid_ && txq_score <= tx_waiting->first)
      break;
    auto nh = waiting_convergence_.extract(tx_waiting);
    NotifyConvergence(nh.mapped());
  } while (!waiting_convergence_.empty());
}

void EngineShard::NotifyConvergence(Transaction* tx) {
  LOG(FATAL) << "TBD";
}

// There are several cases that contain proof of convergence for this shard:
// 1. txq_ empty - it means that anything that is goonna be scheduled will already be scheduled
//    with txid > notifyid.
// 2. committed_txid_ > notifyid - similarly, this shard can not affect the result with timestamp
//    notifyid.
// 3. committed_txid_ == notifyid, then if a transaction in progress (continuation_trans_ != NULL)
//    the this transaction can still affect the result, hence we require continuation_trans_ is null
//    which will point to converged result @notifyid.
// 4. Finally with committed_txid_ < notifyid and continuation_trans_ == nullptr,
//    we can check if the next in line (HeadScore) is after notifyid in that case we can also
//    conclude regarding the result convergence for this shard.
bool EngineShard::HasResultConverged(TxId notifyid) const {
  return txq_.Empty() || committed_txid_ > notifyid ||
         (continuation_trans_ == nullptr &&
          (committed_txid_ == notifyid || txq_.HeadScore() > notifyid));
}

void EngineShard::CacheStats() {
#if 0
  mi_heap_t* tlh = mi_resource_.heap();
  struct Sum {
    size_t used = 0;
    size_t comitted = 0;
  } sum;

  auto visit_cb = [](const mi_heap_t* heap, const mi_heap_area_t* area, void* block,
                     size_t block_size, void* arg) -> bool {
    DCHECK(!block);
    Sum* sum = (Sum*)arg;

    // mimalloc mistakenly exports used in blocks instead of bytes.
    sum->used += block_size * area->used;
    sum->comitted += area->committed;

    DVLOG(1) << "block_size " << block_size << "/" << area->block_size << ", reserved "
             << area->reserved << " comitted " << area->committed << " used: " << area->used;
    return true;  // continue iteration
  };
#endif
  // mi_heap_visit_blocks(tlh, false /* visit all blocks*/, visit_cb, &sum);
  mi_stats_merge();
  // stats_.heap_used_bytes = sum.used;
  stats_.heap_used_bytes =
      mi_resource_.used() + zmalloc_used_memory_tl + SmallString::UsedThreadLocal();
  cached_stats[db_slice_.shard_id()].used_memory.store(stats_.heap_used_bytes,
                                                       memory_order_relaxed);
  // stats_.heap_comitted_bytes = sum.comitted;
}

void EngineShardSet::Init(uint32_t sz) {
  CHECK_EQ(0u, size());
  cached_stats.resize(sz);
  shard_queue_.resize(sz);
}

void EngineShardSet::InitThreadLocal(ProactorBase* pb, bool update_db_time) {
  EngineShard::InitThreadLocal(pb, update_db_time);
  EngineShard* es = EngineShard::tlocal();
  shard_queue_[es->shard_id()] = es->GetFiberQueue();
}

const vector<EngineShardSet::CachedStats>& EngineShardSet::GetCachedStats() {
  return cached_stats;
}

}  // namespace dfly
