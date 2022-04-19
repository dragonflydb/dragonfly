// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

extern "C" {
#include "redis/sds.h"
}

#include <absl/container/btree_map.h>
#include <absl/container/flat_hash_map.h>
#include <xxhash.h>

#include "base/string_view_sso.h"
#include "core/external_alloc.h"
#include "core/mi_memory_resource.h"
#include "core/tx_queue.h"
#include "server/channel_slice.h"
#include "server/db_slice.h"
#include "util/fibers/fiberqueue_threadpool.h"
#include "util/fibers/fibers_ext.h"
#include "util/proactor_pool.h"

namespace dfly {

class TieredStorage;

class EngineShard {
 public:
  struct Stats {
    uint64_t ooo_runs = 0;    // how many times transactions run as OOO.
    uint64_t quick_runs = 0;  //  how many times single shard "RunQuickie" transaction run.
  };

  // EngineShard() is private down below.
  ~EngineShard();

  // Sets up a new EngineShard in the thread.
  // If update_db_time is true, initializes periodic time update for its db_slice.
  static void InitThreadLocal(util::ProactorBase* pb, bool update_db_time);

  static void DestroyThreadLocal();

  static EngineShard* tlocal() {
    return shard_;
  }

  ShardId shard_id() const {
    return db_slice_.shard_id();
  }

  DbSlice& db_slice() {
    return db_slice_;
  }

  const DbSlice& db_slice() const {
    return db_slice_;
  }

  ChannelSlice& channel_slice() {
    return channel_slice_;
  }

  std::pmr::memory_resource* memory_resource() {
    return &mi_resource_;
  }

  ::util::fibers_ext::FiberQueue* GetFiberQueue() {
    return &queue_;
  }

  // Processes TxQueue, blocked transactions or any other execution state related to that
  // shard. Tries executing the passed transaction if possible (does not guarantee though).
  void PollExecution(const char* context, Transaction* trans);

  // Returns transaction queue.
  TxQueue* txq() {
    return &txq_;
  }

  TxId committed_txid() const {
    return committed_txid_;
  }

  // Signals whether shard-wide lock is active.
  // Transactions that conflict with shard locks must subscribe into pending queue.
  IntentLock* shard_lock() {
    return &shard_lock_;
  }

  // Iterates over awakened key candidates in each db and moves verified ones into
  // global verified_awakened_ array.
  // Returns true if there are active awakened keys, false otherwise.
  // It has 2 responsibilities.
  // 1: to go over potential wakened keys, verify them and activate watch queues.
  // 2: if t is awaked and finished running - to remove it from the head
  //    of the queue and notify the next one.
  //    If t is null then second part is omitted.
  void ProcessAwakened(Transaction* t);

  // Blocking API
  // TODO: consider moving all watched functions to
  // EngineShard with separate per db map.
  //! AddWatched adds a transaction to the blocking queue.
  void AddWatched(std::string_view key, Transaction* me);
  bool RemovedWatched(std::string_view key, Transaction* me);
  void GCWatched(const KeyLockArgs& lock_args);

  void AwakeWatched(DbIndex db_index, std::string_view db_key);

  bool HasAwakedTransaction() const {
    return !awakened_transactions_.empty();
  }

  // TODO: Awkward interface. I should solve it somehow.
  void ShutdownMulti(Transaction* multi);
  void WaitForConvergence(TxId notifyid, Transaction* t);
  bool HasResultConverged(TxId notifyid) const;

  void IncQuickRun() {
    stats_.quick_runs++;
  }

  const Stats& stats() const {
    return stats_;
  }

  // Returns used memory for this shard.
  size_t UsedMemory() const;

  TieredStorage* tiered_storage() { return tiered_storage_.get(); }

  // for everyone to use for string transformations during atomic cpu sequences.
  sds tmp_str1, tmp_str2;

 private:
  EngineShard(util::ProactorBase* pb, bool update_db_time, mi_heap_t* heap);

  // blocks the calling fiber.
  void Shutdown();  // called before destructing EngineShard.

  struct WatchQueue;

  void OnTxFinish();
  void NotifyConvergence(Transaction* tx);
  void CacheStats();

  /// Returns the notified transaction,
  /// or null if all transactions in the queue have expired..
  Transaction* NotifyWatchQueue(WatchQueue* wq);

  using WatchQueueMap = absl::flat_hash_map<std::string, std::unique_ptr<WatchQueue>>;

  // Watch state per db.
  struct DbWatchTable {
    WatchQueueMap queue_map;

    // awakened keys point to blocked keys that can potentially be unblocked.
    // they reference key objects in queue_map.
    absl::flat_hash_set<base::string_view_sso> awakened_keys;

    // Returns true if queue_map is empty and DbWatchTable can be removed as well.
    bool RemoveEntry(WatchQueueMap::iterator it);
  };

  absl::flat_hash_map<DbIndex, DbWatchTable> watched_dbs_;
  absl::flat_hash_set<DbIndex> awakened_indices_;
  absl::flat_hash_set<Transaction*> awakened_transactions_;

  absl::btree_multimap<TxId, Transaction*> waiting_convergence_;

  ::util::fibers_ext::FiberQueue queue_;
  ::boost::fibers::fiber fiber_q_;

  TxQueue txq_;
  MiMemoryResource mi_resource_;
  DbSlice db_slice_;
  ChannelSlice channel_slice_;

  Stats stats_;

  // Logical ts used to order distributed transactions.
  TxId committed_txid_ = 0;
  Transaction* continuation_trans_ = nullptr;
  IntentLock shard_lock_;

  uint32_t periodic_task_ = 0;
  uint64_t task_iters_ = 0;
  std::unique_ptr<TieredStorage> tiered_storage_;

  static thread_local EngineShard* shard_;
};

class EngineShardSet {
 public:
  struct CachedStats {
    std::atomic_uint64_t used_memory;

    CachedStats() : used_memory(0) {
    }

    CachedStats(const CachedStats& o) : used_memory(o.used_memory.load()) {
    }
  };

  explicit EngineShardSet(util::ProactorPool* pp) : pp_(pp) {
  }

  uint32_t size() const {
    return uint32_t(shard_queue_.size());
  }

  util::ProactorPool* pool() {
    return pp_;
  }

  void Init(uint32_t size);
  void InitThreadLocal(util::ProactorBase* pb, bool update_db_time);

  static const std::vector<CachedStats>& GetCachedStats();

  // Uses a shard queue to dispatch. Callback runs in a dedicated fiber.
  template <typename F> auto Await(ShardId sid, F&& f) {
    return shard_queue_[sid]->Await(std::forward<F>(f));
  }

  // Uses a shard queue to dispatch. Callback runs in a dedicated fiber.
  template <typename F> auto Add(ShardId sid, F&& f) {
    assert(sid < shard_queue_.size());
    return shard_queue_[sid]->Add(std::forward<F>(f));
  }

  // Runs a brief function on all shards. Waits for it to complete.
  template <typename U> void RunBriefInParallel(U&& func) const {
    RunBriefInParallel(std::forward<U>(func), [](auto i) { return true; });
  }

  // Runs a brief function on selected shards. Waits for it to complete.
  template <typename U, typename P> void RunBriefInParallel(U&& func, P&& pred) const;

  template <typename U> void RunBlockingInParallel(U&& func);

 private:
  util::ProactorPool* pp_;
  std::vector<util::fibers_ext::FiberQueue*> shard_queue_;
};

template <typename U, typename P>
void EngineShardSet::RunBriefInParallel(U&& func, P&& pred) const {
  util::fibers_ext::BlockingCounter bc{0};

  for (uint32_t i = 0; i < size(); ++i) {
    if (!pred(i))
      continue;

    bc.Add(1);
    util::ProactorBase* dest = pp_->at(i);
    dest->DispatchBrief([f = std::forward<U>(func), bc]() mutable {
      f(EngineShard::tlocal());
      bc.Dec();
    });
  }
  bc.Wait();
}

template <typename U> void EngineShardSet::RunBlockingInParallel(U&& func) {
  util::fibers_ext::BlockingCounter bc{size()};

  for (uint32_t i = 0; i < size(); ++i) {
    util::ProactorBase* dest = pp_->at(i);
    dest->Dispatch([func, bc]() mutable {
      func(EngineShard::tlocal());
      bc.Dec();
    });
  }
  bc.Wait();
}

inline ShardId Shard(std::string_view v, ShardId shard_num) {
  XXH64_hash_t hash = XXH64(v.data(), v.size(), 120577240643ULL);
  return hash % shard_num;
}

}  // namespace dfly
