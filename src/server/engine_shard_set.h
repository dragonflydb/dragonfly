// Copyright 2022, DragonflyDB authors.  All rights reserved.
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
#include "util/proactor_pool.h"
#include "util/sliding_counter.h"

//
#include "core/external_alloc.h"
#include "core/fibers.h"
#include "core/mi_memory_resource.h"
#include "core/tx_queue.h"
#include "server/cluster/cluster_data.h"
#include "server/db_slice.h"

namespace dfly {

namespace journal {
class Journal;
}  // namespace journal

class TieredStorage;
class BlockingController;

class EngineShard {
 public:
  struct Stats {
    uint64_t ooo_runs = 0;    // how many times transactions run as OOO.
    uint64_t quick_runs = 0;  //  how many times single shard "RunQuickie" transaction run.
    uint64_t defrag_attempt_total = 0;
    uint64_t defrag_realloc_total = 0;
    uint64_t defrag_task_invocation_total = 0;

    Stats& operator+=(const Stats&);
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

  std::pmr::memory_resource* memory_resource() {
    return &mi_resource_;
  }

  FiberQueue* GetFiberQueue() {
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

  // TODO: Awkward interface. I should solve it somehow.
  void ShutdownMulti(Transaction* multi);

  void IncQuickRun() {
    stats_.quick_runs++;
  }

  const Stats& stats() const {
    return stats_;
  }

  // Returns used memory for this shard.
  size_t UsedMemory() const;

  TieredStorage* tiered_storage() {
    return tiered_storage_.get();
  }

  BlockingController* EnsureBlockingController();

  BlockingController* blocking_controller() {
    return blocking_controller_.get();
  }

  // for everyone to use for string transformations during atomic cpu sequences.
  sds tmp_str1;

  // Moving average counters.
  enum MovingCnt { TTL_TRAVERSE, TTL_DELETE, COUNTER_TOTAL };

  // Returns moving sum over the last 6 seconds.
  uint32_t GetMovingSum6(MovingCnt type) const {
    return counter_[unsigned(type)].SumTail();
  }

  journal::Journal* journal() {
    return journal_;
  }

  void set_journal(journal::Journal* j) {
    journal_ = j;
  }

  void SetReplica(bool replica) {
    is_replica_ = replica;
  }

  bool IsReplica() const {
    return is_replica_;
  }

  void TEST_EnableHeartbeat();

 private:
  struct DefragTaskState {
    // we will add more data members later
    uint64_t cursor = 0u;
    bool underutilized_found = false;

    // check the current threshold and return true if
    // we need to do the de-fermentation
    bool CheckRequired();

    void UpdateScanState(uint64_t cursor_val);
  };

  EngineShard(util::ProactorBase* pb, bool update_db_time, mi_heap_t* heap);

  // blocks the calling fiber.
  void Shutdown();  // called before destructing EngineShard.

  void Heartbeat();
  void RunPeriodic(std::chrono::milliseconds period_ms);

  void CacheStats();

  // We are running a task that checks whether we need to
  // do memory de-fragmentation here, this task only run
  // when there are available CPU time.
  // --------------------------------------------------------------------------
  // NOTE: This task is running with exclusive access to the shard.
  // i.e. - Since we are using shared noting access here, and all access
  // are done using fibers, This fiber is run only when no other fiber in the
  // context of the controlling thread will access this shard!
  // --------------------------------------------------------------------------
  uint32_t DefragTask();

  // scan the shard with the cursor and apply
  // de-fragmentation option for entries. This function will return the new cursor at the end of the
  // scan This function is called from context of StartDefragTask
  // return true if we did not complete the shard scan
  bool DoDefrag();

  FiberQueue queue_;
  Fiber fiber_q_;

  TxQueue txq_;
  MiMemoryResource mi_resource_;
  DbSlice db_slice_;

  Stats stats_;

  // Become passive if replica: don't automatially evict expired items.
  bool is_replica_ = false;

  // Logical ts used to order distributed transactions.
  TxId committed_txid_ = 0;
  Transaction* continuation_trans_ = nullptr;
  journal::Journal* journal_ = nullptr;
  IntentLock shard_lock_;

  uint32_t defrag_task_ = 0;
  Fiber fiber_periodic_;
  Done fiber_periodic_done_;

  DefragTaskState defrag_state_;
  std::unique_ptr<TieredStorage> tiered_storage_;
  std::unique_ptr<BlockingController> blocking_controller_;

  using Counter = util::SlidingCounter<7>;

  Counter counter_[COUNTER_TOTAL];
  std::vector<Counter> ttl_survivor_sum_;  // we need it per db.

  static __thread EngineShard* shard_;
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

  void Init(uint32_t size, bool update_db_time);
  void Shutdown();

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

  // Runs a brief function on selected shard thread. Waits for it to complete.
  template <typename U, typename P> void RunBriefInParallel(U&& func, P&& pred) const;

  template <typename U> void RunBlockingInParallel(U&& func);

  // Runs func on all shards via the same shard queue that's been used by transactions framework.
  // The functions running inside the shard queue run atomically (sequentially)
  // with respect each other on the same shard.
  template <typename U> void AwaitRunningOnShardQueue(U&& func) {
    BlockingCounter bc{unsigned(shard_queue_.size())};
    for (size_t i = 0; i < shard_queue_.size(); ++i) {
      Add(i, [&func, bc]() mutable {
        func(EngineShard::tlocal());
        bc.Dec();
      });
    }

    bc.Wait();
  }

  // Used in tests
  void TEST_EnableHeartBeat();
  void TEST_EnableCacheMode();

 private:
  void InitThreadLocal(util::ProactorBase* pb, bool update_db_time);

  util::ProactorPool* pp_;
  std::vector<FiberQueue*> shard_queue_;
};

template <typename U, typename P>
void EngineShardSet::RunBriefInParallel(U&& func, P&& pred) const {
  BlockingCounter bc{0};

  for (uint32_t i = 0; i < size(); ++i) {
    if (!pred(i))
      continue;

    bc.Add(1);
    util::ProactorBase* dest = pp_->at(i);
    dest->DispatchBrief([&func, bc]() mutable {
      func(EngineShard::tlocal());
      bc.Dec();
    });
  }
  bc.Wait();
}

template <typename U> void EngineShardSet::RunBlockingInParallel(U&& func) {
  BlockingCounter bc{size()};

  for (uint32_t i = 0; i < size(); ++i) {
    util::ProactorBase* dest = pp_->at(i);

    // the "Dispatch" call spawns a fiber underneath.
    dest->Dispatch([&func, bc]() mutable {
      func(EngineShard::tlocal());
      bc.Dec();
    });
  }
  bc.Wait();
}

inline ShardId Shard(std::string_view v, ShardId shard_num) {
  if (cluster_enabled) {
    v = KeyTag(v);
  }
  XXH64_hash_t hash = XXH64(v.data(), v.size(), 120577240643ULL);
  return hash % shard_num;
}

// absl::GetCurrentTimeNanos is twice faster than clock_gettime(CLOCK_REALTIME) on my laptop
// and 4 times faster than on a VM. it takes 5-10ns to do a call.

extern uint64_t TEST_current_time_ms;

inline uint64_t GetCurrentTimeMs() {
  return TEST_current_time_ms ? TEST_current_time_ms : absl::GetCurrentTimeNanos() / 1000000;
}

extern EngineShardSet* shard_set;

}  // namespace dfly
