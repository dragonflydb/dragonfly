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
#include "core/mi_memory_resource.h"
#include "core/task_queue.h"
#include "core/tx_queue.h"
#include "server/db_slice.h"

namespace dfly {

namespace journal {
class Journal;
}  // namespace journal

class TieredStorage;
class TieredStorageV2;
class ShardDocIndices;
class BlockingController;

class EngineShard {
 public:
  struct Stats {
    uint64_t defrag_attempt_total = 0;
    uint64_t defrag_realloc_total = 0;
    uint64_t defrag_task_invocation_total = 0;
    uint64_t poll_execution_total = 0;

    uint64_t tx_immediate_total = 0;
    uint64_t tx_ooo_total = 0;

    Stats& operator+=(const Stats&);
  };

  // EngineShard() is private down below.
  ~EngineShard();

  // Sets up a new EngineShard in the thread.
  // If update_db_time is true, initializes periodic time update for its db_slice.
  static void InitThreadLocal(util::ProactorBase* pb, bool update_db_time, size_t max_file_size);

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

  PMR_NS::memory_resource* memory_resource() {
    return &mi_resource_;
  }

  TaskQueue* GetFiberQueue() {
    return &queue_;
  }

  // Processes TxQueue, blocked transactions or any other execution state related to that
  // shard. Tries executing the passed transaction if possible (does not guarantee though).
  void PollExecution(const char* context, Transaction* trans);

  // Returns transaction queue.
  TxQueue* txq() {
    return &txq_;
  }

  const TxQueue* txq() const {
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

  // Remove current continuation trans if its equal to tx.
  void RemoveContTx(Transaction* tx);

  const Stats& stats() const {
    return stats_;
  }

  Stats& stats() {
    return stats_;
  }

  // Returns used memory for this shard.
  size_t UsedMemory() const;

  TieredStorage* tiered_storage() {
    return tiered_storage_.get();
  }

  TieredStorageV2* tiered_storage_v2() {
    return tiered_storage_v2_.get();
  }

  ShardDocIndices* search_indices() const {
    return shard_search_indices_.get();
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

  const Transaction* GetContTx() const {
    return continuation_trans_;
  }

  void TEST_EnableHeartbeat();

  struct TxQueueInfo {
    // Armed - those that the coordinator has armed with callbacks and wants them to run.
    // Runnable - those that could run (they own the locks) but probably can not run due
    // to head of line blocking in the transaction queue i.e. there is a transaction that
    // either is not armed or not runnable that is blocking the runnable transactions.
    // tx_total is the size of the transaction queue.
    unsigned tx_armed = 0, tx_total = 0, tx_runnable = 0, tx_global = 0;

    // total_locks - total number of the transaction locks in the shard.
    unsigned total_locks = 0;

    // contended_locks - number of locks that are contended by more than one transaction.
    unsigned contended_locks = 0;

    // The score of the lock with maximum contention (see IntentLock::ContetionScore for details).
    unsigned max_contention_score = 0;

    // the lock fingerprint with maximum contention score.
    uint64_t max_contention_lock;
  };

  TxQueueInfo AnalyzeTxQueue() const;

 private:
  struct DefragTaskState {
    size_t dbid = 0u;
    uint64_t cursor = 0u;
    bool underutilized_found = false;

    // check the current threshold and return true if
    // we need to do the defragmentation
    bool CheckRequired();

    void UpdateScanState(uint64_t cursor_val);

    void ResetScanState();
  };

  EngineShard(util::ProactorBase* pb, mi_heap_t* heap);

  // blocks the calling fiber.
  void Shutdown();  // called before destructing EngineShard.

  void StartPeriodicFiber(util::ProactorBase* pb);

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

  TaskQueue queue_;

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
  util::fb2::Fiber fiber_periodic_;
  util::fb2::Done fiber_periodic_done_;

  DefragTaskState defrag_state_;
  std::unique_ptr<TieredStorage> tiered_storage_;
  std::unique_ptr<TieredStorageV2> tiered_storage_v2_;
  std::unique_ptr<ShardDocIndices> shard_search_indices_;
  std::unique_ptr<BlockingController> blocking_controller_;

  using Counter = util::SlidingCounter<7>;

  Counter counter_[COUNTER_TOTAL];

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

  bool IsTieringEnabled() {
    return is_tiering_enabled_;
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
  // `func` must not preempt.
  template <typename U> void RunBriefInParallel(U&& func) const {
    RunBriefInParallel(std::forward<U>(func), [](auto i) { return true; });
  }

  // Runs a brief function on selected shards. Waits for it to complete.
  // `func` must not preempt.
  template <typename U, typename P> void RunBriefInParallel(U&& func, P&& pred) const;

  // Runs a possibly blocking function on all shards. Waits for it to complete.
  template <typename U> void RunBlockingInParallel(U&& func) {
    RunBlockingInParallel(std::forward<U>(func), [](auto i) { return true; });
  }

  // Runs a possibly blocking function on selected shards. Waits for it to complete.
  template <typename U, typename P> void RunBlockingInParallel(U&& func, P&& pred);

  // Runs func on all shards via the same shard queue that's been used by transactions framework.
  // The functions running inside the shard queue run atomically (sequentially)
  // with respect each other on the same shard.
  template <typename U> void AwaitRunningOnShardQueue(U&& func) {
    util::fb2::BlockingCounter bc(shard_queue_.size());
    for (size_t i = 0; i < shard_queue_.size(); ++i) {
      Add(i, [&func, bc]() mutable {
        func(EngineShard::tlocal());
        bc->Dec();
      });
    }

    bc->Wait();
  }

  // Used in tests
  void TEST_EnableHeartBeat();
  void TEST_EnableCacheMode();

 private:
  void InitThreadLocal(util::ProactorBase* pb, bool update_db_time, size_t max_file_size);

  util::ProactorPool* pp_;
  std::vector<TaskQueue*> shard_queue_;
  bool is_tiering_enabled_ = false;
};

template <typename U, typename P>
void EngineShardSet::RunBriefInParallel(U&& func, P&& pred) const {
  util::fb2::BlockingCounter bc{0};

  for (uint32_t i = 0; i < size(); ++i) {
    if (!pred(i))
      continue;

    bc->Add(1);
    util::ProactorBase* dest = pp_->at(i);
    dest->DispatchBrief([&func, bc]() mutable {
      func(EngineShard::tlocal());
      bc->Dec();
    });
  }
  bc->Wait();
}

template <typename U, typename P> void EngineShardSet::RunBlockingInParallel(U&& func, P&& pred) {
  util::fb2::BlockingCounter bc{0};
  static_assert(std::is_invocable_v<U, EngineShard*>,
                "Argument must be invocable EngineShard* as argument.");
  static_assert(std::is_void_v<std::invoke_result_t<U, EngineShard*>>,
                "Callable must not have a return value!");

  for (uint32_t i = 0; i < size(); ++i) {
    if (!pred(i))
      continue;

    bc->Add(1);
    util::ProactorBase* dest = pp_->at(i);

    // the "Dispatch" call spawns a fiber underneath.
    dest->Dispatch([&func, bc]() mutable {
      func(EngineShard::tlocal());
      bc->Dec();
    });
  }
  bc->Wait();
}

ShardId Shard(std::string_view v, ShardId shard_num);

// absl::GetCurrentTimeNanos is twice faster than clock_gettime(CLOCK_REALTIME) on my laptop
// and 4 times faster than on a VM. it takes 5-10ns to do a call.

extern uint64_t TEST_current_time_ms;

inline uint64_t GetCurrentTimeMs() {
  return TEST_current_time_ms ? TEST_current_time_ms : absl::GetCurrentTimeNanos() / 1000000;
}

extern EngineShardSet* shard_set;

}  // namespace dfly
