// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "core/intent_lock.h"
#include "core/mi_memory_resource.h"
#include "core/task_queue.h"
#include "core/tx_queue.h"
#include "server/common.h"
#include "server/tx_base.h"
#include "util/sliding_counter.h"

typedef char* sds;

namespace dfly {

class EngineShardSet;
class TieredStorage;
class ShardDocIndices;

namespace journal {
class Journal;
}  // namespace journal

class EngineShard {
  friend class EngineShardSet;

 public:
  struct Stats {
    uint64_t defrag_attempt_total = 0;
    uint64_t defrag_realloc_total = 0;
    uint64_t defrag_task_invocation_total = 0;
    uint64_t poll_execution_total = 0;

    // number of optimistic executions - that were run as part of the scheduling.
    uint64_t tx_optimistic_total = 0;
    uint64_t tx_ooo_total = 0;

    // Number of ScheduleBatchInShard calls.
    uint64_t tx_batch_schedule_calls_total = 0;

    // Number of transactions scheduled via ScheduleBatchInShard.
    uint64_t tx_batch_scheduled_items_total = 0;

    Stats& operator+=(const Stats&);
  };

  // Sets up a new EngineShard in the thread.
  // If update_db_time is true, initializes periodic time update for its db_slice.
  static void InitThreadLocal(util::ProactorBase* pb);

  // Must be called after all InitThreadLocal() have finished
  void InitTieredStorage(util::ProactorBase* pb, size_t max_file_size);

  static void DestroyThreadLocal();

  static EngineShard* tlocal() {
    return shard_;
  }

  bool IsMyThread() const {
    return this == shard_;
  }

  ShardId shard_id() const {
    return shard_id_;
  }

  PMR_NS::memory_resource* memory_resource() {
    return &mi_resource_;
  }

  TaskQueue* GetFiberQueue() {
    return &queue_;
  }

  TaskQueue* GetSecondaryQueue() {
    return &queue2_;
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

  ShardDocIndices* search_indices() const {
    return shard_search_indices_.get();
  }

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

  void StopPeriodicFiber();

  struct TxQueueItem {
    std::string debug_id_info;
  };

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

    // We can use a vector to hold debug info for all items in the txqueue
    TxQueueItem head;
  };

  TxQueueInfo AnalyzeTxQueue() const;

  void ForceDefrag();

  // Returns true if revelant write operations should throttle to wait for tiering to catch up.
  // The estimate is based on memory usage crossing tiering redline and the write depth being at
  // least 50% of allowed max, providing at least some guarantee of progress.
  bool ShouldThrottleForTiering() const;

 private:
  struct DefragTaskState {
    size_t dbid = 0u;
    uint64_t cursor = 0u;
    time_t last_check_time = 0;
    bool is_force_defrag = false;

    // check the current threshold and return true if
    // we need to do the defragmentation
    bool CheckRequired();

    void UpdateScanState(uint64_t cursor_val);

    void ResetScanState();
  };

  EngineShard(util::ProactorBase* pb, mi_heap_t* heap);

  // blocks the calling fiber.
  void Shutdown();  // called before destructing EngineShard.

  void StartPeriodicHeartbeatFiber(util::ProactorBase* pb);
  void StartPeriodicShardHandlerFiber(util::ProactorBase* pb, std::function<void()> shard_handler);

  void Heartbeat();
  void RetireExpiredAndEvict();

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

  TaskQueue queue_, queue2_;

  TxQueue txq_;
  MiMemoryResource mi_resource_;
  ShardId shard_id_;

  Stats stats_;

  // Become passive if replica: don't automatially evict expired items.
  bool is_replica_ = false;

  size_t last_cached_used_memory_ = 0;
  uint64_t cache_stats_time_ = 0;  // monotonic, set by ProactorBase::GetMonotonicTimeNs.

  // Logical ts used to order distributed transactions.
  TxId committed_txid_ = 0;
  Transaction* continuation_trans_ = nullptr;
  journal::Journal* journal_ = nullptr;
  IntentLock shard_lock_;

  uint32_t defrag_task_ = 0;
  util::fb2::Fiber fiber_heartbeat_periodic_;
  util::fb2::Done fiber_heartbeat_periodic_done_;

  util::fb2::Fiber fiber_shard_handler_periodic_;
  util::fb2::Done fiber_shard_handler_periodic_done_;

  DefragTaskState defrag_state_;
  std::unique_ptr<TieredStorage> tiered_storage_;
  // TODO: Move indices to Namespace
  std::unique_ptr<ShardDocIndices> shard_search_indices_;

  using Counter = util::SlidingCounter<7>;

  Counter counter_[COUNTER_TOTAL];

  static __thread EngineShard* shard_;
};

}  // namespace dfly
