// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

extern "C" {
  #include "redis/sds.h"
}

#include <xxhash.h>

#include "core/tx_queue.h"
#include "server/db_slice.h"
#include "util/fibers/fiberqueue_threadpool.h"
#include "util/fibers/fibers_ext.h"
#include "util/proactor_pool.h"

namespace dfly {

class EngineShard {
 public:
  // EngineShard() is private down below.
  ~EngineShard();

  static void InitThreadLocal(util::ProactorBase* pb);
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

  ::util::fibers_ext::FiberQueue* GetQueue() {
    return &queue_;
  }

  // Executes a transaction. This transaction is pending in the queue.
  void Execute(Transaction* trans);

  // Returns transaction queue.
  TxQueue* txq() {
    return &txq_;
  }

  TxId committed_txid() const {
    return committed_txid_;
  }

  TxQueue::Iterator InsertTxQ(Transaction* trans) {
    return txq_.Insert(trans);
  }

  sds tmp_str;

 private:
  EngineShard(util::ProactorBase* pb);

  void RunContinuationTransaction();

  ::util::fibers_ext::FiberQueue queue_;
  ::boost::fibers::fiber fiber_q_;

  TxQueue txq_;

  // Logical ts used to order distributed transactions.
  TxId committed_txid_ = 0;
  Transaction* continuation_trans_ = nullptr;

  DbSlice db_slice_;
  uint32_t periodic_task_ = 0;

  static thread_local EngineShard* shard_;
};

class EngineShardSet {
 public:
  explicit EngineShardSet(util::ProactorPool* pp) : pp_(pp) {
  }

  uint32_t size() const {
    return uint32_t(shard_queue_.size());
  }

  util::ProactorPool* pool() {
    return pp_;
  }

  void Init(uint32_t size);
  void InitThreadLocal(util::ProactorBase* pb);

  template <typename F> auto Await(ShardId sid, F&& f) {
    return shard_queue_[sid]->Await(std::forward<F>(f));
  }

  template <typename F> auto Add(ShardId sid, F&& f) {
    assert(sid < shard_queue_.size());
    return shard_queue_[sid]->Add(std::forward<F>(f));
  }

  // Runs a brief function on all shards.
  template <typename U> void RunBriefInParallel(U&& func) {
    RunBriefInParallel(std::forward<U>(func), [](auto i) { return true; });
  }

  template <typename U, typename P> void RunBriefInParallel(U&& func, P&& pred);

  template <typename U> void RunBlockingInParallel(U&& func);

 private:
  util::ProactorPool* pp_;
  std::vector<util::fibers_ext::FiberQueue*> shard_queue_;
};

template <typename U, typename P> void EngineShardSet::RunBriefInParallel(U&& func, P&& pred) {
  util::fibers_ext::BlockingCounter bc{0};

  for (uint32_t i = 0; i < size(); ++i) {
    if (!pred(i))
      continue;

    bc.Add(1);
    util::ProactorBase* dest = pp_->at(i);
    dest->AsyncBrief([f = std::forward<U>(func), bc]() mutable {
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
    dest->AsyncFiber([f = std::forward<U>(func), bc]() mutable {
      f(EngineShard::tlocal());
      bc.Dec();
    });
  }
  bc.Wait();
}

template <typename View> inline ShardId Shard(const View& v, ShardId shard_num) {
  XXH64_hash_t hash = XXH64(v.data(), v.size(), 120577240643ULL);
  return hash % shard_num;
}

}  // namespace dfly
