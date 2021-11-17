// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include "server/db_slice.h"
#include "util/fibers/fibers_ext.h"
#include "util/fibers/fiberqueue_threadpool.h"
#include "util/proactor_pool.h"

namespace dfly {

using ShardId = uint16_t;

class EngineShard {
 public:
  DbSlice db_slice;

  //EngineShard() is private down below.
  ~EngineShard();

  static void InitThreadLocal(ShardId index);
  static void DestroyThreadLocal();

  static EngineShard* tlocal() {
    return shard_;
  }

  ShardId shard_id() const {
    return db_slice.shard_id();
  }

  ::util::fibers_ext::FiberQueue* GetQueue() {
    return &queue_;
  }

 private:
  EngineShard(ShardId index);

  ::util::fibers_ext::FiberQueue queue_;
  ::boost::fibers::fiber fiber_q_;

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
  void InitThreadLocal(ShardId index);

  template <typename F> auto Await(ShardId sid, F&& f) {
    return shard_queue_[sid]->Await(std::forward<F>(f));
  }

  template <typename F> auto Add(ShardId sid, F&& f) {
    assert(sid < shard_queue_.size());
    return shard_queue_[sid]->Add(std::forward<F>(f));
  }

  template <typename U> void RunBriefInParallel(U&& func);

 private:
  util::ProactorPool* pp_;
  std::vector<util::fibers_ext::FiberQueue*> shard_queue_;
};

/**
 * @brief
 *
 * @tparam U - a function that receives EngineShard* argument and returns void.
 * @param func
 */
template <typename U> void EngineShardSet::RunBriefInParallel(U&& func) {
  util::fibers_ext::BlockingCounter bc{size()};

  for (uint32_t i = 0; i < size(); ++i) {
    util::ProactorBase* dest = pp_->at(i);
    dest->AsyncBrief([f = std::forward<U>(func), bc]() mutable {
      f(EngineShard::tlocal());
      bc.Dec();
    });
  }
  bc.Wait();
}

}  // namespace dfly
