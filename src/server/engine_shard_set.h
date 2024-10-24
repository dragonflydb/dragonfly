// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

extern "C" {
#include "redis/sds.h"
}

#include <absl/container/btree_map.h>
#include <absl/container/flat_hash_map.h>
#include <xxhash.h>

#include "core/mi_memory_resource.h"
#include "server/db_slice.h"
#include "server/engine_shard.h"
#include "util/proactor_pool.h"
#include "util/sliding_counter.h"

namespace dfly {

namespace journal {
class Journal;
}  // namespace journal

class TieredStorage;
class ShardDocIndices;
class BlockingController;
class EngineShardSet;

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
    return size_;
  }

  util::ProactorPool* pool() {
    return pp_;
  }

  void Init(uint32_t size, std::function<void()> shard_handler);

  // Shutdown sequence:
  // - EngineShardSet.PreShutDown()
  // - Namespaces.Clear()
  // - EngineShardSet.Shutdown()
  void PreShutdown();
  void Shutdown();

  // Uses a shard queue to dispatch. Callback runs in a dedicated fiber.
  template <typename F> auto Await(ShardId sid, F&& f) {
    return shards_[sid]->GetFiberQueue()->Await(std::forward<F>(f));
  }

  // Uses a shard queue to dispatch. Callback runs in a dedicated fiber.
  template <typename F> auto Add(ShardId sid, F&& f) {
    assert(sid < size_);
    return shards_[sid]->GetFiberQueue()->Add(std::forward<F>(f));
  }

  template <typename F> auto AddL2(ShardId sid, F&& f) {
    return shards_[sid]->GetSecondaryQueue()->Add(std::forward<F>(f));
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
    util::fb2::BlockingCounter bc(size_);
    for (size_t i = 0; i < size_; ++i) {
      Add(i, [&func, bc]() mutable {
        func(EngineShard::tlocal());
        bc->Dec();
      });
    }

    bc->Wait();
  }

  // Used in tests
  void TEST_EnableCacheMode();

 private:
  void InitThreadLocal(util::ProactorBase* pb);
  util::ProactorPool* pp_;
  std::unique_ptr<EngineShard*[]> shards_;
  uint32_t size_ = 0;
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
