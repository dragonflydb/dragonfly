// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "util/fibers/fiberqueue_threadpool.h"
#include "util/fibers/fibers.h"

namespace dfly {

/**
 *  MPSC task-queue that is handled by a single consumer thread.
 *  The queue is just a wrapper around FiberQueue that manages its fiber itself.
 */
class TaskQueue {
 public:
  // TODO: to add a mechanism to moderate pool size. Currently it's static with pool_start_size.
  TaskQueue(unsigned queue_size, unsigned pool_start_size, unsigned pool_max_size);

  template <typename F> bool TryAdd(F&& f) {
    return queue_.TryAdd(std::forward<F>(f));
  }

  template <typename F> bool Add(F&& f) {
    if (queue_.TryAdd(std::forward<F>(f)))
      return true;

    ++blocked_submitters_;
    auto res = queue_.Add(std::forward<F>(f));
    --blocked_submitters_;
    return res;
  }

  template <typename F> auto Await(F&& f) -> decltype(f()) {
    util::fb2::Done done;
    using ResultType = decltype(f());
    util::detail::ResultMover<ResultType> mover;

    ++blocked_submitters_;
    Add([&mover, f = std::forward<F>(f), done]() mutable {
      mover.Apply(f);
      done.Notify();
    });
    --blocked_submitters_;
    done.Wait();
    return std::move(mover).get();
  }

  /**
   * @brief Start running consumer loop in the caller thread by spawning fibers.
   *        Returns immediately.
   */
  void Start(std::string_view base_name);

  /**
   * @brief Notifies Run() function to empty the queue and to exit and waits for the consumer
   *        fiber to finish.
   */
  void Shutdown();

  static unsigned blocked_submitters() {
    return blocked_submitters_;
  }

 private:
  util::fb2::FiberQueue queue_;
  std::vector<util::fb2::Fiber> consumer_fibers_;

  static __thread unsigned blocked_submitters_;
};

}  // namespace dfly
