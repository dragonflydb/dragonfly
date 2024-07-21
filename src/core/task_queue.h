// Copyright 2022, DragonflyDB authors.  All rights reserved.
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
  explicit TaskQueue(unsigned queue_size = 128) : queue_(queue_size) {
  }

  template <typename F> bool TryAdd(F&& f) {
    return queue_.TryAdd(std::forward<F>(f));
  }

  template <typename F> bool Add(F&& f) {
    return queue_.Add(std::forward<F>(f));
  }

  template <typename F> auto Await(F&& f) -> decltype(f()) {
    return queue_.Await(std::forward<F>(f));
  }

  /**
   * @brief Start running consumer loop in the caller thread by spawning fibers.
   *        Returns immediately.
   */
  void Start(std::string_view base_name) {
    consumer_fiber_ = util::fb2::Fiber(base_name, [this] { queue_.Run(); });
  }

  /**
   * @brief Notifies Run() function to empty the queue and to exit and waits for the consumer
   *        fiber to finish.
   */
  void Shutdown() {
    queue_.Shutdown();
    consumer_fiber_.JoinIfNeeded();
  }

 private:
  util::fb2::FiberQueue queue_;
  util::fb2::Fiber consumer_fiber_;
};

}  // namespace dfly
