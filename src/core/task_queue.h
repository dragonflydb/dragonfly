// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "util/fibers/fiberqueue_threadpool.h"
#include "util/fibers/fibers.h"

namespace dfly {

/**
 *  MPSC task-queue that is handled by a single consumer thread.
 *  The queue is just a wrapper around FiberQueue that manages its fibers itself.
 */
class TaskQueue {
 public:
  // TODO: to add a mechanism to moderate pool size. Currently it's static with pool_start_size.
  TaskQueue(unsigned queue_size, unsigned pool_start_size, unsigned pool_max_size);

  // Returns true if Add() had to preempt (block) because the queue was full.
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
  void Start(std::string_view base_name);

  /**
   * @brief Notifies Run() function to empty the queue and to exit and waits for the consumer
   *        fiber to finish.
   */
  void Shutdown();

  // Number of submitters on the current thread currently blocked adding into a full task queue.
  // Contention gauge maintained by FiberQueue::Add.
  static unsigned blocked_submitters() {
    return util::fb2::FiberQueue::blocked_submitters();
  }

 private:
  util::fb2::FiberQueue queue_;
  std::vector<util::fb2::Fiber> consumer_fibers_;
};

}  // namespace dfly
