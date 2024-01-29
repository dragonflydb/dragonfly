// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "base/mpmc_bounded_queue.h"
#include "util/fibers/detail/result_mover.h"
#include "util/fibers/fibers.h"
#include "util/fibers/synchronization.h"
namespace dfly {

/**
 *  MPSC task-queue that is handled by a single consumer thread.
 *  The design is based on helio's FiberQueue with differrence that we can run multiple
 *  consumer Fibers in the consumer thread to allow better CPU utilization in case the
 *  tasks are not CPU bound.
 *
 *  Another difference - TaskQueue manages its consumer fibers itself.
 *  TODO: consider moving to util/fibers.
 */
class TaskQueue {
 public:
  explicit TaskQueue(unsigned consumer_fb_cnt, unsigned queue_size = 128);

  template <typename F> bool TryAdd(F&& f) {
    if (queue_.try_enqueue(std::forward<F>(f))) {
      pull_ec_.notify();
      return true;
    }
    return false;
  }

  /**
   * @brief Submits a callback into the queue. Should not be called after calling Shutdown().
   *
   * @tparam F - callback type
   * @param f  - callback object
   * @return true if Add() had to preempt, false is fast path without preemptions was followed.
   */
  template <typename F> bool Add(F&& f) {
    if (TryAdd(std::forward<F>(f))) {
      return false;
    }

    bool result = false;
    while (true) {
      auto key = push_ec_.prepareWait();

      if (TryAdd(std::forward<F>(f))) {
        break;
      }
      result = true;
      push_ec_.wait(key.epoch());
    }
    return result;
  }

  template <typename F> auto Await(F&& f) -> decltype(f()) {
    util::fb2::Done done;
    using ResultType = decltype(f());
    util::detail::ResultMover<ResultType> mover;

    Add([&mover, f = std::forward<F>(f), done]() mutable {
      mover.Apply(f);
      done.Notify();
    });

    done.Wait();
    return std::move(mover).get();
  }

  /**
   * @brief Start running consumer loop in the caller thread by spawning fibers.
   *        Returns immediately.
   */
  void Start(std::string_view base_name);

  /**
   * @brief Notifies Run() function to empty the queue and to exit.
   *        Does not block.
   */
  void Shutdown();

  // Returns number of callbacks being currently called by the queue.
  // Valid only from the consumer thread.
  unsigned concurrency_level() const {
    return concurrency_level_;
  }

 private:
  void TaskLoop();

  typedef std::function<void()> CbFunc;

  using FuncQ = base::mpmc_bounded_queue<CbFunc>;
  FuncQ queue_;

  util::fb2::EventCount push_ec_, pull_ec_;
  std::atomic_bool is_closed_{false};
  unsigned num_consumers_;
  std::unique_ptr<util::fb2::Fiber[]> consumer_fiber_;
  unsigned concurrency_level_ = 0;
};

}  // namespace dfly
