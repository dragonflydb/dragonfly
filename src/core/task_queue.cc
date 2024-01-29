// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/task_queue.h"

#include <absl/strings/str_cat.h>

#include "base/logging.h"

using namespace std;
using namespace util;

namespace dfly {

TaskQueue::TaskQueue(unsigned consumer_fb_cnt, unsigned queue_size)
    : queue_(queue_size),
      num_consumers_(consumer_fb_cnt),
      consumer_fiber_(new fb2::Fiber[consumer_fb_cnt]) {
}

void TaskQueue::Start(string_view base_name) {
  CHECK_GT(num_consumers_, 0u);

  for (unsigned i = 0; i < num_consumers_; ++i) {
    CHECK(!consumer_fiber_[i].IsJoinable());

    string name = absl::StrCat(base_name, "_", i);
    consumer_fiber_[i] = fb2::Fiber(name, [this] { TaskLoop(); });
  }
}

void TaskQueue::Shutdown() {
  is_closed_.store(true, memory_order_seq_cst);
  pull_ec_.notifyAll();
  for (unsigned i = 0; i < num_consumers_; ++i) {
    consumer_fiber_[i].Join();
  }
  consumer_fiber_.reset();
}

void TaskQueue::TaskLoop() {
  bool is_closed = false;
  CbFunc func;

  auto cb = [&] {
    if (queue_.try_dequeue(func)) {
      push_ec_.notify();
      return true;
    }

    if (is_closed_.load(std::memory_order_acquire)) {
      is_closed = true;
      return true;
    }
    return false;
  };

  while (true) {
    pull_ec_.await(cb);

    if (is_closed)
      break;
    try {
      concurrency_level_++;
      func();
      concurrency_level_--;  // We check-fail on exception, so it's safe to decrement here.
    } catch (std::exception& e) {
      LOG(FATAL) << "Exception " << e.what();
    }
  }
}

}  // namespace dfly
