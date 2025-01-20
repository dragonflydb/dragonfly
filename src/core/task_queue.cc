// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/task_queue.h"

#include <absl/strings/str_cat.h>

#include "base/logging.h"

using namespace std;
namespace dfly {

__thread unsigned TaskQueue::blocked_submitters_ = 0;

TaskQueue::TaskQueue(unsigned queue_size, unsigned start_size, unsigned pool_max_size)
    : queue_(queue_size), consumer_fibers_(start_size) {
  CHECK_GT(start_size, 0u);
  CHECK_LE(start_size, pool_max_size);
}

void TaskQueue::Start(std::string_view base_name) {
  for (size_t i = 0; i < consumer_fibers_.size(); ++i) {
    auto& fb = consumer_fibers_[i];
    CHECK(!fb.IsJoinable());

    string name = absl::StrCat(base_name, "/", i);
    fb = util::fb2::Fiber(name, [this] { queue_.Run(); });
  }
}

void TaskQueue::Shutdown() {
  queue_.Shutdown();
  for (auto& fb : consumer_fibers_)
    fb.JoinIfNeeded();
}

}  // namespace dfly
