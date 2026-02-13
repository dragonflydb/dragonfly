// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/synchronization.h"

#include "base/logging.h"
#include "server/engine_shard_set.h"
#include "server/server_state.h"

namespace dfly {

ThreadLocalMutex::ThreadLocalMutex() {
  shard_ = EngineShard::tlocal();
}

ThreadLocalMutex::~ThreadLocalMutex() {
  DCHECK_EQ(EngineShard::tlocal(), shard_);
}

void ThreadLocalMutex::lock() {
  if (ServerState::tlocal()->serialization_max_chunk_size != 0) {
    DCHECK_EQ(EngineShard::tlocal(), shard_);
    util::fb2::NoOpLock noop_lk_;
    if (locked_fiber_ != nullptr) {
      DCHECK(util::fb2::detail::FiberActive() != locked_fiber_);
    }
    cond_var_.wait(noop_lk_, [this]() { return !flag_; });
    flag_ = true;
    DCHECK_EQ(locked_fiber_, nullptr);
    locked_fiber_ = util::fb2::detail::FiberActive();
  }
}

void ThreadLocalMutex::unlock() {
  if (ServerState::tlocal()->serialization_max_chunk_size != 0) {
    DCHECK_EQ(EngineShard::tlocal(), shard_);
    flag_ = false;
    cond_var_.notify_one();
    locked_fiber_ = nullptr;
  }
}

void LocalLatch::unlock() {
  DCHECK_GT(mutating_, 0u);
  --mutating_;
  if (mutating_ == 0) {
    cond_var_.notify_all();
  }
}

void LocalLatch::Wait() {
  util::fb2::NoOpLock noop_lk_;
  cond_var_.wait(noop_lk_, [this]() { return mutating_ == 0; });
}

}  // namespace dfly
