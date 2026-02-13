// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "util/fibers/fibers.h"
#include "util/fibers/synchronization.h"

namespace dfly {

class EngineShard;

// Helper class used to guarantee atomicity between serialization of buckets
class ABSL_LOCKABLE ThreadLocalMutex {
 public:
  ThreadLocalMutex();
  ~ThreadLocalMutex();

  void lock() ABSL_EXCLUSIVE_LOCK_FUNCTION();
  void unlock() ABSL_UNLOCK_FUNCTION();

 private:
  EngineShard* shard_;
  util::fb2::CondVarAny cond_var_;
  bool flag_ = false;
  util::fb2::detail::FiberInterface* locked_fiber_{nullptr};
};

// Replacement of std::SharedLock that allows -Wthread-safety
template <typename Mutex> class ABSL_SCOPED_LOCKABLE SharedLock {
 public:
  explicit SharedLock(Mutex& m) ABSL_EXCLUSIVE_LOCK_FUNCTION(m) : m_(m) {
    m_.lock_shared();
    is_locked_ = true;
  }

  ~SharedLock() ABSL_UNLOCK_FUNCTION() {
    if (is_locked_) {
      m_.unlock_shared();
    }
  }

  void unlock() ABSL_UNLOCK_FUNCTION() {
    m_.unlock_shared();
    is_locked_ = false;
  }

 private:
  Mutex& m_;
  bool is_locked_;
};

// A single threaded latch that passes a waiter fiber if its count is 0.
// Fibers that increase/decrease the count do not wait on the latch.
class LocalLatch {
 public:
  void lock() {
    ++mutating_;
  }

  void unlock();

  void Wait();

  bool IsBlocked() const {
    return mutating_ > 0;
  }

 private:
  util::fb2::CondVarAny cond_var_;
  size_t mutating_ = 0;
};

}  // namespace dfly
