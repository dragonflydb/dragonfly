// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <utility>

#include "util/fibers/synchronization.h"

namespace cmn {

// A minimal mutex-guarded value. Assignment (last-wins) and dereference are the only operations,
// each taking the (fiber-aware) mutex, so it is meant for cold paths where a full custom holder
// would be overkill. Dereference returns a copy to avoid handing out a reference outside the lock.
template <typename T> class ThreadSafeValue {
 public:
  ThreadSafeValue() = default;
  explicit ThreadSafeValue(T val) : val_(std::move(val)) {
  }

  ThreadSafeValue& operator=(T val) {
    util::fb2::LockGuard lk(mu_);
    val_ = std::move(val);
    return *this;
  }

  T operator*() const {
    util::fb2::LockGuard lk(mu_);
    return val_;
  }

 private:
  mutable util::fb2::Mutex mu_;
  T val_{};
};

}  // namespace cmn
