// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <memory>

#include "server/engine_shard_set.h"
#include "util/proactor_pool.h"

namespace dfly {

// Allows duplicating some class T to all threads via thread-local storage.
// Calls to Set() will copy the shared_ptr<T> to thread_local variables, which can then be quickly
// accessed without locks or atomic operations locally.
// This class is thread-safe, but is not copyable nor movable.
template <typename T> class SharedThreadLocal {
 public:
  static bool HasValue() {
    return tl_t_ != nullptr;
  }

  static T* Get() {
    return tl_t_.get();
  }

  static void Set(T t) {
    Set(std::make_shared<T>(std::move(t)));
  }

  static void Set(std::shared_ptr<T> t) {
    auto cb = [&](util::ProactorBase* pb) { tl_t_ = t; };
    shard_set->pool()->AwaitFiberOnAll(std::move(cb));
  }

 private:
  static thread_local std::shared_ptr<T> tl_t_;
};

template <typename T> thread_local std::shared_ptr<T> SharedThreadLocal<T>::tl_t_;

}  // namespace dfly
