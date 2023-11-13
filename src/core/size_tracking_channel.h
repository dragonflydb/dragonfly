// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "core/fibers.h"

namespace dfly {

// SimpleQueue-like interface, but also keeps track over the size of Ts it owns.
// It has a slightly less efficient TryPush() API as it forces construction of Ts even if they are
// not pushed.
// T must have a .size() method, which should return the heap-allocated size of T, excluding
// anything included in sizeof(T). We could generalize this in the future.
template <typename T, typename Queue = folly::ProducerConsumerQueue<T>> class SizeTrackingChannel {
 public:
  SizeTrackingChannel(size_t n, unsigned num_producers = 1) : queue_(n, num_producers) {
  }

  // Here and below, we must accept a T instead of building it from variadic args, as we need to
  // know its size in case it is added.
  void Push(T t) noexcept {
    size_ += t.size();
    queue_.Push(std::move(t));
  }

  bool TryPush(T t) noexcept {
    const size_t tmp_size = t.size();
    if (queue_.TryPush(std::move(t))) {
      size_ += tmp_size;
      return true;
    }

    return false;
  }

  bool Pop(T& dest) {
    if (queue_.Pop(dest)) {
      size_ -= dest.size();
      return true;
    }

    return false;
  }

  void StartClosing() {
    queue_.StartClosing();
  }

  bool TryPop(T& dest) {
    if (queue_.TryPop(dest)) {
      size_ -= dest.size();
      return true;
    }

    return false;
  }

  bool IsClosing() const {
    return queue_.IsClosing();
  }

  size_t GetSize() const {
    return queue_.Capacity() * sizeof(T) + size_;
  }

 private:
  SimpleChannel<T, Queue> queue_;
  size_t size_ = 0;
};

}  // namespace dfly
