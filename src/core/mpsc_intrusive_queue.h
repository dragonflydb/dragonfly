// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <atomic>
#include <cstddef>

// TODO: to move to helio

namespace dfly {
namespace detail {

// a MPSC queue where multiple threads push and a single thread pops.
//
// Requires global functions for T:
//
// T* MPSC_intrusive_load_next(const T& src)
// void MPSC_intrusive_store_next(T* next, T* dest);
// based on the design from here:
// https://www.1024cores.net/home/lock-free-algorithms/queues/intrusive-mpsc-node-based-queue
template <typename T> class MPSCIntrusiveQueue {
 private:
  static constexpr size_t cache_alignment = 64;
  static constexpr size_t cacheline_length = 64;

  alignas(cache_alignment) typename std::aligned_storage<sizeof(T), alignof(T)>::type storage_{};
  T* dummy_;
  alignas(cache_alignment) std::atomic<T*> head_;
  alignas(cache_alignment) T* tail_;
  char pad_[cacheline_length];

 public:
  MPSCIntrusiveQueue()
      : dummy_{reinterpret_cast<T*>(std::addressof(storage_))}, head_{dummy_}, tail_{dummy_} {
    MPSC_intrusive_store_next(dummy_, nullptr);
  }

  MPSCIntrusiveQueue(MPSCIntrusiveQueue const&) = delete;
  MPSCIntrusiveQueue& operator=(MPSCIntrusiveQueue const&) = delete;

  void Push(T* ctx) noexcept {
    // ctx becomes a new head.
    MPSC_intrusive_store_next(ctx, nullptr);
    T* prev = head_.exchange(ctx, std::memory_order_acq_rel);
    MPSC_intrusive_store_next(prev, ctx);
  }

  T* Pop() noexcept;
};

template <typename T> T* MPSCIntrusiveQueue<T>::Pop() noexcept {
  T* tail = tail_;

  //  tail->next_.load(std::memory_order_acquire);
  T* next = MPSC_intrusive_load_next(*tail);
  if (dummy_ == tail) {
    if (nullptr == next) {
      // empty
      return nullptr;
    }
    tail_ = next;
    tail = next;
    next = MPSC_intrusive_load_next(*next);
  }

  if (nullptr != next) {
    // non-empty
    tail_ = next;
    return tail;
  }

  T* head = head_.load(std::memory_order_acquire);
  if (tail != head) {
    // non-empty, retry is in order: we are in the middle of push.
    return nullptr;
  }

  Push(dummy_);

  next = MPSC_intrusive_load_next(*tail);
  if (nullptr != next) {
    tail_ = next;
    return tail;
  }

  // non-empty, retry is in order: we are still adding.
  return nullptr;
}

}  // namespace detail
}  // namespace dfly
