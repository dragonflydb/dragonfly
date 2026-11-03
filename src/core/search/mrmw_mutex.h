// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <atomic>
#include <cstdint>
#include <mutex>

#include "base/logging.h"
#include "util/fibers/synchronization.h"

namespace dfly::search {

// Multi-reader multi-writer mutex: it is held either by readers or by writers, any number of them.
// Waiting suspends only the calling fiber. A write intent keeps new readers out while writers
// still get in, so a stream of readers can't starve a writer: a waiting writer registers one, and
// so does a deferred MRMWMutexLock. TryLock and Unlock never suspend. Locking recursively for
// reading may deadlock against a waiting writer.
class MRMWMutex {
 public:
  enum class LockMode : uint8_t { kReadLock, kWriteLock };

  void Lock(LockMode mode) {
    if (TryLock(mode))
      return;

    if (mode == LockMode::kReadLock) {
      waiters_.await([this] { return TryLock(LockMode::kReadLock); });
      return;
    }

    AddWriteIntent();
    waiters_.await([this] { return TryLock(LockMode::kWriteLock); });
    RemoveWriteIntent();
  }

  void Unlock(LockMode mode) {
    uint64_t prev = state_.fetch_sub(kRunner, std::memory_order_release);
    DCHECK(prev & kRunnerMask);
    DCHECK_EQ(bool(prev & kWriteMode), mode == LockMode::kWriteLock);
    if ((prev & kRunnerMask) == kRunner)
      waiters_.notifyAll();
  }

  // Non-blocking lock attempt. Returns true if the lock was acquired.
  bool TryLock(LockMode mode) {
    const bool write = mode == LockMode::kWriteLock;
    uint64_t state = state_.load(std::memory_order_relaxed);
    while (true) {
      const bool held = state & kRunnerMask;
      if (held && bool(state & kWriteMode) != write)
        return false;
      if (!write && (state & kIntentMask))
        return false;

      uint64_t next = state + kRunner;
      if (!held)
        next = write ? (next | kWriteMode) : (next & ~kWriteMode);
      if (state_.compare_exchange_weak(state, next, std::memory_order_acquire,
                                       std::memory_order_relaxed))
        return true;
    }
  }

  // Check if the mutex is currently held in read mode with at least one active runner.
  // For use in DCHECKs only.
  bool IsReadLocked() const {
    uint64_t state = state_.load(std::memory_order_relaxed);
    return (state & kRunnerMask) && !(state & kWriteMode);
  }

 private:
  friend class MRMWMutexLock;

  // Keeps new readers out until the matching RemoveWriteIntent.
  void AddWriteIntent() {
    state_.fetch_add(kIntent, std::memory_order_relaxed);
  }

  void RemoveWriteIntent() {
    uint64_t prev = state_.fetch_sub(kIntent, std::memory_order_relaxed);
    DCHECK(prev & kIntentMask);
    if ((prev & kIntentMask) == kIntent)
      waiters_.notifyAll();
  }

  // state_ packs the number of holders, the number of write intents and the mode of the holders.
  static constexpr uint64_t kRunner = 1;
  static constexpr uint64_t kRunnerMask = (1ULL << 32) - 1;
  static constexpr uint64_t kIntent = 1ULL << 32;
  static constexpr uint64_t kIntentMask = ((1ULL << 31) - 1) << 32;
  static constexpr uint64_t kWriteMode = 1ULL << 63;

  std::atomic_uint64_t state_{0};
  util::fb2::EventCount waiters_;
};

class MRMWMutexLock {
 public:
  // Suspends the calling fiber until the lock is acquired.
  explicit MRMWMutexLock(MRMWMutex* mutex, MRMWMutex::LockMode mode)
      : mutex_(mutex), lock_mode_(mode), locked_(true) {
    mutex->Lock(lock_mode_);
  }

  // Non-blocking try-lock. Check locked() to see if the lock was acquired.
  MRMWMutexLock(MRMWMutex* mutex, MRMWMutex::LockMode mode, std::try_to_lock_t)
      : mutex_(mutex), lock_mode_(mode), locked_(mutex->TryLock(mode)) {
  }

  // A write lock to take later with Lock(). Without waiting, it keeps new readers out until
  // released, so code that must not wait can hand its write to a fiber that can.
  MRMWMutexLock(MRMWMutex* mutex, MRMWMutex::LockMode mode, std::defer_lock_t)
      : mutex_(mutex), lock_mode_(mode), locked_(false), deferred_(true) {
    DCHECK(mode == MRMWMutex::LockMode::kWriteLock);
    mutex->AddWriteIntent();
  }

  // Suspends the calling fiber until the lock is acquired.
  void Lock() {
    DCHECK(!locked_);
    mutex_->Lock(lock_mode_);
    locked_ = true;
  }

  bool locked() const {
    return locked_;
  }

  ~MRMWMutexLock() {
    if (locked_)
      mutex_->Unlock(lock_mode_);
    if (deferred_)
      mutex_->RemoveWriteIntent();
  }

  MRMWMutexLock(const MRMWMutexLock&) = delete;
  MRMWMutexLock& operator=(const MRMWMutexLock&) = delete;

  MRMWMutexLock(MRMWMutexLock&& other) noexcept
      : mutex_(other.mutex_),
        lock_mode_(other.lock_mode_),
        locked_(other.locked_),
        deferred_(other.deferred_) {
    other.locked_ = other.deferred_ = false;
  }

  MRMWMutexLock& operator=(MRMWMutexLock&& other) noexcept {
    if (this != &other) {
      if (locked_)
        mutex_->Unlock(lock_mode_);
      if (deferred_)
        mutex_->RemoveWriteIntent();
      mutex_ = other.mutex_;
      lock_mode_ = other.lock_mode_;
      locked_ = other.locked_;
      deferred_ = other.deferred_;
      other.locked_ = other.deferred_ = false;
    }
    return *this;
  }

 private:
  MRMWMutex* mutex_;
  MRMWMutex::LockMode lock_mode_;
  bool locked_;
  bool deferred_ = false;  // Holds a write intent.
};

}  // namespace dfly::search
