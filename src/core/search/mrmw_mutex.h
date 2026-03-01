// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <condition_variable>
#include <mutex>

#include "base/logging.h"
#include "base/spinlock.h"

namespace dfly::search {

// Simple implementation of multi-Reader multi-Writer Mutex
// MRMWMutex supports concurrent reads or concurrent writes but not a mix of
// concurrent reads and writes at the same time.

class MRMWMutex {
 public:
  enum class LockMode : uint8_t { kReadLock, kWriteLock };

  MRMWMutex() : lock_mode_(LockMode::kReadLock) {
  }

  void Lock(LockMode mode) {
    std::unique_lock lk(mutex_);

    // If we have any active_runners we need to check lock mode
    if (active_runners_) {
      auto& waiters = GetWaiters(mode);
      waiters++;
      GetCondVar(mode).wait(lk, [&] { return lock_mode_ == mode; });
      waiters--;
    } else {
      // No active runners so just update to requested lock mode
      lock_mode_ = mode;
    }
    active_runners_++;
  }

  void Unlock(LockMode mode) {
    std::lock_guard lk(mutex_);
    LockMode inverse_mode = GetInverseMode(mode);
    active_runners_--;
    // If this was last runner and there are waiters on inverse mode
    if (!active_runners_ && GetWaiters(inverse_mode) > 0) {
      lock_mode_ = inverse_mode;
      GetCondVar(inverse_mode).notify_all();
    }
  }

  // Check if the mutex is currently held in read mode with at least one active runner.
  // For use in DCHECKs only - not thread-safe without external synchronization.
  bool IsReadLocked() const {
    return active_runners_ > 0 && lock_mode_ == LockMode::kReadLock;
  }

  // Non-blocking lock attempt. Returns true if the lock was acquired.
  bool TryLock(LockMode mode) {
    if (!mutex_.try_lock()) {
      return false;
    }
    if (active_runners_ && lock_mode_ != mode) {
      mutex_.unlock();
      return false;
    }
    if (!active_runners_) {
      lock_mode_ = mode;
    }
    active_runners_++;
    mutex_.unlock();
    return true;
  }

 private:
  inline size_t& GetWaiters(LockMode target_mode) {
    return target_mode == LockMode::kReadLock ? reader_waiters_ : writer_waiters_;
  };

  inline std::condition_variable_any& GetCondVar(LockMode target_mode) {
    return target_mode == LockMode::kReadLock ? reader_cond_var_ : writer_cond_var_;
  };

  static inline LockMode GetInverseMode(LockMode mode) {
    return mode == LockMode::kReadLock ? LockMode::kWriteLock : LockMode::kReadLock;
  }

  // TODO: use fiber sync primitives in future
  base::SpinLock mutex_;
  std::condition_variable_any reader_cond_var_, writer_cond_var_;

  size_t writer_waiters_ = 0, reader_waiters_ = 0;
  size_t active_runners_ = 0;
  LockMode lock_mode_;
};

class MRMWMutexLock {
 public:
  // Blocking lock.
  explicit MRMWMutexLock(MRMWMutex* mutex, MRMWMutex::LockMode mode)
      : mutex_(mutex), lock_mode_(mode), locked_(true) {
    mutex->Lock(lock_mode_);
  }

  // Non-blocking try-lock. Check locked() to see if the lock was acquired.
  MRMWMutexLock(MRMWMutex* mutex, MRMWMutex::LockMode mode, std::try_to_lock_t)
      : mutex_(mutex), lock_mode_(mode), locked_(mutex->TryLock(mode)) {
  }

  bool locked() const {
    return locked_;
  }

  ~MRMWMutexLock() {
    if (locked_)
      mutex_->Unlock(lock_mode_);
  }

  MRMWMutexLock(const MRMWMutexLock&) = delete;
  MRMWMutexLock(MRMWMutexLock&&) = delete;
  MRMWMutexLock& operator=(const MRMWMutexLock&) = delete;
  MRMWMutexLock& operator=(MRMWMutexLock&&) = delete;

 private:
  MRMWMutex* const mutex_;
  MRMWMutex::LockMode lock_mode_;
  bool locked_;
};

}  // namespace dfly::search
