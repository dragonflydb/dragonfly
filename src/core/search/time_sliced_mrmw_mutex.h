// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

// Following code is mutex implementation from valkey-search that is used with HNSW index to support
// thread safe writers/readers.

#pragma once

#include <absl/base/thread_annotations.h>
#include <absl/synchronization/mutex.h>
#include <absl/time/time.h>

#include <cstdint>
#include <optional>

namespace dfly::search {

class StopWatch {
 public:
  StopWatch() {
    Reset();
  }
  ~StopWatch() = default;
  void Reset() {
    start_time_ = absl::Now();
  }
  absl::Duration Duration() const {
    return absl::Now() - start_time_;
  }

 private:
  absl::Time start_time_;
};

struct MRMWMutexOptions {
  absl::Duration read_quota_duration;
  absl::Duration read_switch_grace_period;
  absl::Duration write_quota_duration;
  absl::Duration write_switch_grace_period;
};

// Time Sliced Multi-Reader Multi-Writer Mutex
// MRMWMutex supports concurrent reads or concurrent writes but not a mix of
// concurrent reads and writes at the same time. It allows fine-tuning of
// behavior with the following parameters:
// 1. read/write_switch_grace_period: The inactivity interval of the mode
// to initiate a switch.
// 2. read/write_quota_duration: Defines the maximum duration to remain in the
// active mode while the inverse mode is requested to be acquired.
//
// These parameters can be used to prioritize reads over writes or vice versa.
class ABSL_LOCKABLE TimeSlicedMRMWMutex {
 public:
  explicit TimeSlicedMRMWMutex(const MRMWMutexOptions& options);
  TimeSlicedMRMWMutex(const TimeSlicedMRMWMutex&) = delete;
  TimeSlicedMRMWMutex& operator=(const TimeSlicedMRMWMutex&) = delete;
  ~TimeSlicedMRMWMutex() = default;
  enum class Mode {
    kLockRead,
    kLockWrite,
  };
  void ReaderLock(bool& may_prolong) ABSL_SHARED_LOCK_FUNCTION() ABSL_LOCKS_EXCLUDED(mutex_);
  void WriterLock(bool& may_prolong) ABSL_SHARED_LOCK_FUNCTION() ABSL_LOCKS_EXCLUDED(mutex_);

  void Unlock(bool may_prolong) ABSL_UNLOCK_FUNCTION();

  void IncMayProlongCount() ABSL_LOCKS_EXCLUDED(mutex_);

 private:
  void Lock(Mode target_mode, bool& may_prolong) ABSL_LOCKS_EXCLUDED(mutex_);
  inline uint32_t& GetWaiters(Mode target_mode) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_) {
    return target_mode == Mode::kLockRead ? reader_waiters_ : writer_waiters_;
  };
  inline uint32_t GetWaitersConst(Mode target_mode) const ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_) {
    return target_mode == Mode::kLockRead ? reader_waiters_ : writer_waiters_;
  };
  inline absl::CondVar& GetCondVar(Mode target_mode) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_) {
    return target_mode == Mode::kLockRead ? read_cond_var_ : write_cond_var_;
  };
  inline const absl::Duration& GetTimeQuota(Mode target_mode) const
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_) {
    return target_mode == Mode::kLockRead ? read_quota_duration_ : write_quota_duration_;
  };
  inline bool& WaitWithTimer(Mode target_mode) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_) {
    return target_mode == Mode::kLockRead ? read_wait_with_timer_ : write_wait_with_timer_;
  };
  const absl::Duration& GetSwitchGracePeriod() const ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_) {
    return current_mode_ == Mode::kLockRead ? read_switch_grace_period_
                                            : write_switch_grace_period_;
  }
  absl::Duration CalcWaitTime() const ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  absl::Duration CalcEffectiveGraceTime() const ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  bool MinimizeWaitTime() const ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  bool ShouldSwitch() const ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  void InitSwitch() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  inline bool HasTimeQuotaExceeded() const ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_) {
    return !switch_wait_mode_.has_value() && stop_watch_.Duration() > GetTimeQuota(current_mode_);
  }
  void SwitchWithWait(Mode target_mode) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  void WaitSwitch(Mode target_mode) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  static inline Mode GetInverseMode(Mode target_mode) {
    return target_mode == Mode::kLockRead ? Mode::kLockWrite : Mode::kLockRead;
  }

  mutable absl::Mutex mutex_;
  Mode current_mode_ ABSL_GUARDED_BY(mutex_){Mode::kLockRead};
  int active_lock_count_ ABSL_GUARDED_BY(mutex_){0};
  StopWatch last_lock_acquired_ ABSL_GUARDED_BY(mutex_);
  absl::CondVar write_cond_var_ ABSL_GUARDED_BY(mutex_);
  absl::CondVar read_cond_var_ ABSL_GUARDED_BY(mutex_);
  uint32_t reader_waiters_ ABSL_GUARDED_BY(mutex_){0};
  uint32_t writer_waiters_ ABSL_GUARDED_BY(mutex_){0};
  bool read_wait_with_timer_ ABSL_GUARDED_BY(mutex_){false};
  bool write_wait_with_timer_ ABSL_GUARDED_BY(mutex_){false};
  const absl::Duration read_quota_duration_;
  const absl::Duration read_switch_grace_period_;
  const absl::Duration write_quota_duration_;
  const absl::Duration write_switch_grace_period_;
  std::optional<Mode> switch_wait_mode_ ABSL_GUARDED_BY(mutex_);
  StopWatch stop_watch_ ABSL_GUARDED_BY(mutex_);
  int switches_ ABSL_GUARDED_BY(mutex_){0};
  uint32_t may_prolong_count_ ABSL_GUARDED_BY(mutex_){0};
};

class ABSL_SCOPED_LOCKABLE MRMWReaderMutexLock {
 public:
  explicit MRMWReaderMutexLock(TimeSlicedMRMWMutex* mutex, bool may_prolong = false)
      ABSL_SHARED_LOCK_FUNCTION(mutex)
      : mutex_(mutex), may_prolong_(may_prolong) {
    timer_.Reset();
    mutex->ReaderLock(may_prolong_);
  }

  MRMWReaderMutexLock(const MRMWReaderMutexLock&) = delete;
  MRMWReaderMutexLock(MRMWReaderMutexLock&&) = delete;
  MRMWReaderMutexLock& operator=(const MRMWReaderMutexLock&) = delete;
  MRMWReaderMutexLock& operator=(MRMWReaderMutexLock&&) = delete;
  void SetMayProlong();
  ~MRMWReaderMutexLock() ABSL_UNLOCK_FUNCTION() {
    mutex_->Unlock(may_prolong_);
  }

 private:
  TimeSlicedMRMWMutex* const mutex_;
  bool may_prolong_;
  StopWatch timer_;
};

class ABSL_SCOPED_LOCKABLE MRMWWriterMutexLock {
 public:
  explicit MRMWWriterMutexLock(TimeSlicedMRMWMutex* mutex, bool may_prolong = false)
      ABSL_SHARED_LOCK_FUNCTION(mutex)
      : mutex_(mutex), may_prolong_(may_prolong) {
    timer_.Reset();
    mutex->WriterLock(may_prolong_);
  }

  MRMWWriterMutexLock(const MRMWWriterMutexLock&) = delete;
  MRMWWriterMutexLock(MRMWWriterMutexLock&&) = delete;
  MRMWWriterMutexLock& operator=(const MRMWWriterMutexLock&) = delete;
  MRMWWriterMutexLock& operator=(MRMWWriterMutexLock&&) = delete;
  void SetMayProlong();
  ~MRMWWriterMutexLock() ABSL_UNLOCK_FUNCTION() {
    mutex_->Unlock(may_prolong_);
  }

 private:
  TimeSlicedMRMWMutex* const mutex_;
  bool may_prolong_;
  StopWatch timer_;
};

}  // namespace dfly::search
