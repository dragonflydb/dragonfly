// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "time_sliced_mrmw_mutex.h"

#include <absl/base/optimization.h>
#include <absl/synchronization/mutex.h>
#include <absl/time/time.h>

#include <algorithm>
#include <optional>

#include "base/logging.h"

namespace dfly::search {

void MRMWReaderMutexLock::SetMayProlong() {
  if (may_prolong_) {
    return;
  }
  may_prolong_ = true;
  mutex_->IncMayProlongCount();
}

void MRMWWriterMutexLock::SetMayProlong() {
  if (may_prolong_) {
    return;
  }
  may_prolong_ = true;
  mutex_->IncMayProlongCount();
}

TimeSlicedMRMWMutex::TimeSlicedMRMWMutex(const MRMWMutexOptions& options)
    : read_quota_duration_(options.read_quota_duration),
      read_switch_grace_period_(options.read_switch_grace_period),
      write_quota_duration_(options.write_quota_duration),
      write_switch_grace_period_(options.write_switch_grace_period) {
  DCHECK(read_quota_duration_ > read_switch_grace_period_);
  DCHECK(write_quota_duration_ > write_switch_grace_period_);
}

void TimeSlicedMRMWMutex::IncMayProlongCount() {
  absl::MutexLock lock(&mutex_);
  ++may_prolong_count_;
}

void TimeSlicedMRMWMutex::ReaderLock(bool& may_prolong) {
  Lock(Mode::kLockRead, may_prolong);
}

void TimeSlicedMRMWMutex::WriterLock(bool& may_prolong) {
  Lock(Mode::kLockWrite, may_prolong);
}

void TimeSlicedMRMWMutex::Lock(Mode target_mode, bool& may_prolong) {
  absl::MutexLock lock(&mutex_);
  const Mode inverse_mode = GetInverseMode(target_mode);
  if (current_mode_ == target_mode) {
    if (ABSL_PREDICT_FALSE(HasTimeQuotaExceeded() && GetWaiters(inverse_mode) > 0 &&
                           (may_prolong || may_prolong_count_ == 0))) {
      SwitchWithWait(target_mode);
    }
  } else {
    SwitchWithWait(target_mode);
  }
  if (ABSL_PREDICT_FALSE(may_prolong)) {
    ++may_prolong_count_;
  }
  last_lock_acquired_.Reset();
  ++active_lock_count_;
}

void TimeSlicedMRMWMutex::Unlock(bool may_prolong) {
  absl::MutexLock lock(&mutex_);
  DCHECK_GT(active_lock_count_, 0L);
  --active_lock_count_;
  if (ABSL_PREDICT_FALSE(may_prolong)) {
    DCHECK_GT(may_prolong_count_, 0L);
    --may_prolong_count_;
  }
  if (ShouldSwitch() && GetWaiters(GetInverseMode(current_mode_)) > 0) {
    InitSwitch();
  }
}

void TimeSlicedMRMWMutex::WaitSwitch(Mode target_mode) {
  auto& wait_with_timer = WaitWithTimer(target_mode);
  auto has_time_quota_exceeded = HasTimeQuotaExceeded();
  // The following logic determines whether to wait with a timer:
  // 1. This thread is the first one waiting with a timer for the target
  // mode.
  // 2. The time quota has been exceeded, but there are still active threads in
  // the opposite mode. In this case, the mode switch is triggered by the last
  // remaining active thread, hence the wait with a timer is not needed.
  if (wait_with_timer ||
      (current_mode_ != target_mode && has_time_quota_exceeded && active_lock_count_ > 0)) {
    GetCondVar(target_mode).Wait(&mutex_);
  } else {
    wait_with_timer = true;
    GetCondVar(target_mode).WaitWithTimeout(&mutex_, CalcWaitTime());
    wait_with_timer = false;
  }
}

absl::Duration TimeSlicedMRMWMutex::CalcWaitTime() const {
  auto grace_period = CalcEffectiveGraceTime();
  if (last_lock_acquired_.Duration() >= grace_period) {
    return grace_period;
  }
  return (grace_period - last_lock_acquired_.Duration());
}

absl::Duration TimeSlicedMRMWMutex::CalcEffectiveGraceTime() const {
  // Prefer latency when the concurrency is extremely low.
  if (ABSL_PREDICT_FALSE(MinimizeWaitTime())) {
    return std::min(absl::Milliseconds(1), GetSwitchGracePeriod() / 10);
  }
  return GetSwitchGracePeriod();
}

bool TimeSlicedMRMWMutex::MinimizeWaitTime() const {
  return (GetWaitersConst(current_mode_) + GetWaitersConst(GetInverseMode(current_mode_)) +
              active_lock_count_ <=
          2);
}

// Switch mode may occur only once there are no activity in the active mode,
// active locks is 0, and one of the followings:
// 1. An early switch, a switch before the active mode quota elapsed, may
// occur when the active mode doesn't has activity for sometime, based on
// the mode's poll interval, while the inverse has. Note that we don't
// want to switch mode immediately when there are no active locks, but
// wait for the mode's poll interval, to avoid starving the readers.
// 2. A switch may occur when the active mode's quota elapsed.
bool TimeSlicedMRMWMutex::ShouldSwitch() const {
  return ABSL_PREDICT_FALSE(
      active_lock_count_ == 0 && !switch_wait_mode_.has_value() &&
      (last_lock_acquired_.Duration() >= CalcEffectiveGraceTime() || HasTimeQuotaExceeded()));
}

void TimeSlicedMRMWMutex::InitSwitch() {
  DCHECK(!switch_wait_mode_.has_value());
  auto target_mode = GetInverseMode(current_mode_);
  // The first target mode waiter is responsible to early wake up the other
  // target mode waiters
  switch_wait_mode_ = target_mode;
  GetCondVar(target_mode).SignalAll();
  current_mode_ = target_mode;
}

void TimeSlicedMRMWMutex::SwitchWithWait(Mode target_mode) {
  auto& waiters = GetWaiters(target_mode);
  ++waiters;
  auto captured_switches = switches_;
  bool require_switch = target_mode == current_mode_;
  DCHECK(!require_switch || !switch_wait_mode_.has_value());
  do {
    WaitSwitch(target_mode);
    // switch_wait_mode_ may have a value once the first thread with the right
    // mode manages to exit the loop and its value is cleared right after all
    // threads, with the corresponding mode value, exit the loop.
    if (switch_wait_mode_.has_value()) {
      if (switch_wait_mode_.value() == target_mode) {
        break;
      }
      continue;
    }
    // require_switch is true indicates that SwitchWithWait was entered
    // because the active mode's quota was exceeded. In such case,
    // require_switch is used to make sure that such threads remain in the loop
    // until after a switch to the inverse mode occurs.
    if (require_switch && captured_switches == switches_ &&
        GetWaiters(GetInverseMode(target_mode)) > 0) {
      continue;
    }
    if (ShouldSwitch()) {
      InitSwitch();
      break;
    }
  } while (true);
  --waiters;
  if (ABSL_PREDICT_FALSE(waiters == 0)) {
    // The last target mode waiter is responsible to finalize the switch mode
    // process
    switch_wait_mode_ = std::nullopt;
    stop_watch_.Reset();
    ++switches_;
  }
}

}  // namespace dfly::search
