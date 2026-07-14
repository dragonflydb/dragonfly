// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/detail/egress_throttle.h"

#include <gtest/gtest.h>

#include "base/gtest.h"

namespace dfly::detail {

using namespace std;

// Base timestamp (us) to mimic realistic absl::GetCurrentTimeNanos()/1000 magnitudes.
constexpr uint64_t kT0 = 1'700'000'000'000'000ULL;
constexpr uint64_t kTau = 100'000;             // matches kBurstToleranceUs (100ms) in the .cc
constexpr uint64_t kMicrosPerSec = 1'000'000;  // matches the .cc

// Under the limit the loop is never throttled.
TEST(EgressThrottlerTest, ConformingProceeds) {
  EgressThrottler t{1'000'000};  // 1 MB/s
  t.RecordAt(100'000, false, kT0);
  EXPECT_EQ(t.WakeTime(kT0), 0u);  // only 0.1s worth, within burst tolerance
}

// Sending a full second of budget at once forces a wait until the schedule catches up.
TEST(EgressThrottlerTest, OverBudgetSleepsUntilSchedule) {
  const uint64_t limit = 1'000'000;
  EgressThrottler t{limit};

  t.RecordAt(limit, false, kT0);  // 1 second worth of bytes at t0
  // total_tat_ = kT0 + 1s. It is conforming once now >= total_tat_ - tau.
  uint64_t wake = t.WakeTime(kT0);
  EXPECT_EQ(wake, kT0 + kMicrosPerSec - kTau);

  // At the wake time it may proceed.
  EXPECT_EQ(t.WakeTime(wake), 0u);
}

// High priority writes advance the total schedule but must not throttle the loop while the loop
// is still under its reserved (low priority) share (progress guarantee).
TEST(EgressThrottlerTest, HighPrioDoesNotStarveLowPrio) {
  const uint64_t limit = 1'000'000;
  EgressThrottler t{limit};

  // A huge high priority burst saturates the total budget for many seconds, yet the loop (which has
  // sent nothing) is still allowed to proceed because its own reserved share is untouched.
  t.RecordAt(10 * limit, true, kT0);
  EXPECT_EQ(t.WakeTime(kT0), 0u);
}

// Once the low priority stream itself has consumed its reserved share, it does get throttled under
// total saturation.
TEST(EgressThrottlerTest, LowPrioThrottledAfterReservedShare) {
  const uint64_t limit = 1'000'000;
  EgressThrottler t{limit};

  // Saturate the total budget via high priority load.
  t.RecordAt(10 * limit, true, kT0);
  // Low priority sends more than its reserved 0.1s worth (reserve rate = 0.1 * limit).
  t.RecordAt(limit / 5, false, kT0);  // 0.2 * limit -> low_prio_tat_ ~ kT0 + 2s

  EXPECT_GT(t.WakeTime(kT0), 0u);  // both total and low priority over budget -> throttle
}

// A backward clock step (e.g. NTP) must not corrupt the schedule: bytes attribute to the
// current head, never rewinding the TAT.
TEST(EgressThrottlerTest, BackwardClockDoesNotRewind) {
  const uint64_t limit = 1'000'000;
  EgressThrottler t{limit};

  t.RecordAt(limit, false, kT0 + kMicrosPerSec);
  uint64_t wake_before = t.WakeTime(kT0);

  // Clock steps back; TAT should still reflect the earlier (larger) schedule.
  t.RecordAt(1, false, kT0);
  EXPECT_GE(t.WakeTime(kT0), wake_before);
}

}  // namespace dfly::detail
