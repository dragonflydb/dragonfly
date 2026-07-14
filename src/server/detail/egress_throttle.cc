// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/detail/egress_throttle.h"

#include <absl/time/clock.h>

#include <algorithm>
#include <chrono>
#include <limits>

#include "base/logging.h"
#include "util/fibers/fibers.h"

namespace dfly::detail {

using namespace std;

namespace {

// The low priority stream is guaranteed at least limit/kLowPrioReserveDivisor of egress.
constexpr uint64_t kLowPrioReserveDivisor = 10;  // 1/10 = 10%

// Burst tolerance (GCRA tau): how far ahead of schedule a stream may run. Absorbs chunk
// granularity and avoids tiny frequent sleeps without affecting the long-run average rate.
constexpr uint64_t kBurstToleranceUs = 100'000;  // 100ms

constexpr uint64_t kMicrosPerSec = 1'000'000;

// CycleClock may drift in real time units, cached monotonic time is updated only once per scheduler
// round, so use real time with higher query cost and possible backwards looping
inline uint64_t NowUs() {
  return static_cast<uint64_t>(absl::GetCurrentTimeNanos()) / 1000;
}

}  // namespace

void EgressThrottler::RecordAt(uint64_t bytes, bool high_prio, uint64_t now_us) {
  DCHECK_GT(limit_, 0u);
  // bytes * kMicrosPerSec would overflow only at ~18TB
  DCHECK_GT(std::numeric_limits<uint64_t>::max() / kMicrosPerSec, bytes);

  total_tat_ = std::max(now_us, total_tat_) + bytes * kMicrosPerSec / limit_;
  if (!high_prio) {
    uint64_t reserve = std::max<uint64_t>(1, limit_ / kLowPrioReserveDivisor);
    low_prio_tat_ = std::max(now_us, low_prio_tat_) + bytes * kMicrosPerSec / reserve;
  }
}

uint64_t EgressThrottler::WakeTime(uint64_t now_us) const {
  if (total_tat_ <= now_us + kBurstToleranceUs)  // total egress under budget
    return 0;

  if (low_prio_tat_ <= now_us + kBurstToleranceUs)  // low priority reserved share
    return 0;

  return std::min(total_tat_, low_prio_tat_) - kBurstToleranceUs;
}

void EgressThrottler::Record(uint64_t bytes, bool high_prio) {
  RecordAt(bytes, high_prio, NowUs());
}

void EgressThrottler::Throttle() const {
  while (true) {
    uint64_t now = NowUs();
    uint64_t wake = WakeTime(now);
    if (wake == 0)
      break;

    DCHECK_GT(wake, now);
    util::ThisFiber::SleepFor(chrono::microseconds(wake - now + 1));
  }
}

}  // namespace dfly::detail
