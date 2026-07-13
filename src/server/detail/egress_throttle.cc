// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/detail/egress_throttle.h"

#include <absl/time/clock.h>

#include <algorithm>
#include <chrono>

#include "base/logging.h"
#include "util/fibers/fibers.h"

namespace dfly::detail {

using namespace std;

namespace {

// CycleClock may drift in real time units, cached monotonic time is updated only once per scheduler
// round, so use real time with higher query cost and possible backwards looping
inline uint64_t NowUs() {
  return static_cast<uint64_t>(absl::GetCurrentTimeNanos()) / 1000;
}

}  // namespace

void EgressThrottler::RecordAt(uint64_t bytes, bool out_of_order, uint64_t now_us) {
  if (!enabled())
    return;

  // bytes * kMicrosPerSec would overflow only at ~18TB
  DCHECK_GT(std::numeric_limits<uint64_t>::max() / kMicrosPerSec, bytes);

  socket_tat_ = std::max(now_us, socket_tat_) + bytes * kMicrosPerSec / limit_;
  if (!out_of_order) {
    uint64_t reserve = std::max<uint64_t>(1, limit_ / kLoopReserveDivisor);
    loop_tat_ = std::max(now_us, loop_tat_) + bytes * kMicrosPerSec / reserve;
  }
}

uint64_t EgressThrottler::WakeTime(uint64_t now_us) const {
  if (!enabled())
    return 0;

  if (socket_tat_ <= now_us + kBurstToleranceUs)  // socket under budget
    return 0;

  if (loop_tat_ <= now_us + kBurstToleranceUs)  // loops share
    return 0;

  return std::min(socket_tat_, loop_tat_) - kBurstToleranceUs;
}

void EgressThrottler::Record(uint64_t bytes, bool out_of_order) {
  RecordAt(bytes, out_of_order, NowUs());
}

void EgressThrottler::Throttle() const {
  if (!enabled())
    return;

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
