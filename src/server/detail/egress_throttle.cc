// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/detail/egress_throttle.h"

#include <absl/time/clock.h>

#include <chrono>

#include "util/fibers/fibers.h"

namespace dfly::detail {

using namespace std;

void EgressThrottler::Record(uint64_t bytes, bool out_of_order) {
  if (!enabled())
    return;

  socket_.IncBy(bytes);
  if (!out_of_order)
    loop_.IncBy(bytes);
}

void EgressThrottler::Throttle() {
  if (!enabled())
    return;

  const uint64_t reserve = static_cast<uint64_t>(limit_ * kGuaranteedRatio);
  while (true) {
    if (socket_.Sum() <= limit_)  // under budget
      break;

    if (loop_.Sum() < reserve)  // not reached guaranteed throughput
      break;

    // Suspend until the current slice ends and stale bytes roll off - the earliest point
    // Sum() can decrease. The wait is the real time until the next slot boundary.
    uint64_t now_ns = static_cast<uint64_t>(absl::GetCurrentTimeNanos());
    uint64_t wait_ns = kEgressBucketNs - (now_ns % kEgressBucketNs);
    util::ThisFiber::SleepFor(chrono::microseconds(wait_ns / 1000 + 1));
  }
}

}  // namespace dfly::detail
