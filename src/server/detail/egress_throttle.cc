// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/detail/egress_throttle.h"

#include <absl/time/clock.h>

#include <chrono>

#include "util/fibers/fibers.h"

namespace dfly::detail {

using namespace std;

namespace {

// Current time. More costly than CycleClock, but predictable per-second scale. Accurate values
// compared to GetMonotonicTimeNs with big scheduler round trip
inline uint64_t NowNs() {
  return absl::GetCurrentTimeNanos();
}

}  // namespace

void SlidingTracker::Advance(uint64_t slot) {
  if (slot <= cur_ts_)
    return;

  if (total_ == 0) {  // avoid loops if no update happened
    cur_ts_ = slot;
    return;
  }

  if (slot - cur_ts_ >= kBuckets) {
    values_.fill(0);  // more than a window passed, clear fully
    total_ = 0;
  } else {
    for (uint64_t s = cur_ts_ + 1; s <= slot; ++s)
      total_ -= std::exchange(values_[s % kBuckets], 0);
  }
  cur_ts_ = slot;
}

void EgressThrottler::Record(uint64_t bytes, bool out_of_order) {
  if (!enabled())
    return;

  uint64_t now = NowNs();
  socket_.Record(bytes, now);
  if (!out_of_order)
    loop_.Record(bytes, now);
}

void EgressThrottler::Throttle() {
  if (!enabled())
    return;

  const uint64_t reserve = static_cast<uint64_t>(limit_ * kGuaranteedRatio);
  while (true) {
    uint64_t now = NowNs();

    if (socket_.Estimate(now) <= limit_)  // under budget
      break;

    if (loop_.Estimate(now) < reserve)  // not reached guaranteed throughput
      break;

    // Suspend until next possible decrease
    uint64_t wait_ns = socket_.GetRemainingBucketSpan(now);
    util::ThisFiber::SleepFor(chrono::microseconds(wait_ns / 1000 + 1));
  }
}

}  // namespace dfly::detail
