// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/time/clock.h>

#include <cstdint>

#include "util/sliding_counter.h"

namespace dfly::detail {

// Sliding window length and resolution for egress accounting. The window is split into
// kEgressBuckets slices, so throughput is estimated over the last second with 1/kEgressBuckets
// precision.
inline constexpr unsigned kEgressBuckets = 20;
inline constexpr uint64_t kEgressWindowNs = 1'000'000'000ULL;                  // 1 second
inline constexpr uint64_t kEgressBucketNs = kEgressWindowNs / kEgressBuckets;  // 50ms

// Use nanosecond precision clock with GetCurrentTimeNanos. rdtsc (CycleClock) is too unstable to
// count in real-time second units, cached values can to be imprecise
struct NanoTickClock {
  uint64_t operator()() const {
    return static_cast<uint64_t>(absl::GetCurrentTimeNanos()) / kEgressBucketNs;
  }
};

using ByteTracker = util::SlidingCounter<kEgressBuckets, uint64_t, NanoTickClock>;

// EgressThrottler allows to record bytes and throttle accordingly to keep the throughput
// (bytes/second) under a predefined limit by suspending the fiber.
// Differentiates between regular and out of order writes. Does not throttle if regular writes
// did not reach a baseline limit - this guarantees stable progress for the operation even if
// out of order load eats all the limit (will exceed the limit).
class EgressThrottler {
 public:
  static constexpr double kGuaranteedRatio = 0.1;

  // limit_bytes: max bytes/second for this snapshot. 0 disables throttling.
  explicit EgressThrottler(uint64_t limit_bytes = 0) : limit_{limit_bytes} {
  }

  bool enabled() const {
    return limit_ > 0;
  }

  // Updates the byte/second limit. Cheap; call it to pick up runtime config changes.
  // 0 disables throttling. Must be called from the snapshot's own thread.
  void SetLimit(uint64_t limit_bytes) {
    limit_ = limit_bytes;
  }

  // Record egress and whether it occured from an out of order write.
  void Record(uint64_t bytes, bool out_of_order);

  // Block fiber until egress limit is available again.
  void Throttle();

 private:
  uint64_t limit_;
  ByteTracker socket_;
  ByteTracker loop_;
};

}  // namespace dfly::detail
