// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>

namespace dfly::detail {

// EgressThrottler allows to record bytes and throttle accordingly to keep the throughput
// (bytes/second) under a predefined limit by suspending the fiber.
// Differentiates between regular and out of order writes. Does not throttle if regular writes
// did not reach a baseline limit - this guarantees stable progress for the operation even if
// out of order writes eats all the limit (will exceed the limit).
// Uses GCRA for the throttling calculations
class EgressThrottler {
 public:
  // The loop is guaranteed at least limit/kLoopReserveDivisor of egress (reserved share).
  static constexpr uint64_t kLoopReserveDivisor = 10;  // 1/10 = 10%

  // Burst tolerance (GCRA tau): how far ahead of schedule a stream may run. Absorbs chunk
  // granularity and avoids tiny frequent sleeps without affecting the long-run average rate.
  static constexpr uint64_t kBurstToleranceUs = 100'000;  // 100ms

  static constexpr uint64_t kMicrosPerSec = 1'000'000;

  // limit_bytes: max bytes/second for this snapshot. 0 disables throttling.
  explicit EgressThrottler(uint64_t limit_bytes = 0) : limit_{limit_bytes} {
  }

  bool enabled() const {
    return limit_ > 0;
  }

  // Updates the byte/second limit, 0 disables all throttling
  void SetLimit(uint64_t limit_bytes) {
    limit_ = limit_bytes;
  }

  // Record egress and whether it occured from an out of order write.
  void Record(uint64_t bytes, bool out_of_order);

  // Block fiber until egress limit is available again.
  void Throttle() const;

  // Advances the TATs for `bytes` observed at time `now_us`.
  void RecordAt(uint64_t bytes, bool out_of_order, uint64_t now_us);

  // Returns 0 if the loop may proceed at `now_us`, otherwise the us timestamp to sleep until.
  uint64_t WakeTime(uint64_t now_us) const;

 private:
  uint64_t limit_;           // bytes/second, 0 disables throttling
  uint64_t socket_tat_ = 0;  // theoretical arrival time (us) for all egress
  uint64_t loop_tat_ = 0;    // theoretical arrival time (us) for loop egress only
};

}  // namespace dfly::detail
