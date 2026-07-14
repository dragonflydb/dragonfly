// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>

namespace dfly::detail {

// EgressThrottler allows to record bytes and throttle accordingly to keep the throughput
// (bytes/second) under a predefined limit by suspending the fiber.
// Differentiates between high and low priority writes. Does not throttle if low priority writes
// did not reach a baseline share - this guarantees stable progress for the operation even if
// high priority writes eat all the limit (will exceed the limit).
// Uses GCRA for the throttling calculations. The limit must be positive; enabling/disabling
// throttling is a caller concern.
class EgressThrottler {
 public:
  explicit EgressThrottler(uint64_t limit_bytes) : limit_{limit_bytes} {
  }

  // Updates the byte/second limit.
  void SetLimit(uint64_t limit_bytes) {
    limit_ = limit_bytes;
  }

  // Record egress and whether it came from a high priority (out of order) write.
  void Record(uint64_t bytes, bool high_prio);

  // Block fiber until egress limit is available again.
  void Throttle() const;

  // Advances the TATs for `bytes` observed at time `now_us`.
  void RecordAt(uint64_t bytes, bool high_prio, uint64_t now_us);

  // Returns 0 if the loop may proceed at `now_us`, otherwise the us timestamp to sleep until.
  uint64_t WakeTime(uint64_t now_us) const;

 private:
  uint64_t limit_;             // bytes/second
  uint64_t total_tat_ = 0;     // theoretical arrival time (us) for all egress
  uint64_t low_prio_tat_ = 0;  // theoretical arrival time (us) for low priority egress only
};

}  // namespace dfly::detail
