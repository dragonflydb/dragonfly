// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <array>
#include <cstdint>

namespace dfly::detail {

// SlidingTracker estimates byte throughput over a fixed time window using a ring
// buffer of kBuckets slices. Callers Record byte increases manually. Precision is 1/kBuckets.
class SlidingTracker {
 public:
  static constexpr unsigned kBuckets = 20;

  explicit SlidingTracker(uint64_t window_size) : bucket_size_{window_size / kBuckets} {
  }

  void Record(uint64_t bytes, uint64_t now) {
    Advance(now / bucket_size_);
    values_[cur_ts_ % kBuckets] += bytes;
    total_ += bytes;
  }

  uint64_t Estimate(uint64_t now) {
    Advance(now / bucket_size_);
    return total_;
  }

  // How long till next bucket from now - eariliest point when Estimate() can decrease
  uint64_t GetRemainingBucketSpan(uint64_t now) const {
    return (now / bucket_size_ + 1) * bucket_size_ - now;
  }

 private:
  // Advance to current time and clear stale slices
  void Advance(uint64_t slot);

  const uint64_t bucket_size_;
  uint64_t total_ = 0;
  uint64_t cur_ts_ = 0;
  std::array<uint64_t, kBuckets> values_{};
};

// EgressThrottler allows to record bytes and throttle accordingly to keep the throughput
// (bytes/second) under a predefined limit by suspending the fiber.
// Differentiates between regular and out of order writes. Does not throttle if regular writes
// did not reach a baseline limit - this guarantees stable progress for the operation even if
// out of order load eats all the limit (will exceed the limit)
class EgressThrottler {
 public:
  static constexpr uint64_t kWindowNs = 1'000'000'000ULL;  // 1 second

  static constexpr double kGuaranteedRatio = 0.1;

  // limit_bytes: max bytes/second for this snapshot. 0 disables throttling.
  // window_ns: sliding window length in nanoseconds (defaults to 1 second).
  explicit EgressThrottler(uint64_t limit_bytes, uint64_t window_ns = kWindowNs)
      : limit_{limit_bytes}, socket_{window_ns}, loop_{window_ns} {
  }

  bool enabled() const {
    return limit_ > 0;
  }

  void SetLimit(uint64_t limit_bytes) {
    limit_ = limit_bytes;
  }

  // Record egress and if it occured from an out of order write
  void Record(uint64_t bytes, bool out_of_order);

  // Block fiber until egress limit is available again
  void Throttle();

 private:
  uint64_t limit_;
  SlidingTracker socket_;
  SlidingTracker loop_;
};

}  // namespace dfly::detail
