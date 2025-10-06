// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <array>
#include <cstdint>
#include <vector>

namespace dfly {

/// The CountMinSketch and the MultiSketch together are intended to record values which naturally
/// reduce over time, but for which explicitly recording decrements is not possible or easy.
/// An example is short-lived, large memory allocations. The allocation site can record the size of
/// the block allocated, and the count reduces over time, eventually going down to zero.

// Keeps count of items added with a small probability of overestimating counts due to hash
// collisions. Counts are stored in a table where each row stores counts for each item, and the
// minimum count across all rows is returned for an item when requested.
class CountMinSketch {
 public:
  using SizeT = uint16_t;

  // epsilon is the maximum deviation from actual frequency allowed per element times the sum of all
  // frequencies:
  //    f_actual <= f_estimated <= f_actual + epsilon * N
  //      where N is the sum of all frequencies
  // delta is the probability that f_estimated overshoots the epsilon threshold for a single
  // estimate, aka failure probability which means all bets are off as to what estimate is returned.

  // With default values, the dimension of the counter table is 27182 x 9, and the size is
  // around 490 KiBs.
  explicit CountMinSketch(double epsilon = 0.0001, double delta = 0.0001);

  // Increases the count associated with a key, a potentially slow operation as several hashes are
  // calculated
  void Update(uint64_t key, SizeT incr = 1);

  // Estimated count for the key with a small probability for overshooting the estimate.
  SizeT EstimateFrequency(uint64_t key) const;

  // Loses all existing counts by resetting them to zero.
  void Reset();

  CountMinSketch(const CountMinSketch& other) = delete;
  CountMinSketch& operator=(const CountMinSketch& other) = delete;

  CountMinSketch(CountMinSketch&& other) noexcept = default;
  CountMinSketch& operator=(CountMinSketch&& other) noexcept = default;

 private:
  uint64_t Hash(uint64_t key, uint64_t i) const;

  std::vector<std::vector<SizeT>> counters_;
  uint64_t width_;
  uint64_t depth_;
};

// Maintains a list of three sketches with timestamps. Updates are made to the current sketch.
// Once the oldest sketch is older than a fixed limit, it is discarded and becomes the current
// sketch. Estimates are the sum across all sketches. The counts returned by the sketches "decay" to
// lower values as the sketches become older.
class MultiSketch {
  struct SketchWithTimestamp {
    CountMinSketch sketch_;
    uint64_t start_time_{0};
    uint64_t end_time_{0};
  };

 public:
  // The decay model decides how fast values in sketches reduce as time passes.
  // Exponential: larger values reduce faster
  // Linear: all values decrease at a fixed rate
  // SlidingWindow: values do not decrease until the sketch containing them resets
  enum class Decay : uint8_t {
    Exponential,
    Linear,
    SlidingWindow,
  };

  explicit MultiSketch(uint64_t rollover_ms = 1000, double epsilon = 0.0001, double delta = 0.0001,
                       Decay decay = Decay::Linear);

  MultiSketch(const MultiSketch& other) = delete;
  MultiSketch& operator=(const MultiSketch& other) = delete;

  MultiSketch(MultiSketch&& other) noexcept = default;
  MultiSketch& operator=(MultiSketch&& other) noexcept = default;

  // Updates the current sketch, which is associated with the latest timestamp. Can cause the oldest
  // sketch to be reset as a side effect if the oldest sketch is older than rollover_ms.
  void Update(uint64_t key, CountMinSketch::SizeT incr = 1);

  // Returns estimate by summing estimates from all internal sketches.
  CountMinSketch::SizeT EstimateFrequency(uint64_t key) const;

  // For unit tests, allow setting a smaller limit
  void SetRolloverCheckLimit(uint64_t rollover_check_limit) {
    rollover_check_every_ = rollover_check_limit;
  }

 private:
  void MaybeRolloverCurrentSketch();

  std::array<SketchWithTimestamp, 3> sketches_;
  uint64_t rollover_ms_;
  uint64_t current_sketch_;

  // Do a rollover check every N calls to avoid expensive GetTime calls
  uint64_t rollover_check_every_{512};
  uint64_t rollover_check_{0};
  Decay decay_t_;
};

}  // namespace dfly
