// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/count_min_sketch.h"

#include <absl/time/clock.h>
#include <xxhash.h>

#include <cmath>
#include <iostream>

namespace {

constexpr auto MAX = std::numeric_limits<dfly::CountMinSketch::SizeT>::max();

uint64_t GetCurrentMS() {
  return absl::GetCurrentTimeNanos() / 1000 / 1000;
}

uint64_t ExponentialDecay(uint64_t value, int64_t time_delta) {
  // Value halves every 5000 ms: ln(2) / 5000
  static constexpr double EXP_DECAY_CONST = 0.000138629;

  return value * std::exp(-time_delta * EXP_DECAY_CONST);
}

uint64_t LinearDecay(uint64_t value, int64_t time_delta) {
  // Value decrements by one every 1000 ms
  static constexpr double LIN_DECAY_CONST = 0.001;

  const double decay = time_delta * LIN_DECAY_CONST;
  return value - std::min(static_cast<double>(value), decay);
}

uint64_t ApplyDecay(uint64_t value, int64_t time_delta, dfly::MultiSketch::Decay decay) {
  using dfly::MultiSketch;
  switch (decay) {
    case MultiSketch::Decay::Exponential:
      return ExponentialDecay(value, time_delta);
    case MultiSketch::Decay::Linear:
      return LinearDecay(value, time_delta);
    case MultiSketch::Decay::SlidingWindow:
      return value;
  }
  ABSL_UNREACHABLE();
}

}  // namespace

namespace dfly {

CountMinSketch::CountMinSketch(double epsilon, double delta) {
  width_ = std::exp(1) / epsilon;
  depth_ = std::log(1.0 / delta);
  counters_.reserve(depth_);
  for (uint64_t i = 0; i < depth_; ++i) {
    counters_.emplace_back(width_, 0);
  }
}

void CountMinSketch::Update(uint64_t key, CountMinSketch::SizeT incr) {
  uint64_t i = 0;
  std::for_each(counters_.begin(), counters_.end(), [&](auto& counter) {
    // It is possible to compute just two initial hashes and then use them to derive next i-2
    // hashes, but it results in a lot more collisions and thus much larger overestimates.
    const uint64_t index = Hash(key, i++);
    const SizeT curr = counter[index];
    const SizeT updated = curr + incr;
    counter[index] = updated < curr ? MAX : updated;
  });
}

CountMinSketch::SizeT CountMinSketch::EstimateFrequency(uint64_t key) const {
  uint64_t i = 0;
  SizeT estimate = counters_[i][Hash(key, i)];
  i += 1;
  for (; i < counters_.size(); ++i) {
    estimate = std::min(estimate, counters_[i][Hash(key, i)]);
  }
  return estimate;
}

void CountMinSketch::Reset() {
  for (auto& ctr : counters_) {
    std::fill(ctr.begin(), ctr.end(), 0);
  }
}

uint64_t CountMinSketch::Hash(uint64_t key, uint64_t i) const {
  return XXH3_64bits_withSeed(&key, sizeof(key), i) % width_;
}

MultiSketch::MultiSketch(uint64_t rollover_ms, double epsilon, double delta, Decay decay)
    : rollover_ms_(rollover_ms), current_sketch_(sketches_.size() - 1), decay_t_(decay) {
  const uint64_t now = GetCurrentMS();
  for (uint64_t i = 0; i < sketches_.size(); ++i) {
    sketches_[i] = SketchWithTimestamp{CountMinSketch{epsilon, delta}, now, now};
  }
}

void MultiSketch::Update(uint64_t key, CountMinSketch::SizeT incr) {
  if (++rollover_check_ >= rollover_check_every_) {
    MaybeRolloverCurrentSketch();
    rollover_check_ = 0;
  }
  sketches_[current_sketch_].sketch_.Update(key, incr);
}

CountMinSketch::SizeT MultiSketch::EstimateFrequency(uint64_t key) const {
  CountMinSketch::SizeT estimate = 0;
  const uint64_t now = GetCurrentMS();

  for (const auto& sketch : sketches_) {
    const auto e = sketch.sketch_.EstimateFrequency(key);
    // TODO use average time of sketch to compute delta
    estimate += ApplyDecay(e, now - sketch.start_time_, decay_t_);
  }
  return estimate;
}

void MultiSketch::MaybeRolloverCurrentSketch() {
  const uint64_t now = GetCurrentMS();
  const uint64_t oldest = (current_sketch_ + 1) % sketches_.size();
  if (const uint64_t oldest_ts = sketches_[oldest].start_time_; now - oldest_ts > rollover_ms_) {
    sketches_[oldest].sketch_.Reset();
    sketches_[oldest].start_time_ = now;
    sketches_[current_sketch_].end_time_ = now;
    current_sketch_ = oldest;
  }
}

}  // namespace dfly
