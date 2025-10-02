// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/count_min_sketch.h"

#include <benchmark/benchmark.h>
#include <gmock/gmock-matchers.h>

namespace {

std::vector<uint16_t> CollectFrequencies(dfly::MultiSketch::Decay decay) {
  constexpr auto rollover_ms = 100;
  constexpr auto eps = 0.0001;
  constexpr auto delta = 0.0001;
  auto ms = dfly::MultiSketch{rollover_ms, eps, delta, decay};
  ms.SetRolloverCheckLimit(1);

  constexpr auto elem = 999;

  for (auto i = 0; i < 52345; ++i) {
    ms.Update(elem);
  }

  std::vector<uint16_t> freqs;

  freqs.push_back(ms.EstimateFrequency(elem));
  for (auto i = 0; i < 3; ++i) {
    usleep(rollover_ms * 1000);
    ms.Update(1);
    freqs.push_back(ms.EstimateFrequency(elem));
  }
  return freqs;
}

constexpr auto EPS = 0.0001;
constexpr auto DELTA = 0.0001;

}  // namespace

TEST(CountMinSketch, Estimate) {
  auto cms = dfly::CountMinSketch{EPS, DELTA};

  constexpr auto actual_freq = 52345;
  const auto elems = {999, 123456, 2785, 0, 96, 221};
  for (auto i = 0; i < actual_freq; ++i) {
    for (const auto elem : elems) {
      cms.Update(elem);
    }
  }

  constexpr auto err_margin_per_elem = actual_freq * EPS;
  auto errors = 0;
  for (const auto elem : elems) {
    if (cms.EstimateFrequency(elem) - actual_freq > err_margin_per_elem) {
      errors++;
    }
  }

  // Total errors should be less than the probability of overshoot per element * elements
  ASSERT_LE(errors, elems.size() * DELTA);
}

TEST(CountMinSketch, EstimateDoesNotOverflow) {
  auto cms = dfly::CountMinSketch{};
  for (uint32_t i = 0; i < 65535 + 100; ++i) {
    cms.Update(42);
  }
  ASSERT_EQ(cms.EstimateFrequency(42), std::numeric_limits<dfly::CountMinSketch::SizeT>::max());
}

TEST(MultiSketch, Estimate) {
  auto ms = dfly::MultiSketch{1000, EPS, DELTA};

  constexpr auto actual_freq = 52345;
  const auto elems = {999, 123456, 2785, 0, 96, 221};
  for (auto i = 0; i < actual_freq; ++i) {
    for (const auto elem : elems) {
      ms.Update(elem);
    }
  }

  constexpr auto err_margin_per_elem = actual_freq * EPS;
  auto errors = 0;
  for (const auto elem : elems) {
    if (ms.EstimateFrequency(elem) - actual_freq > err_margin_per_elem) {
      ++errors;
    }
  }
  ASSERT_LE(errors, DELTA * elems.size());
}

TEST(MultiSketch, RollOverDiscardsOldEstimates) {
  constexpr auto rollover_ms = 100;
  auto ms = dfly::MultiSketch{rollover_ms, EPS, DELTA, dfly::MultiSketch::Decay::SlidingWindow};
  ms.SetRolloverCheckLimit(1);

  constexpr auto actual_freq = 52345;
  const auto elems = {999, 123456, 2785, 0, 96, 221};
  for (auto i = 0; i < actual_freq; ++i) {
    for (const auto elem : elems) {
      ms.Update(elem);
    }
  }

  constexpr auto rollover_count = 3;
  constexpr auto sleep_usec = rollover_ms * 1000;

  // Force rollover
  for (auto i = 0; i < rollover_count; ++i) {
    usleep(sleep_usec);
    ms.Update(1);
  }

  for (const auto elem : elems) {
    ASSERT_EQ(ms.EstimateFrequency(elem), 0) << "item still has non zero count" << elem;
  }

  ASSERT_EQ(ms.EstimateFrequency(1), rollover_count);
}

TEST(MultiSketch, Decay) {
  using dfly::MultiSketch;
  for (const auto decay : {MultiSketch::Decay::Exponential, MultiSketch::Decay::Linear}) {
    auto freqs = CollectFrequencies(decay);
    // counts keep decreasing
    ASSERT_TRUE(std::is_sorted(freqs.begin(), freqs.end(), std::greater<>{}));
  }

  // For sliding window, estimates either remain the same or fall off and become 0
  const auto freqs = CollectFrequencies(MultiSketch::Decay::SlidingWindow);
  const auto f = freqs[0];
  ASSERT_THAT(freqs, testing::Each(testing::AnyOf(0, f)));
}

TEST(CountMinSketch, EstimateOverLargeRange) {
  // track each size from 1 KiB to 1 GiB in steps of 150 KiB
  constexpr uint64_t start = 1024;
  constexpr uint64_t end = 1024 * 1024 * 1024;
  constexpr uint64_t step_size = 1024 * 150;

  auto cms = dfly::CountMinSketch{EPS, DELTA};

  uint64_t num_items_in_sketch = 0;
  for (uint64_t i = start; i <= end; i += step_size, ++num_items_in_sketch) {
    cms.Update(i, num_items_in_sketch);
  }

  const auto max_err = num_items_in_sketch * EPS;
  auto errors = 0;
  for (uint64_t i = start, value = 0; i <= end; i += step_size, value += 1) {
    if (cms.EstimateFrequency(i) - value > max_err) {
      ++errors;
    }
  }
  ASSERT_LE(errors, num_items_in_sketch * DELTA);
}

void BM_CMSEstimate(benchmark::State& state) {
  auto cms = dfly::CountMinSketch{};
  const uint64_t start = 1;
  const uint64_t end = state.range(0);
  const uint64_t step_size = 10;
  for (auto _ : state) {
    for (auto i = start; i < end; i += step_size) {
      cms.Update(i);
    }

    for (auto i = start; i < end; i += step_size) {
      benchmark::DoNotOptimize(cms.EstimateFrequency(i));
    }
  }
}

BENCHMARK(BM_CMSEstimate)->Arg(UINT32_MAX / 100);
