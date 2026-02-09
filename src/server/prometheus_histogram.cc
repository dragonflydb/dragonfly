// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/prometheus_histogram.h"

#include <glog/logging.h>
#include <hdr/hdr_histogram.h>

namespace {
// TODO this is kind of tied to time units, maybe it should be generic, or allow the user to
//   override custom units.
constexpr int64_t kHistogramMinValue = 1;         // Minimum value in usec
constexpr int64_t kHistogramMaxValue = 20000000;  // Maximum value in usec (20s)
constexpr int32_t kHistogramPrecision = 2;
}  // namespace

namespace dfly {

PrometheusHistogram::PrometheusHistogram(const std::initializer_list<double> bucket_boundaries)
    : bucket_boundaries_(bucket_boundaries) {
  hdr_histogram* hist = nullptr;
  const int init_result =
      hdr_init(kHistogramMinValue, kHistogramMaxValue, kHistogramPrecision, &hist);
  CHECK_EQ(init_result, 0) << "failed to initialize histogram";
  stats_ = hist;
}

void PrometheusHistogram::Record(double value) {
  hdr_record_value(stats_, value);
  sum_ += value;
}

PrometheusHistogram::Output PrometheusHistogram::Report() const {
  hdr_iter iter;
  hdr_iter_recorded_init(&iter, stats_);

  size_t index = 0;
  const size_t num_buckets = bucket_boundaries_.size();
  absl::InlinedVector<int64_t, 16> count(num_buckets, 0);

  while (hdr_iter_next(&iter)) {
    while (index < num_buckets && iter.value > bucket_boundaries_[index])
      ++index;
    if (index < num_buckets)
      count[index] = iter.cumulative_count;
  }

  // backfill any zero slots with cumulative count
  for (size_t i = 1; i < num_buckets; ++i)
    count[i] = std::max(count[i], count[i - 1]);

  return Output{
      .buckets = std::move(count),
      .sum = sum_,
      .count = stats_->total_count,
  };
}

}  // namespace dfly
