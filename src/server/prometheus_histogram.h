// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/inlined_vector.h>

struct hdr_histogram;
namespace dfly {

// Allows recording values and then returning them as cumulative counts, as expected by prometheus,
// separated into buckets. eg create a histogram with usec values to track time taken for commands:
// PrometheusHistogram p{100, 500, 1000, 1000000};
// p.Record(...);
// p.Record(...);
// p.Report() then gives buckets of cumulative counts, sum and total count.
class PrometheusHistogram {
 public:
  struct Output {
    absl::InlinedVector<int64_t, 16> buckets;
    double sum;
    int64_t count;
  };

  PrometheusHistogram(std::initializer_list<double> bucket_boundaries);

  void Record(double value);

  Output Report() const;

 private:
  absl::InlinedVector<double, 16> bucket_boundaries_;
  hdr_histogram* stats_;
  double sum_{0.0};
};

}  // namespace dfly
