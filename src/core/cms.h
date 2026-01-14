// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>
#include <string_view>
#include <vector>

#include "base/pmr/memory_resource.h"

namespace dfly {

/// Redis-compatible Count-Min Sketch implementation.
/// This is a probabilistic data structure for frequency estimation.
/// It allows counting the frequency of items in a stream with sub-linear space.
class CMS {
 public:
  // Construct CMS with explicit dimensions
  // width - number of counters per row (hash buckets)
  // depth - number of rows (hash functions)
  CMS(uint32_t width, uint32_t depth, PMR_NS::memory_resource* mr);

  // Construct CMS from error rate and probability parameters
  // error - the acceptable error rate (0 < error < 1)
  // probability - the acceptable probability of exceeding error (0 < probability < 1)
  // Width is calculated as ceil(e / error)
  // Depth is calculated as ceil(ln(1 / probability))
  static CMS* FromErrorRate(double error, double probability, PMR_NS::memory_resource* mr);

  CMS(const CMS&) = delete;
  CMS& operator=(const CMS&) = delete;

  ~CMS() = default;

  // Increment the count for an item by the given value
  // Returns the new estimated count after increment
  int64_t IncrBy(std::string_view item, int64_t increment);

  // Query the estimated count for an item
  int64_t Query(std::string_view item) const;

  // Merge another CMS into this one with optional weight
  // The source CMS must have the same dimensions
  // Returns true on success, false if dimensions don't match
  bool Merge(const CMS* other, int64_t weight = 1);

  // Getters for dimensions
  uint32_t Width() const {
    return width_;
  }
  uint32_t Depth() const {
    return depth_;
  }

  // Total count of all increments
  int64_t Count() const {
    return count_;
  }

  // Memory usage estimation
  size_t MallocUsed() const;

  // Get read-only access to counters (for serialization/cross-shard merge)
  const std::vector<int64_t, PMR_NS::polymorphic_allocator<int64_t>>& Counters() const {
    return counters_;
  }

  // Merge from raw counter vector (for cross-shard merge)
  // counters must have size = width * depth
  // src_count is the total count from the source CMS
  void MergeCounters(const std::vector<int64_t>& counters, int64_t src_count, int64_t weight = 1);

  // Reset all counters to zero (used before merge to replace instead of add)
  void Reset();

 private:
  // Hash function using XXH3 with seed
  uint32_t Hash(std::string_view item, uint32_t seed) const;

  uint32_t width_;
  uint32_t depth_;
  int64_t count_;  // Total of all increments

  // Counter matrix: depth rows x width columns
  // Using a flat vector for better cache locality
  std::vector<int64_t, PMR_NS::polymorphic_allocator<int64_t>> counters_;
};

}  // namespace dfly
