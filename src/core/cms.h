// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>
#include <string_view>
#include <vector>

#include "base/pmr/memory_resource.h"

namespace dfly {

/// Count-Min Sketch implementation compatible with Redis CMS commands.
class CMS {
 public:
  // Create a CMS with given width and depth dimensions.
  // width: number of counters per row
  // depth: number of rows (hash functions)
  CMS(uint32_t width, uint32_t depth, PMR_NS::memory_resource* mr = nullptr);

  CMS(const CMS&) = delete;
  CMS& operator=(const CMS&) = delete;

  CMS(CMS&& other) noexcept;
  CMS& operator=(CMS&& other) noexcept;

  ~CMS() = default;

  // Create a CMS from error and probability parameters.
  // error: the acceptable overestimate error rate (0 < error < 1)
  // probability: the probability of exceeding the error rate (0 < probability < 1)
  // width = ceil(e / error), depth = ceil(ln(1 / probability))
  static CMS CreateByProb(double error, double probability, PMR_NS::memory_resource* mr = nullptr);

  // Increment the count for an item by the given value.
  // Returns the new estimated count for the item.
  int64_t IncrBy(std::string_view item, int64_t increment);

  // Query the estimated count for an item.
  [[nodiscard]] int64_t Query(std::string_view item) const;

  // Merge another CMS into this one with the given weight.
  // The other CMS must have the same dimensions.
  // Returns false if dimensions don't match.
  bool MergeFrom(const CMS& other, int64_t weight = 1);

  // Accessors for CMS properties
  [[nodiscard]] uint32_t Width() const {
    return width_;
  }

  [[nodiscard]] uint32_t Depth() const {
    return depth_;
  }

  // Total count of all increments
  [[nodiscard]] int64_t Count() const {
    return count_;
  }

  // Memory usage in bytes
  [[nodiscard]] size_t MallocUsed() const;

  // For serialization - returns the raw counter data
  [[nodiscard]] size_t CounterBytes() const {
    return counters_.size() * sizeof(int64_t);
  }

  [[nodiscard]] const int64_t* Data() const {
    return counters_.data();
  }

  // Reset all counters and total count to zero.
  void Reset();

  // For deserialization - set counter data directly
  // The data size must match width_ * depth_ * sizeof(int64_t)
  void SetCounters(const int64_t* data, size_t count, int64_t total_count);

 private:
  [[nodiscard]] uint64_t Hash(std::string_view item, uint32_t row) const;

  uint32_t width_;
  uint32_t depth_;
  int64_t count_ = 0;  // Total count of all IncrBy operations
  std::vector<int64_t, PMR_NS::polymorphic_allocator<int64_t>> counters_;
};

}  // namespace dfly
