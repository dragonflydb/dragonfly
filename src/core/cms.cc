// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/cms.h"

#include <xxhash.h>

#include <algorithm>
#include <cmath>
#include <limits>

namespace dfly {

CMS::CMS(uint32_t width, uint32_t depth, PMR_NS::memory_resource* mr)
    : width_(width), depth_(depth), count_(0), counters_(mr) {
  // Initialize counter matrix with zeros
  counters_.resize(static_cast<size_t>(width_) * depth_, 0);
}

CMS* CMS::FromErrorRate(double error, double probability, PMR_NS::memory_resource* mr) {
  // Width = ceil(e / error) where e is Euler's number
  // Depth = ceil(ln(1 / probability))
  uint32_t width = static_cast<uint32_t>(std::ceil(std::exp(1.0) / error));
  uint32_t depth = static_cast<uint32_t>(std::ceil(std::log(1.0 / probability)));

  // Ensure minimum dimensions
  width = std::max(width, 1u);
  depth = std::max(depth, 1u);

  return new CMS(width, depth, mr);
}

uint32_t CMS::Hash(std::string_view item, uint32_t seed) const {
  uint64_t hash = XXH3_64bits_withSeed(item.data(), item.size(), seed);
  return static_cast<uint32_t>(hash % width_);
}

int64_t CMS::IncrBy(std::string_view item, int64_t increment) {
  count_ += increment;

  int64_t min_count = std::numeric_limits<int64_t>::max();

  for (uint32_t i = 0; i < depth_; ++i) {
    uint32_t index = Hash(item, i);
    size_t offset = static_cast<size_t>(i) * width_ + index;
    counters_[offset] += increment;
    min_count = std::min(min_count, counters_[offset]);
  }

  return min_count;
}

int64_t CMS::Query(std::string_view item) const {
  int64_t min_count = std::numeric_limits<int64_t>::max();

  for (uint32_t i = 0; i < depth_; ++i) {
    uint32_t index = Hash(item, i);
    size_t offset = static_cast<size_t>(i) * width_ + index;
    min_count = std::min(min_count, counters_[offset]);
  }

  return min_count;
}

bool CMS::Merge(const CMS* other, int64_t weight) {
  // Dimensions must match
  if (width_ != other->width_ || depth_ != other->depth_) {
    return false;
  }

  // Add weighted counters from other CMS
  for (size_t i = 0; i < counters_.size(); ++i) {
    counters_[i] += other->counters_[i] * weight;
  }

  count_ += other->count_ * weight;
  return true;
}

void CMS::MergeCounters(const std::vector<int64_t>& counters, int64_t src_count, int64_t weight) {
  // Caller must ensure counters.size() == width_ * depth_
  for (size_t i = 0; i < counters_.size() && i < counters.size(); ++i) {
    counters_[i] += counters[i] * weight;
  }
  count_ += src_count * weight;
}

void CMS::Reset() {
  std::fill(counters_.begin(), counters_.end(), 0);
  count_ = 0;
}

size_t CMS::MallocUsed() const {
  // Approximate memory usage: counters + object overhead
  return counters_.capacity() * sizeof(int64_t) + sizeof(CMS);
}

}  // namespace dfly
