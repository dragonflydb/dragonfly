// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/cms.h"

#include <xxhash.h>

#include <algorithm>
#include <cmath>

namespace dfly {

CMS::CMS(uint32_t width, uint32_t depth, PMR_NS::memory_resource* mr)
    : width_(width),
      depth_(depth),
      counters_(static_cast<size_t>(width) * depth, 0, PMR_NS::polymorphic_allocator<int64_t>(mr)) {
}

CMS::CMS(CMS&& other) noexcept
    : width_(other.width_),
      depth_(other.depth_),
      count_(other.count_),
      counters_(std::move(other.counters_)) {
  other.width_ = 0;
  other.depth_ = 0;
  other.count_ = 0;
}

CMS& CMS::operator=(CMS&& other) noexcept {
  if (this != &other) {
    width_ = other.width_;
    depth_ = other.depth_;
    count_ = other.count_;
    counters_ = std::move(other.counters_);
    other.width_ = 0;
    other.depth_ = 0;
    other.count_ = 0;
  }
  return *this;
}

CMS CMS::CreateByProb(double error, double probability, PMR_NS::memory_resource* mr) {
  // width = ceil(e / error) where e is Euler's number
  // depth = ceil(ln(1 / probability))
  auto width = static_cast<uint32_t>(std::ceil(std::exp(1.0) / error));
  auto depth = static_cast<uint32_t>(std::ceil(std::log(1.0 / probability)));

  // Ensure minimum dimensions
  width = std::max(width, 1u);
  depth = std::max(depth, 1u);

  return {width, depth, mr};
}

int64_t CMS::IncrBy(std::string_view item, int64_t increment) {
  count_ += increment;

  int64_t min_count = std::numeric_limits<int64_t>::max();

  for (uint32_t row = 0; row < depth_; ++row) {
    uint64_t idx = Hash(item, row);
    size_t offset = static_cast<size_t>(row) * width_ + idx;
    counters_[offset] += increment;
    min_count = std::min(min_count, counters_[offset]);
  }

  return min_count;
}

int64_t CMS::Query(std::string_view item) const {
  int64_t min_count = std::numeric_limits<int64_t>::max();

  for (uint32_t row = 0; row < depth_; ++row) {
    uint64_t idx = Hash(item, row);
    size_t offset = static_cast<size_t>(row) * width_ + idx;
    min_count = std::min(min_count, counters_[offset]);
  }

  return min_count;
}

bool CMS::MergeFrom(const CMS& other, int64_t weight) {
  if (width_ != other.width_ || depth_ != other.depth_) {
    return false;
  }

  for (size_t i = 0; i < counters_.size(); ++i) {
    counters_[i] += other.counters_[i] * weight;
  }

  count_ += other.count_ * weight;
  return true;
}

void CMS::Reset() {
  std::fill(counters_.begin(), counters_.end(), 0);
  count_ = 0;
}

size_t CMS::MallocUsed() const {
  return sizeof(CMS) + counters_.capacity() * sizeof(int64_t);
}

void CMS::SetCounters(const int64_t* data, size_t count, int64_t total_count) {
  if (count == counters_.size()) {
    std::copy_n(data, count, counters_.begin());
    count_ = total_count;
  }
}

uint64_t CMS::Hash(std::string_view item, uint32_t row) const {
  return XXH3_64bits_withSeed(item.data(), item.size(), row) % width_;
}

}  // namespace dfly
