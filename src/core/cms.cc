// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/cms.h"

#include <xxhash.h>

#include <algorithm>
#include <cmath>
#include <limits>

namespace dfly {
namespace {

uint32_t Offset(uint64_t h1, uint64_t h2, uint32_t row, uint32_t width) {
  uint32_t idx = static_cast<uint32_t>((h1 + (row * h2)) % width);
  return row * width + idx;
}

}  // namespace

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

CMS::CMS(ErrorRateTag /*tag*/, double error, double probability, PMR_NS::memory_resource* mr)
    : CMS(static_cast<uint32_t>(std::ceil(M_E / error)),
          static_cast<uint32_t>(std::ceil(std::log(1.0 / probability))), mr) {
}

int64_t CMS::IncrBy(std::string_view item, int64_t increment) {
  count_ += increment;

  int64_t min_count = std::numeric_limits<int64_t>::max();
  XXH128_hash_t hash = XXH3_128bits(item.data(), item.size());
  uint64_t h1 = hash.low64;
  uint64_t h2 = hash.high64;

  for (uint32_t row = 0; row < depth_; ++row) {
    uint32_t offset = Offset(h1, h2, row, width_);
    counters_[offset] += increment;
    min_count = std::min(min_count, counters_[offset]);
  }

  return min_count;
}

int64_t CMS::Query(std::string_view item) const {
  XXH128_hash_t hash = XXH3_128bits(item.data(), item.size());
  uint64_t h1 = hash.low64;
  uint64_t h2 = hash.high64;

  int64_t min_count = std::numeric_limits<int64_t>::max();
  for (uint32_t row = 0; row < depth_; ++row) {
    uint32_t offset = Offset(h1, h2, row, width_);
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

void CMS::Load(int64_t total_count, const int64_t* data, size_t count) {
  count_ = total_count;
  for (size_t i = 0; i < count; ++i) {
    counters_[i] = static_cast<int64_t>(data[i]);
  }
}

size_t CMS::MallocUsed() const {
  return counters_.size() * sizeof(int64_t);
}

}  // namespace dfly
