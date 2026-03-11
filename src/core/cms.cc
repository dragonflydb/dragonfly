// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/cms.h"

#include <xxhash.h>

#include <algorithm>
#include <cmath>

#include "base/logging.h"

namespace dfly {
namespace {

uint32_t Offset(uint64_t h1, uint64_t h2, uint32_t row, uint32_t width) {
  uint32_t idx = static_cast<uint32_t>((h1 + (row * h2)) % width);
  return row * width + idx;
}

}  // namespace

CMS::CMS(uint32_t width, uint32_t depth, PMR_NS::memory_resource* mr)
    : width_(width), depth_(depth), mr_(mr) {
  DCHECK(mr_);
  DCHECK(width_ > 0 && depth_ > 0);
  size_t len = size();
  counters_ = static_cast<int64_t*>(mr_->allocate(len * sizeof(int64_t), alignof(int64_t)));
  std::fill_n(counters_, len, 0);
}

CMS::CMS(ErrorRateTag /*tag*/, double error, double probability, PMR_NS::memory_resource* mr)
    : CMS(static_cast<uint32_t>(std::ceil(M_E / error)),
          static_cast<uint32_t>(std::ceil(std::log(1.0 / probability))), mr) {
}

CMS::~CMS() {
  if (counters_) {
    mr_->deallocate(counters_, size() * sizeof(int64_t), alignof(int64_t));
  }
}

CMS::CMS(CMS&& other) noexcept
    : width_(other.width_),
      depth_(other.depth_),
      mr_(other.mr_),
      count_(other.count_),
      counters_(other.counters_) {
  other.width_ = 0;
  other.depth_ = 0;
  other.count_ = 0;
  other.counters_ = nullptr;
}

CMS& CMS::operator=(CMS&& other) noexcept {
  if (this != &other) {
    if (counters_) {
      mr_->deallocate(counters_, size() * sizeof(int64_t), alignof(int64_t));
    }
    width_ = other.width_;
    depth_ = other.depth_;
    mr_ = other.mr_;
    count_ = other.count_;
    counters_ = other.counters_;
    other.width_ = 0;
    other.depth_ = 0;
    other.count_ = 0;
    other.counters_ = nullptr;
  }
  return *this;
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

  for (size_t i = 0; i < size(); ++i) {
    counters_[i] += other.counters_[i] * weight;
  }

  count_ += other.count_ * weight;
  return true;
}

}  // namespace dfly
