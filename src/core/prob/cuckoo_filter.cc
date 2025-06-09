// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/prob/cuckoo_filter.h"

#include <xxhash.h>

#include <cmath>
#include <cstdint>

#include "absl/numeric/bits.h"
#include "glog/logging.h"

namespace dfly::prob {

namespace {

bool IsPowerOfTwo(uint64_t n) {
  return absl::has_single_bit(n);
}

uint64_t GetNextPowerOfTwo(uint64_t n) {
  return absl::bit_ceil(n);
}

uint8_t GetFingerprint(uint64_t hash) {
  return static_cast<uint8_t>(hash % 255 + 1);
}

uint64_t AltIndex(uint8_t fp, uint64_t index) {
  return index ^ (static_cast<uint64_t>(fp) * 0x5bd1e995);
}

}  // anonymous namespace

std::optional<CuckooFilter> CuckooFilter::Init(const CuckooReserveParams& params,
                                               std::pmr::memory_resource* mr) {
  CuckooFilter filter{params, mr};
  if (!filter.AddNewSubFilter()) {
    return std::nullopt;
  }
  return filter;
}

CuckooFilter::Hash CuckooFilter::GetHash(std::string_view item) {
  return XXH3_64bits_withSeed(item.data(), item.size(), 0xc6a4a7935bd1e995ULL);
}

CuckooFilter::CuckooFilter(const CuckooReserveParams& params, std::pmr::memory_resource* mr)
    : bucket_size_(params.bucket_size),
      max_iterations_(params.max_iterations),
      expansion_(GetNextPowerOfTwo(params.expansion)),
      filters_(mr),
      mr_(mr) {
  if (bucket_size_) {
    num_buckets_ = GetNextPowerOfTwo(params.capacity / bucket_size_);
    if (!num_buckets_) {
      num_buckets_ = 1;
    }
  } else {
    num_buckets_ = 1;
  }

  DCHECK(IsPowerOfTwo(num_buckets_));
}

bool CuckooFilter::AddNewSubFilter() {
  static constexpr uint64_t kCfMaxNumBuckets = (1ULL << 56) - 1;
  DCHECK(filters_.size() < std::numeric_limits<uint16_t>::max());

  const uint64_t growth = std::pow(expansion_, filters_.size());
  if (growth > kCfMaxNumBuckets / num_buckets_) {
    return false;
  }

  const uint64_t new_buckets_count = num_buckets_ * growth;
  if (new_buckets_count > std::numeric_limits<size_t>::max() / bucket_size_) {
    return false;
  }

  filters_.emplace_back(new_buckets_count * bucket_size_, mr_);
  return true;
}

CuckooFilter::LookupParams CuckooFilter::MakeLookupParams(uint64_t hash, uint64_t num_buckets) {
  const uint8_t fp = GetFingerprint(hash);
  const uint64_t h1 = hash % num_buckets;
  return {fp, h1, AltIndex(fp, h1) % num_buckets};
}

bool CuckooFilter::Insert(Hash hash) {
  LookupParams p = MakeLookupParams(hash, num_buckets_);

  for (int i = filters_.size() - 1; i >= 0; --i) {
    SubFilter& f = filters_[i];
    for (uint64_t idx : GetIndexesInSubFilter(f, p)) {
      for (uint8_t b = 0; b < bucket_size_; ++b) {
        size_t offset = idx * bucket_size_ + b;
        if (f[offset] == 0) {
          f[offset] = p.fp;
          ++num_items_;
          return true;
        }
      }
    }
  }

  if (KOInsert(p, &filters_.back())) {
    ++num_items_;
    return true;
  }

  if (expansion_ == 0) {
    LOG(WARNING) << "Cuckoo filter is full, unable to allocate new subfilter due to expansion = 0";
    return false;
  }

  if (!AddNewSubFilter()) {
    return false;
  }

  return Insert(hash);
}

bool CuckooFilter::KOInsert(const LookupParams& p, SubFilter* sub_filter) {
  const uint64_t num_buckets = GetNumBucketsInSubFilter(*sub_filter);
  uint64_t idx = p.h1 % num_buckets;
  Entry fp = p.fp;

  uint16_t victim_idx = 0;
  for (uint16_t i = 0; i < max_iterations_; ++i) {
    const size_t base = idx * bucket_size_;
    const size_t victim_offset = base + victim_idx;

    std::swap((*sub_filter)[victim_offset], fp);
    idx = AltIndex(fp, idx) % num_buckets;

    const uint16_t new_base = idx * bucket_size_;
    for (uint8_t b = 0; b < bucket_size_; ++b) {
      const size_t offset = new_base + b;
      if ((*sub_filter)[offset] == 0) {
        (*sub_filter)[offset] = fp;
        return true;
      }
    }

    victim_idx = (victim_idx + 1) % bucket_size_;
  }

  // Roll back
  for (uint16_t i = 0; i < max_iterations_; ++i) {
    victim_idx = (victim_idx + bucket_size_ - 1) % bucket_size_;
    idx = AltIndex(fp, idx) % num_buckets;

    const size_t base = idx * bucket_size_;
    const size_t victim_offset = base + victim_idx;

    std::swap((*sub_filter)[victim_offset], fp);
  }

  return false;
}

bool CuckooFilter::Exists(std::string_view item) const {
  return Exists(GetHash(item));
}

bool CuckooFilter::Exists(Hash hash) const {
  LookupParams p = MakeLookupParams(hash, num_buckets_);

  for (const auto& f : filters_) {
    for (uint64_t idx : GetIndexesInSubFilter(f, p)) {
      for (uint8_t b = 0; b < bucket_size_; ++b) {
        size_t offset = idx * bucket_size_ + b;
        if (f[offset] == p.fp) {
          return true;
        }
      }
    }
  }

  return false;
}

bool CuckooFilter::Delete(std::string_view item) {
  const auto hash = GetHash(item);
  LookupParams p = MakeLookupParams(hash, num_buckets_);

  for (int i = filters_.size() - 1; i >= 0; --i) {
    SubFilter& f = filters_[i];
    for (uint64_t idx : GetIndexesInSubFilter(f, p)) {
      const size_t base = idx * bucket_size_;
      for (uint8_t b = 0; b < bucket_size_; ++b) {
        const size_t offset = base + b;
        if (f[offset] == p.fp) {
          f[offset] = 0;
          --num_items_;
          ++num_deletes_;
          return true;
        }
      }
    }
  }

  return false;
}

uint64_t CuckooFilter::Count(std::string_view item) const {
  const auto hash = GetHash(item);
  LookupParams p = MakeLookupParams(hash, num_buckets_);
  uint64_t count = 0;

  for (const SubFilter& f : filters_) {
    for (uint64_t idx : GetIndexesInSubFilter(f, p)) {
      const size_t base = idx * bucket_size_;
      for (uint8_t b = 0; b < bucket_size_; ++b) {
        if (f[base + b] == p.fp) {
          ++count;
        }
      }
    }
  }

  return count;
}

}  // namespace dfly::prob
