// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/cuckoo.h"

#include <xxhash.h>

#include <cmath>
#include <numeric>

#include "base/logging.h"

namespace dfly {

CuckooFilter::CuckooFilter(uint64_t capacity, std::pmr::memory_resource* mr,
                           uint8_t slots_per_bucket, uint16_t max_iterations, uint16_t expansion)
    : slots_per_bucket_(slots_per_bucket),
      max_iterations_(max_iterations),
      expansion_(expansion ? NextPowerOfTwo(expansion) : 0),
      mr_(mr),
      filters_(mr) {
  DCHECK(mr);
  DCHECK_GT(slots_per_bucket_, 0);
  num_buckets_ = slots_per_bucket_ ? NextPowerOfTwo(capacity / slots_per_bucket_) : 1;
  if (num_buckets_ == 0)
    num_buckets_ = 1;
  DCHECK(IsPowerOfTwo(num_buckets_));
  AddNewSubFilter();
}

bool CuckooFilter::Insert(uint64_t hash) {
  DCHECK(!filters_.empty());
  const LookupParams p = LookupParamsFromHash(hash);

  for (;;) {
    for (size_t i = filters_.size(); i-- > 0;) {
      SubFilter& sf = filters_[i];
      auto [i1, i2] = BucketIndices(sf, p);
      for (uint64_t idx : {i1, i2}) {
        const size_t base = idx * slots_per_bucket_;
        for (uint8_t s = 0; s < slots_per_bucket_; ++s) {
          if (sf[base + s] == 0) {
            sf[base + s] = p.fp;
            ++num_items_;
            return true;
          }
        }
      }
    }

    if (KOInsert(p, filters_.back())) {
      ++num_items_;
      ++num_ko_inserts_;
      return true;
    }

    if (expansion_ == 0 || !AddNewSubFilter()) {
      return false;
    }
    // Loop: the new SubFilter has empty slots, insert will succeed on next iteration.
  }
}

bool CuckooFilter::InsertUnique(uint64_t hash) {
  if (Exists(hash))
    return false;
  return Insert(hash);
}

bool CuckooFilter::Exists(uint64_t hash) const {
  DCHECK(!filters_.empty());
  const LookupParams p = LookupParamsFromHash(hash);

  for (const SubFilter& sf : filters_) {
    auto [i1, i2] = BucketIndices(sf, p);
    for (uint64_t idx : {i1, i2}) {
      const size_t base = idx * slots_per_bucket_;
      for (uint8_t s = 0; s < slots_per_bucket_; ++s) {
        if (sf[base + s] == p.fp)
          return true;
      }
    }
  }
  return false;
}

bool CuckooFilter::Delete(uint64_t hash) {
  DCHECK(!filters_.empty());
  const LookupParams p = LookupParamsFromHash(hash);

  for (size_t i = filters_.size(); i-- > 0;) {
    SubFilter& sf = filters_[i];
    auto [i1, i2] = BucketIndices(sf, p);
    for (uint64_t idx : {i1, i2}) {
      const size_t base = idx * slots_per_bucket_;
      for (uint8_t s = 0; s < slots_per_bucket_; ++s) {
        if (sf[base + s] == p.fp) {
          sf[base + s] = 0;
          --num_items_;
          ++num_deletes_;
          return true;
        }
      }
    }
  }
  return false;
}

uint64_t CuckooFilter::Hash(std::string_view item) {
  return XXH3_64bits_withSeed(item.data(), item.size(), 0xc6a4a7935bd1e995ULL);
}

size_t CuckooFilter::UsedMemory() const {
  return std::transform_reduce(filters_.begin(), filters_.end(), size_t{0}, std::plus<>{},
                               [](const SubFilter& sf) { return sf.size(); });
}

CuckooFilter::LookupParams CuckooFilter::LookupParamsFromHash(uint64_t hash) const {
  const uint8_t fp = Fingerprint(hash);
  return {fp, hash, AltIndex(fp, hash)};
}

std::pair<uint64_t, uint64_t> CuckooFilter::BucketIndices(const SubFilter& sf,
                                                          const LookupParams& p) const {
  const uint64_t n = NumBuckets(sf);
  return {p.h1 % n, p.h2 % n};
}

uint64_t CuckooFilter::NumBuckets(const SubFilter& sf) const {
  return sf.size() / slots_per_bucket_;
}

bool CuckooFilter::AddNewSubFilter() {
  static constexpr uint64_t kMaxBuckets =
      (1ULL << 56) - 1;  // preserve semantics with SubFilter numBuckets field (56-bit)

  const uint64_t growth = static_cast<uint64_t>(std::pow(expansion_, filters_.size()));

  if (growth > (kMaxBuckets / num_buckets_)) {
    return false;
  }

  const uint64_t bucket_count = num_buckets_ * growth;
  if (bucket_count > (SIZE_MAX / slots_per_bucket_)) {
    return false;
  }

  SubFilter sf(bucket_count * slots_per_bucket_, uint8_t{0}, mr_);
  filters_.push_back(std::move(sf));
  return true;
}

bool CuckooFilter::KOInsert(const LookupParams& p, SubFilter& sf) {
  const uint64_t n = NumBuckets(sf);
  uint64_t idx = p.h1 % n;
  uint8_t fp = p.fp;
  uint8_t victim_slot = 0;

  for (uint16_t i = 0; i < max_iterations_; ++i) {
    std::swap(sf[idx * slots_per_bucket_ + victim_slot], fp);
    idx = AltIndex(fp, idx) % n;

    for (uint8_t s = 0; s < slots_per_bucket_; ++s) {
      if (sf[idx * slots_per_bucket_ + s] == 0) {
        sf[idx * slots_per_bucket_ + s] = fp;
        return true;
      }
    }
    victim_slot = (victim_slot + 1) % slots_per_bucket_;
  }

  // Roll back all swaps to restore the SubFilter to its original state.
  for (uint16_t i = 0; i < max_iterations_; ++i) {
    victim_slot = (victim_slot + slots_per_bucket_ - 1) % slots_per_bucket_;
    idx = AltIndex(fp, idx) % n;
    std::swap(sf[idx * slots_per_bucket_ + victim_slot], fp);
  }

  return false;
}

}  // namespace dfly
