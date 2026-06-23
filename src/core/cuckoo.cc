// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/cuckoo.h"

#include <xxhash.h>

#include <cmath>

#include "absl/numeric/bits.h"
#include "base/logging.h"

namespace dfly {

namespace {

bool IsPowerOfTwo(uint64_t n) {
  return absl::has_single_bit(n);
}

uint64_t NextPowerOfTwo(uint64_t n) {
  return absl::bit_ceil(n);
}

// Result is in [1, 255] — 0 is reserved as "empty slot".
uint8_t Fingerprint(uint64_t hash) {
  return static_cast<uint8_t>(hash % 255 + 1);
}

// 0x5bd1e995 is the MurmurHash2 mixing constant (Austin Appleby),
// chosen for good bit-avalanche properties.
// AltIndex symmetry requires num_buckets to be a power of two. Power-of-2 modulo
// is a bitmask (x % N == x & (N-1)), and bitmasks commute with XOR:
//   (a XOR b) & mask == (a & mask) XOR (b & mask)
// This means AltIndex(fp, h1 & mask) & mask == AltIndex(fp, h1) & mask, so
//   AltIndex(fp, AltIndex(fp, i) % N) % N == i % N  holds.
// With arbitrary N, modulo is not a bitmask and the identity breaks, corrupting
// KO-insert rollback and deletions.
// Requirement from: Fan et al., "Cuckoo Filter: Practically Better Than Bloom" (2014).
uint64_t AltIndex(uint8_t fp, uint64_t index) {
  return index ^ (static_cast<uint64_t>(fp) * 0x5bd1e995);
}

}  // namespace

CuckooFilter::CuckooFilter(std::pmr::memory_resource* mr) : mr_(mr), filters_(mr) {
  DCHECK(mr);
}

void CuckooFilter::Init(const Options& options) {
  DCHECK_GT(options.slots_per_bucket, 0);
  uint16_t new_expansion = options.expansion ? NextPowerOfTwo(options.expansion) : 0;
  uint64_t new_num_buckets =
      options.slots_per_bucket ? NextPowerOfTwo(options.capacity / options.slots_per_bucket) : 1;
  if (new_num_buckets == 0)
    new_num_buckets = 1;
  DCHECK(IsPowerOfTwo(new_num_buckets));

  // Build the first SubFilter before touching any existing state: if this throws,
  // *this is left completely unchanged.
  SubFilter sf(new_num_buckets * options.slots_per_bucket, uint8_t{0}, mr_);

  // Nothing below can throw.
  filters_.clear();
  filters_.push_back(std::move(sf));
  slots_per_bucket_ = options.slots_per_bucket;
  max_iterations_ = options.max_iterations;
  expansion_ = new_expansion;
  num_buckets_ = new_num_buckets;
  num_items_ = 0;
  num_deletes_ = 0;
  num_ko_inserts_ = 0;
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

size_t CuckooFilter::Count(uint64_t hash) const {
  const LookupParams p = LookupParamsFromHash(hash);

  size_t count = 0;
  for (const SubFilter& sf : filters_) {
    auto [i1, i2] = BucketIndices(sf, p);
    for (uint64_t idx : {i1, i2}) {
      const size_t base = idx * slots_per_bucket_;
      for (uint8_t s = 0; s < slots_per_bucket_; ++s) {
        if (sf[base + s] == p.fp)
          ++count;
      }
    }
  }
  return count;
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

void CuckooFilter::Deserialize(const SerializedDataView& data) {
  std::pmr::vector<SubFilter> new_filters(filters_.get_allocator());
  new_filters.reserve(data.filters.size());
  for (const std::string& blob : data.filters) {
    SubFilter sf(blob.begin(), blob.end(), mr_);
    new_filters.push_back(std::move(sf));
  }

  // Nothing below can throw.
  slots_per_bucket_ = data.slots_per_bucket;
  max_iterations_ = data.max_iterations;
  expansion_ = data.expansion;
  num_buckets_ = data.num_buckets;
  num_items_ = data.num_items;
  num_deletes_ = data.num_deletes;
  num_ko_inserts_ = 0;
  filters_.swap(new_filters);
}

void CuckooFilter::AppendFilter(std::string_view blob) {
  SubFilter sf(reinterpret_cast<const uint8_t*>(blob.data()),
               reinterpret_cast<const uint8_t*>(blob.data()) + blob.size(), mr_);
  filters_.push_back(std::move(sf));
}

uint64_t CuckooFilter::Hash(std::string_view item) {
  return XXH3_64bits_withSeed(item.data(), item.size(), 0xc6a4a7935bd1e995ULL);
}

size_t CuckooFilter::MallocUsed() const {
  size_t res = sizeof(CuckooFilter) + filters_.capacity() * sizeof(SubFilter);
  for (const SubFilter& sf : filters_) {
    res += sf.size();
  }
  return res;
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

bool CuckooFilter::RelocateSlot(size_t filter_idx, uint64_t bucket_idx, uint8_t slot_idx) {
  SubFilter& sf = filters_[filter_idx];
  uint8_t& slot = sf[bucket_idx * slots_per_bucket_ + slot_idx];
  if (slot == 0)
    return true;

  const uint8_t fp = slot;
  // bucket_idx is this sub-filter's bucket index, not the raw hash. Reusing it works because
  // each sub-filter's bucket count is a power-of-two multiple of every earlier sub-filter's
  // count, so bucket_idx % earlier_n == raw_hash % earlier_n. Same symmetry argument as the
  // one documented above AltIndex's definition.
  const uint64_t alt_bucket_idx = AltIndex(fp, bucket_idx);

  for (size_t prior = 0; prior < filter_idx; ++prior) {
    SubFilter& prior_sf = filters_[prior];
    const uint64_t n = NumBuckets(prior_sf);
    for (uint64_t idx : {bucket_idx % n, alt_bucket_idx % n}) {
      const size_t base = idx * slots_per_bucket_;
      for (uint8_t s = 0; s < slots_per_bucket_; ++s) {
        if (prior_sf[base + s] == 0) {
          prior_sf[base + s] = fp;
          slot = 0;
          return true;
        }
      }
    }
  }
  return false;
}

bool CuckooFilter::CompactSingleFilter(size_t filter_idx) {
  const uint64_t n = NumBuckets(filters_[filter_idx]);
  bool fully_emptied = true;
  for (uint64_t bucket_idx = 0; bucket_idx < n; ++bucket_idx) {
    for (uint8_t slot_idx = 0; slot_idx < slots_per_bucket_; ++slot_idx) {
      if (!RelocateSlot(filter_idx, bucket_idx, slot_idx))
        fully_emptied = false;
    }
  }
  // Only the newest sub-filter can ever be freed: freeing a middle one would leave a gap that
  // breaks the "bucket count grows by expansion_ per index" invariant RelocateSlot relies on.
  if (fully_emptied && filter_idx == filters_.size() - 1) {
    filters_.pop_back();
  }
  return fully_emptied;
}

void CuckooFilter::Compact(bool cont) {
  for (size_t i = filters_.size(); i-- > 1;) {
    if (!CompactSingleFilter(i) && !cont)
      break;
  }
  num_deletes_ = 0;
}

bool CuckooFilter::KOInsert(const LookupParams& p, SubFilter& sf) {
  const uint64_t n = NumBuckets(sf);
  uint64_t idx = p.h1 % n;
  uint8_t fp = p.fp;
  uint8_t victim_slot = 0;

  for (uint16_t i = 0; i < max_iterations_; ++i) {
    // Evict the fingerprint at victim_slot in bucket idx and take its place.
    // Then jump to the evicted fingerprint's alternate bucket and try to place it there.
    // victim_slot cycles across slots to avoid getting stuck in displacement cycles.
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
