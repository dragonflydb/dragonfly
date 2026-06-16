// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cmath>
#include <cstdint>
#include <memory_resource>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/numeric/bits.h"

namespace dfly {

class CuckooFilter {
 public:
  static constexpr uint8_t kDefaultSlotsPerBucket = 2;
  static constexpr uint16_t kDefaultMaxIterations = 20;
  static constexpr uint16_t kDefaultExpansion = 1;

  explicit CuckooFilter(uint64_t capacity, std::pmr::memory_resource* mr,
                        uint8_t slots_per_bucket = kDefaultSlotsPerBucket,
                        uint16_t max_iterations = kDefaultMaxIterations,
                        uint16_t expansion = kDefaultExpansion);

  // Inserts a pre-computed hash. Returns false only if the filter is full
  // and expansion is disabled (expansion_ == 0) or memory allocation fails.
  // Allows duplicate insertions — use InsertUnique to prevent them.
  bool Insert(uint64_t hash);

  // Inserts only if hash is not already present. Returns false if the item
  // already exists or the filter is full.
  bool InsertUnique(uint64_t hash);

  // Returns true if hash is present in the filter. May return false positives
  // but never false negatives.
  // TODO(kostas): SIMD for the inner bucket scan. Establish a baseline bench and then add SIMD..
  bool Exists(uint64_t hash) const;

  // Removes one occurrence of hash from the filter. Returns true if found and removed.
  // This is the key advantage over Bloom filters, which do not support deletion.
  bool Delete(uint64_t hash);

  static uint64_t Hash(std::string_view item);

  size_t NumItems() const {
    return num_items_;
  }

  size_t NumKOInserts() const {
    return num_ko_inserts_;
  }

  // Returns approximate heap bytes used by this filter's SubFilter data.
  size_t UsedMemory() const;

 private:
  using SubFilter = std::pmr::vector<uint8_t>;

  struct LookupParams {
    uint8_t fp;
    uint64_t h1;  // raw (unmodded) first candidate index
    uint64_t h2;  // raw (unmodded) alternate index
  };

  static bool IsPowerOfTwo(uint64_t n) {
    return absl::has_single_bit(n);
  }

  static uint64_t NextPowerOfTwo(uint64_t n) {
    return absl::bit_ceil(n);
  }

  // Result is in [1, 255] — 0 is reserved as "empty slot".
  static uint8_t Fingerprint(uint64_t hash) {
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
  static uint64_t AltIndex(uint8_t fp, uint64_t index) {
    return index ^ (static_cast<uint64_t>(fp) * 0x5bd1e995);
  }

  // Returns the number of buckets for SubFilter at position i.
  // Each successive SubFilter grows by expansion^i relative to base num_buckets_.
  static uint64_t SubFilterBucketCount(uint64_t num_buckets, uint64_t expansion, size_t i) {
    return num_buckets * static_cast<uint64_t>(std::pow(expansion, i));
  }

  LookupParams LookupParamsFromHash(uint64_t hash) const;

  // Returns {h1 % num_buckets, h2 % num_buckets} for the given SubFilter.
  std::pair<uint64_t, uint64_t> BucketIndices(const SubFilter& sf, const LookupParams& p) const;

  uint64_t NumBuckets(const SubFilter& sf) const;

  // Appends a new SubFilter sized num_buckets_ * expansion_^filters_.size().
  // This is a Redis engineering choice to avoid rehashing on growth; the original
  // Fan et al. paper describes a single fixed-size filter only.
  bool AddNewSubFilter();

  // When both candidate buckets are full, evicts a fingerprint from h1, places ours
  // there, then tries to reinsert the evicted fingerprint into its own alternate bucket.
  // Repeats up to max_iterations_ times. On failure, rolls back all swaps.
  bool KOInsert(const LookupParams& p, SubFilter& sf);

  const uint8_t slots_per_bucket_;
  const uint16_t max_iterations_;
  const uint16_t expansion_;

  uint64_t num_buckets_ = 0;
  uint64_t num_items_ = 0;
  uint64_t num_deletes_ = 0;
  uint64_t num_ko_inserts_ = 0;

  std::pmr::memory_resource* mr_;
  std::pmr::vector<SubFilter> filters_;
};

}  // namespace dfly
