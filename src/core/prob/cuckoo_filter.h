// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <array>
#include <cstdint>
#include <optional>
#include <vector>

#include "core/detail/fixed_array.h"

namespace dfly::prob {

struct CuckooReserveParams {
  static constexpr uint8_t kDefaultBucketSize = 2;
  static constexpr uint16_t kDefaultMaxIterations = 20;
  static constexpr uint16_t kDefaultExpansion = 1;

  uint8_t bucket_size = kDefaultBucketSize;
  uint16_t max_iterations = kDefaultMaxIterations;
  uint16_t expansion = kDefaultExpansion;
  uint64_t capacity;
};

class CuckooFilter {
 private:
  using Entry = uint8_t;
  // SubFilter stores num_buckets * bucket_size entries.
  using SubFilter = detail::PmrFixedArray<Entry>;

  struct LookupParams {
    Entry fp;     // fingerprint
    uint64_t h1;  // first hash
    uint64_t h2;  // second hash
  };

  static LookupParams MakeLookupParams(uint64_t hash, uint64_t num_buckets);

 public:
  static std::optional<CuckooFilter> Init(const CuckooReserveParams& params,
                                          std::pmr::memory_resource* mr);

  using Hash = uint64_t;
  static Hash GetHash(std::string_view item);

  CuckooFilter(const CuckooFilter&) = delete;
  CuckooFilter& operator=(const CuckooFilter&) = delete;

  CuckooFilter(CuckooFilter&&) = default;
  CuckooFilter& operator=(CuckooFilter&&) = delete;

  ~CuckooFilter() = default;

  bool Insert(Hash hash);

  bool Exists(std::string_view item) const;
  bool Exists(Hash hash) const;

  bool Delete(std::string_view item);

  uint64_t Count(std::string_view item) const;

  size_t UsedBytes() const;
  size_t NumItems() const;

 private:
  explicit CuckooFilter(const CuckooReserveParams& params, std::pmr::memory_resource* mr);

  // Inserts new subfilter with expansion ^ filters_.size() buckets.
  bool AddNewSubFilter();

  /* Attempts to insert the fingerprint by randomly evicting existing entries ("kick-out").
     The evicted fingerprint is recursively reinserted into its alternate bucket up to
     max_iterations. */
  bool KOInsert(const LookupParams& p, SubFilter* sub_filter);

  std::array<uint64_t, 2> GetIndexesInSubFilter(const SubFilter& sub_filter,
                                                const LookupParams& p) const;

  uint64_t GetNumBucketsInSubFilter(const SubFilter& sub_filter) const;

 private:
  const uint8_t bucket_size_;
  const uint16_t max_iterations_;
  const uint16_t expansion_;

  uint64_t num_buckets_ = 0;
  uint64_t num_items_ = 0;
  uint64_t num_deletes_ = 0;

  std::pmr::vector<SubFilter> filters_;
  std::pmr::memory_resource* mr_;
};

// Implementation
/******************************************************************/
inline std::array<uint64_t, 2> CuckooFilter::GetIndexesInSubFilter(const SubFilter& sub_filter,
                                                                   const LookupParams& p) const {
  const uint64_t num_buckets = GetNumBucketsInSubFilter(sub_filter);
  return {p.h1 % num_buckets, p.h2 % num_buckets};
}

inline uint64_t CuckooFilter::GetNumBucketsInSubFilter(const SubFilter& sub_filter) const {
  return sub_filter.size() / bucket_size_;
}

inline size_t CuckooFilter::UsedBytes() const {
  // TODO: there is a bug in the code
  return filters_.capacity() * sizeof(SubFilter) + sizeof(CuckooFilter);
}

inline size_t CuckooFilter::NumItems() const {
  return num_items_;
}

}  // namespace dfly::prob
