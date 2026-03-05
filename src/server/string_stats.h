// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

extern "C" {
#include "redis/hyperloglog.h"
}

#include "server/container_utils.h"
#include "server/table.h"

namespace dfly {

// Single shard counter, provides API to consume data structures and update counter
struct UniqueStrings {
  HllBufferPtr ctr;
  uint64_t total_count{0};
  uint64_t total_bytes{0};

  UniqueStrings();
  ~UniqueStrings() {
    delete[] ctr.hll;
  }

  UniqueStrings(const UniqueStrings&) = delete;
  UniqueStrings& operator=(const UniqueStrings&) = delete;

  // To store in flat hash map
  UniqueStrings(UniqueStrings&&) noexcept;
  UniqueStrings& operator=(UniqueStrings&&) noexcept;

  void AddHMap(const PrimeValue& pv);
  void AddSet(const PrimeValue& pv);
  void AddList(const PrimeValue& pv);
  void AddZSet(const PrimeValue& pv);

 private:
  bool AddString(const container_utils::ContainerEntry& e);
};

// Provides machine wide (cross-shard) summary for given data structure type
struct UniqueStringsSummary {
  HllBufferPtr ctr;
  uint64_t total_count{0};
  uint64_t total_bytes{0};

  UniqueStringsSummary();
  ~UniqueStringsSummary() {
    delete[] ctr.hll;
  }

  UniqueStringsSummary(const UniqueStringsSummary&) = delete;
  UniqueStringsSummary& operator=(const UniqueStringsSummary&) = delete;

  // To store in flat hash map
  UniqueStringsSummary(UniqueStringsSummary&&) noexcept;
  UniqueStringsSummary& operator=(UniqueStringsSummary&&) noexcept;

  void Add(const UniqueStrings& u);

  uint64_t UniqueCount() const {
    return pfcountSingle(ctr);
  }

  double AverageLength() const {
    return total_count ? static_cast<double>(total_bytes) / total_count : 0;
  }

  uint64_t ByteSavingsOnDedup() const {
    const auto uniques = UniqueCount();
    const auto diff = total_count > uniques ? total_count - uniques : 0;
    return diff * AverageLength();
  }

  std::string ToString(std::string_view label) const;
};

}  // namespace dfly
