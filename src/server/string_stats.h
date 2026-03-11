// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

extern "C" {
#include "redis/hyperloglog.h"
}

#include "server/container_utils.h"

namespace dfly {

struct UniqueStrings {
  uint64_t total_count{0};
  uint64_t total_bytes{0};

  UniqueStrings();
  ~UniqueStrings() {
    delete[] counter_.hll;
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

  void Add(UniqueStrings&& other);

  std::string ToString(std::string_view label) const;

 private:
  HllBufferPtr counter_;
  bool AddString(const container_utils::ContainerEntry& e);

  uint64_t ByteSavingsOnDedup() const;

  uint64_t UniqueCount() const {
    return pfcountSingle(counter_);
  }

  double AverageLength() const {
    return total_count ? static_cast<double>(total_bytes) / total_count : 0;
  }
};

}  // namespace dfly
