// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include "core/compact_object.h"
#include "core/string_set.h"
#include "server/table.h"

extern "C" {
#include "redis/object.h"
#include "redis/quicklist.h"
}

#include <functional>

namespace dfly {

namespace container_utils {

// IsContainer returns true if the iterator points to a container type.
inline bool IsContainer(const PrimeValue& pv) {
  unsigned type = pv.ObjType();
  return (type == OBJ_LIST || type == OBJ_SET || type == OBJ_ZSET);
}

// Create empty quicklistEntry
quicklistEntry QLEntry();

// Stores either:
// - A single long long value (longval) when value = nullptr
// - A single char* (value) when value != nullptr
struct ContainerEntry {
  ContainerEntry(const char* value, size_t length) : value{value}, length{length} {
  }
  ContainerEntry(long long longval) : value{nullptr}, longval{longval} {
  }

  std::string ToString() {
    if (value)
      return {value, length};
    else
      return absl::StrCat(longval);
  }

  const char* value;
  union {
    size_t length;
    long long longval;
  };
};

using IterateFunc = std::function<bool(ContainerEntry)>;
using IterateSortedFunc = std::function<bool(ContainerEntry, double)>;

// Iterate over all values and call func(val). Iteration stops as soon
// as func return false. Returns true if it successfully processed all elements
// without stopping.
bool IterateList(const PrimeValue& pv, const IterateFunc& func, long start = 0, long end = -1);

// Iterate over all values and call func(val). Iteration stops as soon
// as func return false. Returns true if it successfully processed all elements
// without stopping.
bool IterateSet(const PrimeValue& pv, const IterateFunc& func);

// Iterate over all values and call func(val). Iteration stops as soon
// as func return false. Returns true if it successfully processed all elements
// without stopping.
bool IterateSortedSet(robj* zobj, const IterateSortedFunc& func, int32_t start = 0,
                      int32_t end = -1, bool reverse = false, bool use_score = false);

};  // namespace container_utils

}  // namespace dfly
