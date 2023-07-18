// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include "base/logging.h"
#include "core/compact_object.h"
#include "server/table.h"

extern "C" {
#include "redis/listpack.h"
#include "redis/object.h"
#include "redis/quicklist.h"
}

#include <functional>

namespace dfly {

class StringMap;

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
bool IterateSortedSet(const detail::RobjWrapper* robj_wrapper, const IterateSortedFunc& func,
                      int32_t start = 0, int32_t end = -1, bool reverse = false,
                      bool use_score = false);

// Get StringMap pointer from primetable value. Sets expire time from db_context
StringMap* GetStringMap(const PrimeValue& pv, const DbContext& db_context);

// Get string_view from listpack poiner. Intbuf to store integer values as strings.
std::string_view LpGetView(uint8_t* lp_it, uint8_t int_buf[]);

// Find value by key and return stringview to it, otherwise nullopt.
std::optional<std::string_view> LpFind(uint8_t* lp, std::string_view key, uint8_t int_buf[]);

struct ShardFFResult {
  PrimeKey key;
  ShardId sid = kInvalidSid;
};

OpResult<ShardFFResult> FindFirstNonEmptyKey(Transaction* trans, int req_obj_type);

using BlockingResultCb = std::function<void(Transaction*, EngineShard*, std::string_view)>;
OpResult<std::string> RunCbOnFirstNonEmptyBlocking(Transaction* trans, int req_obj_type,
                                                   BlockingResultCb cb, unsigned limit_ms);

};  // namespace container_utils

}  // namespace dfly
