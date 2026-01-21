// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include "base/logging.h"
#include "core/collection_entry.h"
#include "core/compact_object.h"
#include "server/table.h"

extern "C" {
#include "redis/listpack.h"
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

using ContainerEntry = CollectionEntry;

using IterateFunc = std::function<bool(ContainerEntry)>;
using IterateSortedFunc = std::function<bool(ContainerEntry, double)>;
using IterateKVFunc = std::function<bool(ContainerEntry, ContainerEntry)>;

// Iterate over all values in [start, end] range (inclusive) and call func(val).
// Iteration stops as soon
// as func return false. Returns true if it successfully processed all elements
// without breaking.
bool IterateList(const PrimeValue& pv, const IterateFunc& func, size_t start = 0,
                 size_t end = SIZE_MAX);

// Iterate over all values and call func(val). Iteration stops as soon
// as func return false. Returns true if it successfully processed all elements
// without stopping.
bool IterateSet(const PrimeValue& pv, const IterateFunc& func);

// Iterate over all values and call func(val). Iteration stops as soon
// as func return false. Returns true if it successfully processed all elements
// without stopping.
bool IterateSortedSet(const PrimeValue& pv, const IterateSortedFunc& func, size_t start = 0,
                      size_t end = SIZE_MAX, bool reverse = false, bool use_score = false);

bool IterateMap(const PrimeValue& pv, const IterateKVFunc& func);

// Get StringMap pointer from primetable value. Sets expire time from db_context
StringMap* GetStringMap(const PrimeValue& pv, const DbContext& db_context);

using BlockingResultCb =
    std::function<void(Transaction*, EngineShard*, std::string_view /* key */)>;

// Block until a any key of the transaction becomes non-empty and executes the callback.
// If multiple keys are non-empty when this function is called, the callback is executed
// immediately with the first key listed in the tx arguments.
OpResult<std::string> RunCbOnFirstNonEmptyBlocking(Transaction* trans, int req_obj_type,
                                                   BlockingResultCb cb, unsigned limit_ms,
                                                   bool* block_flag, bool* pause_flag);

};  // namespace container_utils

}  // namespace dfly
