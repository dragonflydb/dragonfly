// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_map.h>

#include "core/compact_object.h"

namespace dfly {

using MainValue = CompactObj;
using MainTable = absl::flat_hash_map<std::string, MainValue>;
using ExpireTable = absl::flat_hash_map<std::string, uint64_t>;

/// Iterators are invalidated when new keys are added to the table or some entries are deleted.
/// Iterators are still valid  if a different entry in the table was mutated.
using MainIterator = MainTable::iterator;
using ExpireIterator = ExpireTable::iterator;

inline bool IsValid(MainIterator it) {
  return it != MainIterator{};
}

inline bool IsValid(ExpireIterator it) {
  return it != ExpireIterator{};
}

}  // namespace dfly
