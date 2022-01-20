// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_map.h>

#include "core/compact_object.h"
#include "core/dash.h"

namespace dfly {

namespace detail {

struct ExpireTablePolicy {
  enum { kSlotNum = 12, kBucketNum = 64, kStashBucketNum = 2 };
  static constexpr bool kUseVersion = false;

  static uint64_t HashFn(const CompactObj& s) {
    return s.HashCode();
  }

  static void DestroyKey(CompactObj& cs) {
    cs.Reset();
  }

  static void DestroyValue(uint64_t) {
  }

  static bool Equal(const CompactObj& s1, const CompactObj& s2) {
    return s1 == s2;
  }
};

}  // namespace detail

using MainValue = CompactObj;
using MainTable = absl::flat_hash_map<std::string, MainValue>;
using ExpireTable = DashTable<CompactObj, uint64_t, detail::ExpireTablePolicy>;

/// Iterators are invalidated when new keys are added to the table or some entries are deleted.
/// Iterators are still valid  if a different entry in the table was mutated.
using MainIterator = MainTable::iterator;
using ExpireIterator = ExpireTable::iterator;

inline bool IsValid(MainIterator it) {
  return it != MainIterator{};
}

inline bool IsValid(ExpireIterator it) {
  return !it.is_done();
}

}  // namespace dfly
