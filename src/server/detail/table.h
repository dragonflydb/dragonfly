// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "core/compact_object.h"
#include "core/dash.h"
#include "core/memory_accounting.h"

namespace dfly {

namespace detail {

using PrimeKey = CompactKey;
using PrimeValue = CompactValue;

struct PrimeTablePolicy {
  enum : uint8_t { kSlotNum = 14, kBucketNum = 56 };

  static constexpr bool kUseVersion = true;

  static uint64_t HashFn(const PrimeKey& s) {
    return s.HashCode();
  }

  static uint64_t HashFn(std::string_view u) {
    return CompactObj::HashCode(u);
  }

  static void DestroyKey(PrimeKey& cs) {
    cs.Reset();
  }

  static void DestroyValue(PrimeValue& o) {
    o.Reset();
  }

  static void DestroyEntryWithAccounting(PrimeKey& key, PrimeValue& value) {
    // Clear's enclosing key scope accounts for this free.
    DestroyKey(key);
    AtomicMemoryScope scope(value.ObjType());
    DestroyValue(value);
  }

  static bool Equal(const PrimeKey& s1, std::string_view s2) {
    return s1 == s2;
  }
};

struct ExpireTablePolicy {
  enum : uint8_t { kSlotNum = 14, kBucketNum = 56 };
  static constexpr bool kUseVersion = false;

  static uint64_t HashFn(const PrimeKey& s) {
    return s.HashCode();
  }

  static uint64_t HashFn(std::string_view u) {
    return CompactObj::HashCode(u);
  }

  static void DestroyKey(PrimeKey& cs) {
    cs.Reset();
  }

  static void DestroyValue(uint32_t val) {
  }

  static bool Equal(const PrimeKey& s1, std::string_view s2) {
    return s1 == s2;
  }
};

}  // namespace detail
}  // namespace dfly
