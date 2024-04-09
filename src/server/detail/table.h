// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "core/compact_object.h"
#include "core/dash.h"
#include "core/expire_period.h"

namespace dfly {

namespace detail {

using PrimeKey = CompactObj;
using PrimeValue = CompactObj;

struct PrimeTablePolicy {
  enum { kSlotNum = 14, kBucketNum = 56, kStashBucketNum = 4 };

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

  static bool Equal(const PrimeKey& s1, std::string_view s2) {
    return s1 == s2;
  }

  static bool Equal(const PrimeKey& s1, const PrimeKey& s2) {
    return s1 == s2;
  }
};

struct ExpireTablePolicy {
  enum { kSlotNum = 14, kBucketNum = 56, kStashBucketNum = 4 };
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

  static void DestroyValue(ExpirePeriod e) {
  }

  static void DestroyValue(uint32_t val) {
  }

  static bool Equal(const PrimeKey& s1, std::string_view s2) {
    return s1 == s2;
  }

  static bool Equal(const PrimeKey& s1, const PrimeKey& s2) {
    return s1 == s2;
  }
};

}  // namespace detail
}  // namespace dfly
