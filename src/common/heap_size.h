// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

// This file provides utilities to *estimate* heap memory usage of classes.
// HeapSize() handles strings, trivial values and objects exposing UsedMemory(), including
// std::unique_ptr ownership. SlowHeapSize() traverses containers recursively.
//
// SlowHeapSize() traverses all container elements and is not O(1). Do not call it from
// UsedMemory(), GetMemoryUsage(), or other metric-accounting chains. Use at mutation time to
// populate cached totals instead.
//
// Example usage:
// std::vector<std::unique_ptr<int>> v;
// ...
// size_t size = SlowHeapSize(v);

#pragma once

#include <absl/container/flat_hash_set.h>
#include <absl/types/span.h>

#include <concepts>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

namespace cmn {

inline size_t HeapSize(const std::string& s) {
  constexpr size_t kSmallStringOptSize = 15;
  return s.capacity() > kSmallStringOptSize ? s.capacity() : 0UL;
}

// Overload for types that have defined UsedMemory
template <typename T>
requires requires(T t) {
  { t.UsedMemory() } -> std::convertible_to<size_t>;
}
size_t HeapSize(const T& t) {
  return t.UsedMemory();
}

// Overload for types that should be explicitly excluded from calculations
template <typename T>
requires requires {
  typename T::is_stackonly;
}
size_t HeapSize(const T& t) {
  return 0;
}

// Overload for trivial types we don't have to account for
template <typename T> size_t HeapSize(const T& t) {
  static_assert(std::is_trivial_v<T> || std::is_same_v<std::string_view, T>,
                "Use SlowHeapSize for containers");
  return 0;
}

// Declare first, so that we can use these "recursively"
template <typename T> size_t HeapSize(const std::unique_ptr<T>& t);
template <typename T1, typename T2> size_t HeapSize(const std::pair<T1, T2>& p);

template <typename T> size_t HeapSize(const std::unique_ptr<T>& t) {
  if (t == nullptr) {
    return 0;
  } else {
    return sizeof(T) + HeapSize(*t);
  }
}

template <typename T1, typename T2> size_t HeapSize(const std::pair<T1, T2>& p) {
  return HeapSize(p.first) + HeapSize(p.second);
}

template <typename T> size_t SlowHeapSize(const std::vector<T>& v);
template <typename K> size_t SlowHeapSize(const absl::flat_hash_set<K>& s);

// Only the container overloads above participate; strings and cached objects do not match.
template <typename T>
requires requires(const T& t) {
  { cmn::SlowHeapSize(t) } -> std::same_as<size_t>;
}
size_t SlowHeapSize(const std::unique_ptr<T>& t) {
  return t ? sizeof(T) + SlowHeapSize(*t) : 0;
}

namespace detail {
template <typename Container> size_t AccumulateContainer(const Container& c) {
  size_t size = 0;
  for (const auto& e : c) {
    if constexpr (requires { cmn::SlowHeapSize(e); })
      size += SlowHeapSize(e);
    else
      size += HeapSize(e);
  }
  return size;
}
}  // namespace detail

template <typename T> size_t SlowHeapSize(const std::vector<T>& v) {
  return (v.capacity() * sizeof(T)) + detail::AccumulateContainer(v);
}

template <typename K> size_t SlowHeapSize(const absl::flat_hash_set<K>& s) {
  size_t size = s.capacity() * sizeof(typename absl::flat_hash_set<K>::value_type);
  return size + detail::AccumulateContainer(s);
}

}  // namespace cmn
