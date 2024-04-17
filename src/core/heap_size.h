// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

// This file provides utilities to *estimate* heap memory usage of classes.
// The main function exposed here is HeapSize() (with various overloads).
// It supports simple structs (returns 0), std::string (returns capacity if it's larger than SSO)
// and common containers, such as std::vector, std::deque, absl::flat_hash_map and unique_ptr.
//
// Example usage:
// absl::flat_hash_map<std::string, std::vector<std::unique_ptr<int>>> m;
// ...
// size_t size = HeapSize(m);

#pragma once

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <absl/container/inlined_vector.h>
#include <absl/types/span.h>

#include <deque>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

namespace dfly {

namespace heap_size_detail {

template <class, class = void> struct has_marked_stackonly : std::false_type {};

template <class T>
struct has_marked_stackonly<T, std::void_t<typename T::is_stackonly>> : std::true_type {};

template <typename T> constexpr bool StackOnlyType() {
  return std::is_trivial_v<T> || std::is_same_v<T, std::string_view> ||
         has_marked_stackonly<T>::value;
}

template <typename T, typename = void> struct has_used_mem : std::false_type {};

template <typename T>
struct has_used_mem<T, std::void_t<decltype(&T::UsedMemory)>> : std::true_type {};

template <typename Container> size_t AccumulateContainer(const Container& c);
}  // namespace heap_size_detail

inline size_t HeapSize(const std::string& s) {
  constexpr size_t kSmallStringOptSize = 15;
  return s.capacity() > kSmallStringOptSize ? s.capacity() : 0UL;
}

template <typename T, std::enable_if_t<heap_size_detail::has_used_mem<T>::value, bool> = true>
size_t HeapSize(const T& t) {
  return t.UsedMemory();
}

template <typename T, std::enable_if_t<heap_size_detail::StackOnlyType<T>(), bool> = true>
size_t HeapSize(const T& t) {
  return 0;
}

template <typename T> size_t HeapSize(absl::Span<T>) {
  return 0;
}

// Declare first, so that we can use these "recursively"
template <typename T> size_t HeapSize(const std::vector<T>& v);
template <typename T> size_t HeapSize(const std::unique_ptr<T>& t);
template <typename T> size_t HeapSize(const std::deque<T>& d);
template <typename T1, typename T2> size_t HeapSize(const std::pair<T1, T2>& p);
template <typename T, size_t N> size_t HeapSize(const absl::InlinedVector<T, N>& v);
template <typename K, typename V> size_t HeapSize(const absl::flat_hash_map<K, V>& m);
template <typename K> size_t HeapSize(const absl::flat_hash_set<K>& s);

template <typename T> size_t HeapSize(const std::unique_ptr<T>& t) {
  if (t == nullptr) {
    return 0;
  } else {
    return sizeof(T) + HeapSize(*t);
  }
}

template <typename T> size_t HeapSize(const std::vector<T>& v) {
  return (v.capacity() * sizeof(T)) + heap_size_detail::AccumulateContainer(v);
}

template <typename T> size_t HeapSize(const std::deque<T>& d) {
  return (d.size() * sizeof(T)) + heap_size_detail::AccumulateContainer(d);
}

template <typename T1, typename T2> size_t HeapSize(const std::pair<T1, T2>& p) {
  return HeapSize(p.first) + HeapSize(p.second);
}

template <typename T, size_t N> size_t HeapSize(const absl::InlinedVector<T, N>& v) {
  size_t size = 0;
  if (v.capacity() > N) {
    size += v.capacity() * sizeof(T);
  }
  size += heap_size_detail::AccumulateContainer(v);
  return size;
}

template <typename K, typename V> size_t HeapSize(const absl::flat_hash_map<K, V>& m) {
  size_t size = m.capacity() * sizeof(typename absl::flat_hash_map<K, V>::value_type);

  if constexpr (!heap_size_detail::StackOnlyType<K>() || !heap_size_detail::StackOnlyType<V>()) {
    for (const auto& kv : m) {
      size += HeapSize(kv);
    }
  }

  return size;
}

template <typename K> size_t HeapSize(const absl::flat_hash_set<K>& s) {
  size_t size = s.capacity() * sizeof(typename absl::flat_hash_set<K>::value_type);

  if constexpr (!heap_size_detail::StackOnlyType<K>()) {
    for (const auto& k : s) {
      size += HeapSize(k);
    }
  }

  return size;
}

namespace heap_size_detail {
template <typename Container> size_t AccumulateContainer(const Container& c) {
  size_t size = 0;

  if constexpr (!heap_size_detail::StackOnlyType<typename Container::value_type>()) {
    for (const auto& e : c) {
      size += HeapSize(e);
    }
  }

  return size;
}
}  // namespace heap_size_detail

}  // namespace dfly
