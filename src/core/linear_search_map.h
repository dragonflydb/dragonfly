// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/inlined_vector.h>

#include "base/logging.h"

namespace dfly {

/* LinearSearchMap is a small key-value map implemented using an inlined vector of (key, value)
   pairs. It performs key lookup using linear search (O(n)) and is optimized for small maps
   (typically <32 keys).

   Compared to a hash map, it avoids hashing overhead and has better memory locality and cache
   performance. Use it when:
    - The number of keys is small
    - You care about minimal memory usage
    - Fast iteration is more important than fast lookup

   NOTE:
     - Insert() and Emplace() do NOT check for duplicate keys at runtime.
       Inserting a duplicate key results in undefined behavior.
     - You must ensure keys are unique when inserting.
     - This syntax is used to maintain compatibility with absl::InlinedVector. */
template <typename Key, typename Value, size_t N = 8>
class LinearSearchMap : public absl::InlinedVector<std::pair<Key, Value>, N> {
 private:
  using Base = absl::InlinedVector<std::pair<Key, Value>, N>;
  using Base::emplace_back;

 public:
  using Base::begin;
  using Base::clear;
  using Base::empty;
  using Base::end;
  using Base::reserve;
  using Base::shrink_to_fit;
  using Base::size;
  using Base::operator[];
  using Base::erase;
  using Base::resize;

  using iterator = typename Base::iterator;
  using const_iterator = typename Base::const_iterator;

  // Does not check if key already exists.
  // If key already exists - undefined behavior.
  void insert(Key key, Value value);
  template <typename... Args> void emplace(Key key, Args&&... args);

  void erase(const Key& key);

  bool contains(const Key& key) const;

  iterator find(const Key& key);
  const_iterator find(const Key& key) const;
  size_t find_index(const Key& key) const;
};

// Implementation
/******************************************************************/
template <typename Key, typename Value, size_t N>
void LinearSearchMap<Key, Value, N>::insert(Key key, Value value) {
  DCHECK(!contains(key)) << "Key already exists: " << key;
  emplace_back(std::move(key), std::move(value));
}

template <typename Key, typename Value, size_t N>
template <typename... Args>
void LinearSearchMap<Key, Value, N>::emplace(Key key, Args&&... args) {
  DCHECK(!contains(key)) << "Key already exists: " << key;
  emplace_back(std::piecewise_construct, std::forward_as_tuple(std::move(key)),
               std::forward_as_tuple(std::forward<Args>(args)...));
}

template <typename Key, typename Value, size_t N>
void LinearSearchMap<Key, Value, N>::erase(const Key& key) {
  erase(find(key));
}

template <typename Key, typename Value, size_t N>
bool LinearSearchMap<Key, Value, N>::contains(const Key& key) const {
  return find(key) != end();
}

template <typename Key, typename Value, size_t N>
typename LinearSearchMap<Key, Value, N>::iterator LinearSearchMap<Key, Value, N>::find(
    const Key& key) {
  return std::find_if(begin(), end(), [&key](const auto& pair) { return pair.first == key; });
}

template <typename Key, typename Value, size_t N>
typename LinearSearchMap<Key, Value, N>::const_iterator LinearSearchMap<Key, Value, N>::find(
    const Key& key) const {
  return std::find_if(begin(), end(), [&key](const auto& pair) { return pair.first == key; });
}

template <typename Key, typename Value, size_t N>
size_t LinearSearchMap<Key, Value, N>::find_index(const Key& key) const {
  return std::distance(begin(), find(key));
}

}  // namespace dfly
