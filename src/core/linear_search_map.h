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
     - You must ensure keys are unique when inserting. */
template <typename Key, typename Value, size_t N = 8> class LinearSearchMap {
 private:
  using Vector = absl::InlinedVector<std::pair<Key, Value>, N>;

 public:
  using iterator = typename Vector::iterator;
  using const_iterator = typename Vector::const_iterator;

  LinearSearchMap() = default;

  // Does not check if key already exists.
  // If key already exists - undefined behavior.
  void Insert(Key key, Value value);
  template <typename... Args> void Emplace(Key key, Args&&... args);

  void Erase(iterator it);
  void Erase(const Key& key);

  bool Contains(const Key& key) const;

  Value& operator[](size_t index);
  const Value& operator[](size_t index) const;

  iterator Find(const Key& key);
  const_iterator Find(const Key& key) const;
  size_t FindIndex(const Key& key) const;

  size_t Size() const;
  bool Empty() const;

  void Reserve(size_t n);

  iterator begin();
  const_iterator begin() const;
  iterator end();
  const_iterator end() const;

 private:
  Vector data_;
};

// Implementation
/******************************************************************/
template <typename Key, typename Value, size_t N>
void LinearSearchMap<Key, Value, N>::Insert(Key key, Value value) {
  DCHECK(!Contains(key)) << "Key already exists: " << key;
  data_.emplace_back(std::move(key), std::move(value));
}

template <typename Key, typename Value, size_t N>
template <typename... Args>
void LinearSearchMap<Key, Value, N>::Emplace(Key key, Args&&... args) {
  DCHECK(!Contains(key)) << "Key already exists: " << key;
  data_.emplace_back(std::piecewise_construct, std::forward_as_tuple(std::move(key)),
                     std::forward_as_tuple(std::forward<Args>(args)...));
}

template <typename Key, typename Value, size_t N>
void LinearSearchMap<Key, Value, N>::Erase(iterator it) {
  data_.erase(it);
}

template <typename Key, typename Value, size_t N>
void LinearSearchMap<Key, Value, N>::Erase(const Key& key) {
  data_.erase(Find(key));
}

template <typename Key, typename Value, size_t N>
bool LinearSearchMap<Key, Value, N>::Contains(const Key& key) const {
  return Find(key) != end();
}

template <typename Key, typename Value, size_t N>
Value& LinearSearchMap<Key, Value, N>::operator[](size_t index) {
  return data_[index].second;
}

template <typename Key, typename Value, size_t N>
const Value& LinearSearchMap<Key, Value, N>::operator[](size_t index) const {
  return data_[index].second;
}

template <typename Key, typename Value, size_t N>
typename LinearSearchMap<Key, Value, N>::iterator LinearSearchMap<Key, Value, N>::Find(
    const Key& key) {
  return std::find_if(data_.begin(), data_.end(),
                      [&key](const auto& pair) { return pair.first == key; });
}

template <typename Key, typename Value, size_t N>
typename LinearSearchMap<Key, Value, N>::const_iterator LinearSearchMap<Key, Value, N>::Find(
    const Key& key) const {
  return std::find_if(data_.begin(), data_.end(),
                      [&key](const auto& pair) { return pair.first == key; });
}

template <typename Key, typename Value, size_t N>
size_t LinearSearchMap<Key, Value, N>::FindIndex(const Key& key) const {
  return std::distance(data_.begin(), Find(key));
}

template <typename Key, typename Value, size_t N>
size_t LinearSearchMap<Key, Value, N>::Size() const {
  return data_.size();
}

template <typename Key, typename Value, size_t N>
bool LinearSearchMap<Key, Value, N>::Empty() const {
  return data_.empty();
}

template <typename Key, typename Value, size_t N>
void LinearSearchMap<Key, Value, N>::Reserve(size_t n) {
  data_.reserve(n);
}

template <typename Key, typename Value, size_t N>
typename LinearSearchMap<Key, Value, N>::iterator LinearSearchMap<Key, Value, N>::begin() {
  return data_.begin();
}

template <typename Key, typename Value, size_t N>
typename LinearSearchMap<Key, Value, N>::const_iterator LinearSearchMap<Key, Value, N>::begin()
    const {
  return data_.begin();
}

template <typename Key, typename Value, size_t N>
typename LinearSearchMap<Key, Value, N>::iterator LinearSearchMap<Key, Value, N>::end() {
  return data_.end();
}

template <typename Key, typename Value, size_t N>
typename LinearSearchMap<Key, Value, N>::const_iterator LinearSearchMap<Key, Value, N>::end()
    const {
  return data_.end();
}

}  // namespace dfly
