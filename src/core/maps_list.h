// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_map.h>
#include <absl/container/inlined_vector.h>

#include <vector>

#include "base/logging.h"

namespace dfly {

/* MapsList is a compact container for storing a list of maps that all share the same fixed set of
   keys.
   Instead of storing full key-value maps repeatedly, MapsList stores a single key-to-index map,
   and represents each individual map as a flat vector of values (absl::InlinedVector).

   This layout is optimized for use cases where:
    - The key set is known in advance and identical across all maps
    - You need to store many such maps efficiently

   To add a new map, use CreateNewMap(). You can then access values by key using operator[].
   Lookup is implemented via index mapping (O(1)), and keys are not stored more than once.

   Notes:
    - Value must be default-constructible.
    - Accessing a key that was not part of the original key set leads to undefined behavior. */
template <typename Key, typename Value,
          typename = std::enable_if_t<std::is_default_constructible_v<Value>>>
class MapsList {
 private:
  using KeyToIndexMap = absl::flat_hash_map<Key, size_t>;
  using ValuesVector = absl::InlinedVector<Value, 8>;

 public:
  class Map;

  // Size is number of maps, keys are fixed and shared across all maps.
  MapsList(std::initializer_list<Key> keys, size_t size = 0);
  // Size is number of maps, iterators are used to initialize keys.
  template <typename Iterator> MapsList(Iterator begin, Iterator end, size_t size = 0);

  Map operator[](size_t index);
  Map CreateNewMap();

  size_t Size() const;
  bool Empty() const;
  void Reserve(size_t size);

  // Trivially copyable liteweight wrapper around a map.
  class Map {
   private:
    friend class MapsList;

    Map(const KeyToIndexMap& key_to_index, ValuesVector* maps_values);

   public:
    using iterator = typename ValuesVector::iterator;
    using const_iterator = typename ValuesVector::const_iterator;

    Value& operator[](const Key& key);
    const Value& operator[](const Key& key) const;

    Value& operator[](size_t index);
    const Value& operator[](size_t index) const;

    bool Contains(const Key& key) const;
    iterator Find(const Key& key);
    const_iterator Find(const Key& key) const;

    iterator begin();
    iterator end();
    const_iterator begin() const;
    const_iterator end() const;

   private:
    const KeyToIndexMap& key_to_index_;
    ValuesVector* maps_values_;
  };

 private:
  KeyToIndexMap key_to_index_;
  std::vector<ValuesVector> maps_values_;
};

// Implementation
/******************************************************************/
template <typename Key, typename Value, typename E>
MapsList<Key, Value, E>::MapsList(std::initializer_list<Key> keys, size_t size) {
  key_to_index_.reserve(keys.size());
  size_t index = 0;
  for (const auto& key : keys) {
    key_to_index_[key] = index++;
  }

  maps_values_.resize(size, ValuesVector(key_to_index_.size()));
}

template <typename Key, typename Value, typename E>
template <typename Iterator>
MapsList<Key, Value, E>::MapsList(Iterator begin, Iterator end, size_t size) {
  size_t index = 0;
  for (auto it = begin; it != end; ++it) {
    key_to_index_[*it] = index++;
  }

  maps_values_.resize(size, ValuesVector(key_to_index_.size()));
}

template <typename Key, typename Value, typename E>
typename MapsList<Key, Value, E>::Map MapsList<Key, Value, E>::CreateNewMap() {
  maps_values_.emplace_back(key_to_index_.size());
  return Map(key_to_index_, &maps_values_.back());
}

template <typename Key, typename Value, typename E>
typename MapsList<Key, Value, E>::Map MapsList<Key, Value, E>::operator[](size_t index) {
  DCHECK_LT(index, maps_values_.size());
  return Map(key_to_index_, &maps_values_[index]);
}

template <typename Key, typename Value, typename E> size_t MapsList<Key, Value, E>::Size() const {
  return maps_values_.size();
}

template <typename Key, typename Value, typename E> bool MapsList<Key, Value, E>::Empty() const {
  return maps_values_.empty();
}

template <typename Key, typename Value, typename E>
void MapsList<Key, Value, E>::Reserve(size_t size) {
  maps_values_.reserve(size);
}

template <typename Key, typename Value, typename E>
MapsList<Key, Value, E>::Map::Map(const KeyToIndexMap& key_to_index, ValuesVector* maps_values)
    : key_to_index_(key_to_index), maps_values_(maps_values) {
  DCHECK(maps_values_ != nullptr);
}

template <typename Key, typename Value, typename E>
Value& MapsList<Key, Value, E>::Map::operator[](const Key& key) {
  auto it = Find(key);
  DCHECK(it != end());
  return *it;
}

template <typename Key, typename Value, typename E>
const Value& MapsList<Key, Value, E>::Map::operator[](const Key& key) const {
  auto it = Find(key);
  DCHECK(it != end());
  return *it;
}

template <typename Key, typename Value, typename E>
Value& MapsList<Key, Value, E>::Map::operator[](size_t index) {
  DCHECK_LT(index, maps_values_->size());
  return (*maps_values_)[index];
}

template <typename Key, typename Value, typename E>
const Value& MapsList<Key, Value, E>::Map::operator[](size_t index) const {
  DCHECK_LT(index, maps_values_->size());
  return (*maps_values_)[index];
}

template <typename Key, typename Value, typename E>
bool MapsList<Key, Value, E>::Map::Contains(const Key& key) const {
  return key_to_index_.find(key) != key_to_index_.end();
}

template <typename Key, typename Value, typename E>
typename MapsList<Key, Value, E>::Map::iterator MapsList<Key, Value, E>::Map::Find(const Key& key) {
  auto it = key_to_index_.find(key);
  if (it != key_to_index_.end()) {
    return begin() + it->second;
  }
  return end();
}

template <typename Key, typename Value, typename E>
typename MapsList<Key, Value, E>::Map::const_iterator MapsList<Key, Value, E>::Map::Find(
    const Key& key) const {
  auto it = key_to_index_.find(key);
  if (it != key_to_index_.end()) {
    return begin() + it->second;
  }
  return end();
}

template <typename Key, typename Value, typename E>
typename MapsList<Key, Value, E>::Map::iterator MapsList<Key, Value, E>::Map::begin() {
  return maps_values_->begin();
}

template <typename Key, typename Value, typename E>
typename MapsList<Key, Value, E>::Map::iterator MapsList<Key, Value, E>::Map::end() {
  return maps_values_->end();
}

template <typename Key, typename Value, typename E>
typename MapsList<Key, Value, E>::Map::const_iterator MapsList<Key, Value, E>::Map::begin() const {
  return maps_values_->cbegin();
}

template <typename Key, typename Value, typename E>
typename MapsList<Key, Value, E>::Map::const_iterator MapsList<Key, Value, E>::Map::end() const {
  return maps_values_->cend();
}

}  // namespace dfly
