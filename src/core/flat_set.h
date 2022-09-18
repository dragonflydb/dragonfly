// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once
#include <absl/container/flat_hash_set.h>

#include <memory_resource>

#include "core/compact_object.h"

namespace dfly {

class FlatSet {
  struct Hasher {
    using is_transparent = void;  // to allow heterogeneous lookups.

    size_t operator()(const CompactObj& o) const {
      return o.HashCode();
    }

    size_t operator()(std::string_view s) const {
      return CompactObj::HashCode(s);
    }
  };

  struct Eq {
    using is_transparent = void;  // to allow heterogeneous lookups.

    bool operator()(const CompactObj& left, const CompactObj& right) const {
      return left == right;
    }

    bool operator()(const CompactObj& left, std::string_view right) const {
      return left == right;
    }
  };

  using FlatSetType =
      absl::flat_hash_set<CompactObj, Hasher, Eq, std::pmr::polymorphic_allocator<CompactObj>>;

 public:
  using iterator = FlatSetType::iterator;

  FlatSet(std::pmr::memory_resource* mr) : set_(mr) {
  }

  void Reserve(size_t sz) {
    set_.reserve(sz);
  }

  bool Add(std::string_view str) {
    return set_.emplace(str).second;
  }

  bool Remove(std::string_view str) {
    size_t res = set_.erase(str);
    return res > 0;
  }

  void Erase(iterator it) {
    set_.erase(it);
  }

  size_t Size() const {
    return set_.size();
  }

  bool Empty() const {
    return set_.empty();
  }

  bool Contains(std::string_view val) const {
    return set_.contains(val);
  }

  auto begin() const {
    return set_.begin();
  }

  auto begin() {
    return set_.begin();
  }

  auto end() const {
    return set_.end();
  }

  auto end() {
    return set_.end();
  }

 private:
  FlatSetType set_;
};

}  // namespace dfly
