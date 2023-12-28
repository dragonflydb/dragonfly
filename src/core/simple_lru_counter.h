// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <absl/container/flat_hash_map.h>

#include "base/string_view_sso.h"

namespace dfly {

class SimpleLruCounter {
  struct Node {
    base::string_view_sso key;  // key to the table.

    uint32_t prev;
    uint32_t next;

    uint64_t count;

    Node() : prev(0), next(0), count(0) {
    }
  };

 public:
  explicit SimpleLruCounter(size_t capacity);
  ~SimpleLruCounter();

  std::optional<uint64_t> Get(std::string_view key) const;
  std::optional<uint64_t> GetPrev(std::string_view key) const;
  std::optional<uint64_t> GetLast() const;
  enum class Position {
    kHead,
    kTail,
  };
  void Put(std::string_view key, uint64_t count, Position position = Position::kHead);
  void Remove(std::string_view key);

  size_t Size() const {
    return table_.size();
  }

 private:
  void BumpToHead(uint32_t index);
  void MoveToTail(uint32_t index);

  void MoveToPosition(uint32_t index, Position position);

  absl::flat_hash_map<std::string, uint32_t> table_;
  std::vector<Node> node_arr_;
  uint32_t head_;
  size_t grow_size_;
};

};  // namespace dfly
