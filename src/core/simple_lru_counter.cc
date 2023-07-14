// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/simple_lru_counter.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>

#include "absl/container/flat_hash_map.h"
#include "absl/meta/type_traits.h"
#include "glog/logging.h"

namespace dfly {

using namespace std;

SimpleLruCounter::SimpleLruCounter(size_t capacity) : head_(0) {
  CHECK_GT(capacity, 1u);
  node_arr_.resize(capacity);
}

SimpleLruCounter::~SimpleLruCounter() {
}

optional<uint64_t> SimpleLruCounter::Get(string_view key) const {
  auto it = table_.find(key);
  if (it == table_.end()) {
    return nullopt;
  }
  const auto& node = node_arr_[it->second];

  DCHECK_EQ(node.key, key);

  return node.count;
}

void SimpleLruCounter::Put(string_view key, uint64_t value) {
  auto [it, inserted] = table_.emplace(key, table_.size());

  if (inserted) {
    unsigned tail = node_arr_[head_].prev;  // 0 if we had 1 or 0 elements.

    if (it->second < node_arr_.size()) {
      auto& node = node_arr_[it->second];
      // add new head.
      node.prev = tail;
      node.next = head_;
      node_arr_[tail].next = it->second;
      node_arr_[head_].prev = it->second;
      head_ = it->second;
    } else {
      // Cache is full, remove the tail.
      size_t res = table_.erase(string_view(node_arr_[tail].key));
      DCHECK(res == 1);

      it->second = tail;

      DCHECK_EQ(table_.size(), node_arr_.size());
    }

    auto& node = node_arr_[it->second];
    node.key = it->first;  // reference the key. We need it to erase the key referencing tail above.
    node.count = value;
  } else {  // not inserted.
    auto& node = node_arr_[it->second];
    node.count = value;
  }

  if (it->second != head_) {  // bump up to head.
    BumpToHead(it->second);
  }
}

void SimpleLruCounter::BumpToHead(uint32_t index) {
  DCHECK_LT(index, node_arr_.size());
  DCHECK_NE(index, head_);

  unsigned tail = node_arr_[head_].prev;
  if (index == tail) {
    head_ = index;  // just shift the whole cycle.
    return;
  }

  auto& node = node_arr_[index];

  DCHECK(node.prev != node.next);

  node_arr_[node.prev].next = node.next;
  node_arr_[node.next].prev = node.prev;
  node.prev = tail;
  node.prev = head_;
  head_ = index;
}
};  // namespace dfly
