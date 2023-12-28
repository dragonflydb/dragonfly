// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <absl/container/node_hash_map.h>

#include <optional>

#include "base/logging.h"

namespace dfly {

template <typename T> class Lru {
  struct Node {
    const T* data_ptr;

    uint32_t prev;
    uint32_t next;

    Node() : prev(0), next(0) {
    }
  };

 public:
  explicit Lru(size_t capacity) : head_(0), grow_size_(capacity) {
    CHECK_GT(capacity, 1u);
    node_arr_.resize(capacity);
  }
  ~Lru() {
  }

  std::optional<T> GetPrev(const T& data) const;
  std::optional<T> GetLast() const;
  enum class Position {
    kHead,
    kTail,
  };
  void Put(const T& data, Position position = Position::kHead);
  void Remove(const T& data);

  size_t Size() const {
    return table_.size();
  }

 private:
  void MoveToPosition(uint32_t index, Position position);

  absl::node_hash_map<T, uint32_t> table_;  // map from item to index in node arr
  std::vector<Node> node_arr_;
  uint32_t head_;
  size_t grow_size_;
};

template <typename T> std::optional<T> Lru<T>::GetPrev(const T& data) const {
  auto it = table_.find(data);
  if (it == table_.end()) {
    return std::nullopt;
  }
  const auto& node = node_arr_[it->second];

  DCHECK_EQ(node.data_ptr, &it->first);
  const auto& node_prev = node_arr_[node.prev];

  return *node_prev.data_ptr;
}

template <typename T> std::optional<T> Lru<T>::GetLast() const {
  if (table_.size() == 0) {
    return std::nullopt;
  }
  unsigned tail = node_arr_[head_].prev;
  return *node_arr_[tail].data_ptr;
}

template <typename T> void Lru<T>::Put(const T& data, Position position) {
  auto [it, inserted] = table_.emplace(data, table_.size());
  if (inserted) {
    if (it->second < node_arr_.size()) {
      node_arr_.resize(node_arr_.size() + grow_size_);
    }
    unsigned tail = node_arr_[head_].prev;  // 0 if we had 1 or 0 elements.

    auto& node = node_arr_[it->second];
    // add new item between head and tail.
    node.prev = tail;
    node.next = head_;
    node_arr_[tail].next = it->second;
    node_arr_[head_].prev = it->second;

    node.data_ptr = &(it->first);

    if (position == Position::kHead) {
      head_ = it->second;
    }
  } else {  // not inserted.
    MoveToPosition(it->second, position);
  }
}

template <typename T> void Lru<T>::Remove(const T& data) {
  auto it = table_.find(data);
  if (it == table_.end()) {
    return;
  }
  uint32_t remove_index = it->second;
  auto& node = node_arr_[remove_index];

  // remove from list
  node_arr_[node.prev].next = node.next;
  node_arr_[node.next].prev = node.prev;

  // remove item from table.
  if (remove_index == head_) {
    head_ = node.next;
  }
  table_.erase(data);

  if (table_.size() == remove_index) {
    return;  // if the removed item was the last in the node array nothing else to do.
  }

  // move last item from node array to the removed index
  uint32_t move_index = table_.size();
  auto& node_to_move = node_arr_[move_index];
  it = table_.find(*node_to_move.data_ptr);
  CHECK(it != table_.end());

  it->second = remove_index;
  // now update the next and prev to point to it
  node_arr_[node_to_move.prev].next = remove_index;
  node_arr_[node_to_move.next].prev = remove_index;

  // move the data from the node to the removed node.
  node = node_to_move;

  if (head_ == move_index) {
    head_ = remove_index;
  }
}

template <typename T> void Lru<T>::MoveToPosition(uint32_t index, Position position) {
  DCHECK_LT(index, node_arr_.size());
  uint32_t tail = node_arr_[head_].prev;
  uint32_t curr_node_index = position == Position::kHead ? head_ : tail;
  if (index == curr_node_index) {  // the index is already head/tail. nothing to change.
    return;
  }

  auto& node = node_arr_[index];
  DCHECK(node.prev != node.next);

  if (position == Position::kHead && index == tail) {
    head_ = index;  // just shift the cycle.
    return;
  }
  if (position == Position::kTail && index == head_) {
    head_ = node.next;  // just shift the cycle.
    return;
  }

  // remove from list
  node_arr_[node.prev].next = node.next;
  node_arr_[node.next].prev = node.prev;

  // update node next and prev
  node.prev = tail;
  node.next = head_;

  // update tail to point to new head
  node_arr_[tail].next = index;

  // update last head to point to new head
  node_arr_[head_].prev = index;

  if (position == Position::kHead) {
    head_ = index;
  }
}

};  // namespace dfly
