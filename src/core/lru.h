// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <absl/container/node_hash_map.h>

#include <optional>

#include "base/logging.h"
#include "base/pmr/memory_resource.h"

namespace dfly {

enum class Position {
  kHead,
  kTail,
};

template <typename T> class Lru {
  struct Node {
    const T* data_ptr;

    uint32_t prev;
    uint32_t next;

    Node() : prev(0), next(0) {
    }
  };

 public:
  explicit Lru(uint32_t capacity, PMR_NS::memory_resource* mr) : table_(mr), node_arr_(mr) {
    CHECK_GT(capacity, 1u);
    node_arr_.reserve(capacity);
  }

  // Get prev item. In case item is head return tail.
  std::optional<T> GetPrev(const T& data) const;
  std::optional<T> GetTail() const;
  std::optional<T> GetHead() const;

  void Put(const T& data, Position position = Position::kHead);
  bool Remove(const T& data);

  size_t Size() const {
    DCHECK_EQ(table_.size(), node_arr_.size());
    return table_.size();
  }

 private:
  void MoveToPosition(uint32_t index, Position position);
  using AllocatorType = PMR_NS::polymorphic_allocator<std::pair<T, uint32_t>>;
  absl::node_hash_map<T, uint32_t, absl::Hash<T>, std::equal_to<>, AllocatorType>
      table_;  // map from item to index in node arr
  std::vector<Node, PMR_NS::polymorphic_allocator<Node>> node_arr_;
  uint32_t head_ = 0;
};

template <typename T> std::optional<T> Lru<T>::GetPrev(const T& data) const {
  auto it = table_.find(data);
  if (it == table_.end()) {
    return std::nullopt;
  }
  DCHECK_GT(node_arr_.size(), it->second);
  const auto& node = node_arr_[it->second];

  DCHECK_EQ(node.data_ptr, &it->first);
  const auto& node_prev = node_arr_[node.prev];

  return *node_prev.data_ptr;
}

template <typename T> std::optional<T> Lru<T>::GetTail() const {
  if (table_.size() == 0) {
    return std::nullopt;
  }
  unsigned tail = node_arr_[head_].prev;
  return *node_arr_[tail].data_ptr;
}

template <typename T> std::optional<T> Lru<T>::GetHead() const {
  if (table_.size() == 0) {
    return std::nullopt;
  }
  return *node_arr_[head_].data_ptr;
}

template <typename T> void Lru<T>::Put(const T& data, Position position) {
  DCHECK_EQ(table_.size(), node_arr_.size());
  auto [it, inserted] = table_.emplace(data, table_.size());
  if (inserted) {
    unsigned tail = 0;
    if (node_arr_.size() > 0) {
      tail = node_arr_[head_].prev;
    }

    Node node;
    // add new item between head and tail.
    node.prev = tail;
    node.next = head_;
    node_arr_[tail].next = it->second;
    node_arr_[head_].prev = it->second;

    node.data_ptr = &(it->first);
    node_arr_.push_back(node);

    if (position == Position::kHead) {
      head_ = it->second;
    }
  } else {  // not inserted.
    MoveToPosition(it->second, position);
  }
}

template <typename T> bool Lru<T>::Remove(const T& data) {
  auto it = table_.find(data);
  if (it == table_.end()) {
    return false;
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
  table_.erase(it);

  if (table_.size() == remove_index) {
    node_arr_.pop_back();
    DCHECK_EQ(table_.size(), node_arr_.size());
    return true;  // if the removed item was the last in the node array nothing else to do.
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
  node_arr_.pop_back();

  if (head_ == move_index) {
    head_ = remove_index;
  }
  DCHECK_EQ(table_.size(), node_arr_.size());
  return true;
}

template <typename T> void Lru<T>::MoveToPosition(uint32_t index, Position position) {
  DCHECK_LT(index, node_arr_.size());
  uint32_t tail = node_arr_[head_].prev;
  uint32_t curr_node_index = position == Position::kHead ? head_ : tail;
  if (index == curr_node_index) {  // the index is already head/tail. nothing to change.
    return;
  }

  auto& node = node_arr_[index];
  CHECK_NE(node.prev, node.next);

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
