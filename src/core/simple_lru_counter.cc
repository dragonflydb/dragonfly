// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/simple_lru_counter.h"

#include "base/logging.h"

namespace dfly {

using namespace std;

SimpleLruCounter::SimpleLruCounter(size_t capacity) : head_(0), grow_size_(capacity) {
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

optional<uint64_t> SimpleLruCounter::GetPrev(string_view key) const {
  auto it = table_.find(key);
  if (it == table_.end()) {
    return nullopt;
  }
  const auto& node = node_arr_[it->second];

  DCHECK_EQ(node.key, key);
  const auto& node_prev = node_arr_[node.prev];

  return node_prev.count;
}

optional<uint64_t> SimpleLruCounter::GetLast() const {
  if (table_.size() == 0) {
    return nullopt;
  }
  unsigned tail = node_arr_[head_].prev;
  return node_arr_[tail].count;
}

void SimpleLruCounter::Put(string_view key, uint64_t value, Position position) {
  auto [it, inserted] = table_.emplace(key, table_.size());
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

    node.key = it->first;  // reference the key. We need it to erase the key referencing tail above.
    node.count = value;

    if (position == Position::kHead) {
      head_ = it->second;
    }
  } else {  // not inserted.
    auto& node = node_arr_[it->second];
    node.count = value;
    MoveToPosition(it->second, position);
  }
}

void SimpleLruCounter::Remove(std::string_view key) {
  auto it = table_.find(key);
  if (it == table_.end()) {
    return;
  }
  uint32_t remove_index = it->second;
  auto& node = node_arr_[remove_index];

  // remove from list
  node_arr_[node.prev].next = node.next;
  node_arr_[node.next].prev = node.prev;

  // remove key from table.
  if (remove_index == head_) {
    head_ = node.next;
  }
  table_.erase(key);

  if (table_.size() == remove_index) {
    return;  // if the removed item was the last in the node array nothing else to do.
  }

  // move last item from node array to the removed index
  uint32_t move_index = table_.size();
  auto& node_to_move = node_arr_[move_index];
  it = table_.find(string_view(node_to_move.key));
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

void SimpleLruCounter::MoveToPosition(uint32_t index, Position position) {
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
