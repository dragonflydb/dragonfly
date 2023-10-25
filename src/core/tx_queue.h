// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <cstdint>
#include <functional>
#include <variant>
#include <vector>

namespace dfly {

class Transaction;

// TxQueue implemmented as a circular doubly-linked list.
class TxQueue {
  void Link(uint32_t p, uint32_t n) {
    uint32_t next = vec_[p].next;
    vec_[n].next = next;
    vec_[n].prev = p;
    vec_[p].next = n;
    vec_[next].prev = n;
  }

 public:
  // uint64_t is used for unit-tests.
  using ValueType = std::variant<Transaction*, uint64_t>;
  using Iterator = uint32_t;
  enum { kEnd = Iterator(-1) };

  TxQueue(std::function<uint64_t(const Transaction*)> score_fun = nullptr);

  // returns iterator to that item the list
  Iterator Insert(Transaction* t);

  Iterator Insert(uint64_t val);
  void Remove(Iterator);

  ValueType At(Iterator it) const {
    switch (vec_[it].tag) {
      case TRANS_TAG:
        return vec_[it].u.trans;
      case UINT_TAG:
        return vec_[it].u.uval;
    }
    return 0u;
  }

  ValueType Front() const {
    return At(head_);
  }

  void PopFront() {
    Remove(head_);
  }

  size_t size() const {
    return size_;
  }

  bool Empty() const {
    return size_ == 0;
  }

  //! returns the score of the tail record. Can be called only if !Empty().
  uint64_t TailScore() const {
    return Rank(vec_[vec_[head_].prev]);
  }

  //! returns the score of the head record. Can be called only if !Empty().
  uint64_t HeadScore() const {
    return Rank(vec_[head_]);
  }

  //! Can be called only if !Empty().
  Iterator Head() const {
    return head_;
  }

  // Returns the next iterator, it's circular so it always returns a valid
  // iterator. Can be called only if !Empty().
  Iterator Next(Iterator it) const {
    return vec_[it].next;
  }

 private:
  enum { TRANS_TAG = 0, UINT_TAG = 11, FREE_TAG = 12 };

  void Grow();
  void LinkFree(uint64_t rank);

  struct QRecord {
    union {
      Transaction* trans;
      uint64_t uval;
    } u;

    uint32_t tag : 8;
    uint32_t next : 24;
    uint32_t prev;

    QRecord() : tag(FREE_TAG), prev(kEnd) {
    }
  };

  static_assert(sizeof(QRecord) == 16, "");

  uint64_t Rank(const QRecord& r) const;

  std::function<uint64_t(const Transaction*)> score_fun_;
  std::vector<QRecord> vec_;
  uint32_t next_free_ = 0, head_ = kEnd;
  size_t size_ = 0;

  TxQueue(const TxQueue&) = delete;
};

}  // namespace dfly
