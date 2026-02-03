// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "core/tx_queue.h"

#include "base/logging.h"

namespace dfly {

TxQueue::TxQueue(std::function<uint64_t(const Transaction*)> sf)
    : score_fun_(sf), vec_(32) {
  for (size_t i = 0; i < vec_.size(); ++i) {
    vec_[i].next = i + 1;
  }
}

auto TxQueue::Insert(Transaction* t) -> Iterator {
  if (next_free_ >= vec_.size()) {
    Grow();
  }
  DCHECK_LT(next_free_, vec_.size());
  DCHECK_EQ(FREE_TAG, vec_[next_free_].tag);

  Iterator res = next_free_;
  vec_[next_free_].u.trans = t;
  vec_[next_free_].tag = TRANS_TAG;
  DVLOG(1) << "Insert " << next_free_ << " " << t;
  LinkFree(score_fun_(t));
  return res;
}

auto TxQueue::Insert(uint64_t val) -> Iterator {
  if (next_free_ >= vec_.size()) {
    Grow();
  }
  DCHECK_LT(next_free_, vec_.size());

  Iterator res = next_free_;

  vec_[next_free_].u.uval = val;
  vec_[next_free_].tag = UINT_TAG;

  LinkFree(val);
  return res;
}

void TxQueue::LinkFree(uint64_t weight) {
  uint32_t taken = next_free_;
  next_free_ = vec_[taken].next;

  if (size_ == 0) {
    head_ = taken;
    vec_[head_].next = vec_[head_].prev = head_;
  } else {
    uint32_t cur = vec_[head_].prev;
    while (true) {
      if (Rank(vec_[cur]) < weight) {
        Link(cur, taken);
        break;
      }
      if (cur == head_) {
        Link(vec_[head_].prev, taken);
        head_ = taken;
        break;
      }
      cur = vec_[cur].prev;
    }
  }
  ++size_;
}

void TxQueue::Grow() {
  size_t start = vec_.size();
  DVLOG(1) << "Grow from " << start << " to " << start * 2;

  vec_.resize(start * 2);
  for (size_t i = start; i < vec_.size(); ++i) {
    vec_[i].next = i + 1;
  }
}

void TxQueue::Remove(Iterator it) {
  DCHECK_GT(size_, 0u);
  DCHECK_LT(it, vec_.size());
  DCHECK_NE(FREE_TAG, vec_[it].tag);

  DVLOG(1) << "Remove " << it << " " << vec_[it].u.trans;
  Iterator next = kEnd;
  if (size_ > 1) {
    Iterator prev = vec_[it].prev;
    next = vec_[it].next;

    vec_[prev].next = next;
    vec_[next].prev = prev;
  }
  --size_;
  vec_[it].next = next_free_;
  vec_[it].tag = FREE_TAG;
  next_free_ = it;
  if (head_ == it) {
    head_ = next;
  }
}

uint64_t TxQueue::Rank(const QRecord& r) const {
  switch (r.tag) {
    case UINT_TAG:
      return r.u.uval;
    case TRANS_TAG:
      return score_fun_(r.u.trans);
  }
  return 0;
}

}  // namespace dfly
