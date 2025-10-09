// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "core/detail/listpack_wrap.h"

#include "server/container_utils.h"

extern "C" {
#include "redis/listpack.h"
}

namespace dfly::detail {

ListpackWrap::Iterator::Iterator(uint8_t* lp, uint8_t* ptr)
    : lp_{lp}, ptr_{ptr}, next_ptr_{nullptr} {
  static_assert(sizeof(intbuf_[0]) >= LP_INTBUF_SIZE);  // to avoid header dependency
  Read();
}

ListpackWrap::Iterator& ListpackWrap::Iterator::operator++() {
  ptr_ = next_ptr_;
  Read();
  return *this;
}

void ListpackWrap::Iterator::Read() {
  if (!ptr_)
    return;

  using container_utils::LpGetView;
  key_v_ = LpGetView(ptr_, intbuf_[0]);
  next_ptr_ = lpNext(lp_, ptr_);
  value_v_ = LpGetView(next_ptr_, intbuf_[1]);
  next_ptr_ = lpNext(lp_, next_ptr_);
}

ListpackWrap::Iterator ListpackWrap::Find(std::string_view key) const {
  uint8_t* ptr = lpFind(lp_, lpFirst(lp_), (unsigned char*)key.data(), key.size(), 1);
  return Iterator{lp_, ptr};
}

size_t ListpackWrap::size() const {
  return lpLength(lp_) / 2;
}

ListpackWrap::Iterator ListpackWrap::begin() const {
  return Iterator{lp_, lpFirst(lp_)};
}

ListpackWrap::Iterator ListpackWrap::end() const {
  return Iterator{lp_, nullptr};
}

bool ListpackWrap::Iterator::operator==(const Iterator& other) const {
  return lp_ == other.lp_ && ptr_ == other.ptr_;
}
}  // namespace dfly::detail
