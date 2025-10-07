// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "core/detail/listpack_wrap.h"

#include "server/container_utils.h"

extern "C" {
#include "redis/listpack.h"
}

namespace dfly::detail {

ListpackWrap::Iterator::Iterator(uint8_t* lp, uint8_t* ptr) : lp{lp}, ptr{ptr}, next_ptr{nullptr} {
  static_assert(sizeof(intbuf[0]) >= LP_INTBUF_SIZE);  // to avoid header dependency
  Read();
}

ListpackWrap::Iterator& ListpackWrap::Iterator::operator++() {
  ptr = next_ptr;
  Read();
  return *this;
}

void ListpackWrap::Iterator::Read() {
  if (!ptr)
    return;

  using container_utils::LpGetView;
  key_v = LpGetView(ptr, intbuf[0]);
  next_ptr = lpNext(lp, ptr);
  value_v = LpGetView(next_ptr, intbuf[1]);
  next_ptr = lpNext(lp, next_ptr);
}

ListpackWrap::Iterator ListpackWrap::Find(std::string_view key) const {
  uint8_t* ptr = lpFind(lp, lpFirst(lp), (unsigned char*)key.data(), key.size(), 1);
  return Iterator{lp, ptr};
}

size_t ListpackWrap::size() const {
  return lpLength(lp) / 2;
}

ListpackWrap::Iterator ListpackWrap::begin() const {
  return Iterator{lp, lpFirst(lp)};
}

ListpackWrap::Iterator ListpackWrap::end() const {
  return Iterator{lp, nullptr};
}

bool ListpackWrap::Iterator::operator==(const Iterator& other) const {
  return lp == other.lp && ptr == other.ptr;
}
}  // namespace dfly::detail
