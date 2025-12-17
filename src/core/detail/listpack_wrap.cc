// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "core/detail/listpack_wrap.h"

#include "base/logging.h"

extern "C" {
#include "redis/listpack.h"
}

namespace dfly::detail {

ListpackWrap::Iterator::Iterator(uint8_t* lp, uint8_t* ptr, IntBuf& intbuf)
    : lp_{lp}, ptr_{ptr}, next_ptr_{nullptr}, intbuf_(intbuf) {
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

  key_v_ = GetView(ptr_, intbuf_[0]);
  next_ptr_ = lpNext(lp_, ptr_);
  value_v_ = GetView(next_ptr_, intbuf_[1]);
  next_ptr_ = lpNext(lp_, next_ptr_);
}

ListpackWrap ListpackWrap::WithCapacity(size_t capacity) {
  return ListpackWrap{lpNew(capacity)};
}

uint8_t* ListpackWrap::GetPointer() {
  return lp_;
}

ListpackWrap::Iterator ListpackWrap::Find(std::string_view key) const {
  if (size() == 0)
    return end();

  uint8_t* ptr = lpFind(lp_, lpFirst(lp_), (unsigned char*)key.data(), key.size(), 1);
  return Iterator{lp_, ptr, intbuf_};
}

bool ListpackWrap::Delete(std::string_view key) {
  if (size() == 0)
    return false;

  uint8_t* ptr = lpFind(lp_, lpFirst(lp_), (unsigned char*)key.data(), key.size(), 1);
  if (ptr == nullptr)
    return false;

  lp_ = lpDeleteRangeWithEntry(lp_, &ptr, 2);
  return true;
}

bool ListpackWrap::Insert(std::string_view key, std::string_view value, bool skip_exists) {
  uint8_t* vptr;
  uint8_t* fptr = lpFirst(lp_);
  uint8_t* fsrc = key.empty() ? lp_ : (uint8_t*)key.data();
  // if we vsrc is NULL then lpReplace will delete the element, which is not what we want.
  // therefore, for an empty val we set it to some other valid address so that lpReplace
  // will do the right thing and encode empty string instead of deleting the element.
  uint8_t* vsrc = value.empty() ? lp_ : (uint8_t*)value.data();

  bool updated = false;
  if (fptr) {
    fptr = lpFind(lp_, fptr, fsrc, key.size(), 1);
    if (fptr) {
      if (skip_exists)
        return false;

      // Grab pointer to the value (fptr points to the field)
      vptr = lpNext(lp_, fptr);

      // Replace value
      lp_ = lpReplace(lp_, &vptr, vsrc, value.size());
      DCHECK_EQ(0u, lpLength(lp_) % 2);

      updated = true;
    }
  }

  if (!updated) {
    // Push new field/value pair onto the tail of the listpack.
    // TODO: we should at least allocate once for both elements
    lp_ = lpAppend(lp_, fsrc, key.size());
    lp_ = lpAppend(lp_, vsrc, value.size());
  }

  return !updated;
}

size_t ListpackWrap::size() const {
  return lpLength(lp_) / 2;
}

size_t ListpackWrap::DataBytes() const {
  return lpBytes(lp_);
}

ListpackWrap::Iterator ListpackWrap::begin() const {
  return Iterator{lp_, lpFirst(lp_), intbuf_};
}

ListpackWrap::Iterator ListpackWrap::end() const {
  return Iterator{lp_, nullptr, intbuf_};
}

size_t ListpackWrap::UsedBytes() const {
  return lpBytes(lp_);
}

std::string_view ListpackWrap::GetView(uint8_t* lp_it, uint8_t int_buf[]) {
  int64_t ele_len = 0;
  uint8_t* elem = lpGet(lp_it, &ele_len, int_buf);
  DCHECK(elem);
  return std::string_view{reinterpret_cast<char*>(elem), size_t(ele_len)};
}

bool ListpackWrap::Iterator::operator==(const Iterator& other) const {
  return lp_ == other.lp_ && ptr_ == other.ptr_;
}
}  // namespace dfly::detail
