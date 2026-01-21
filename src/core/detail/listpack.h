// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <optional>
#include <string>
#include <string_view>

#include "core/qlist.h"

extern "C" {
#include "redis/listpack.h"
}

namespace dfly {
namespace detail {

// A listpack wrapper that provides basic list operations.
// Unfortunately, we already have a listpack wrapper in core/detail/listpack_wrap.h but
// it's more map oriented and doesn't provide the basic list operations we need here.
// TODO: to unify both wrappers into one.
class ListPack {
 public:
  explicit ListPack(uint8_t* lp = nullptr) : lp_(lp) {
  }

  size_t Size() const {
    return lpLength(lp_);
  }

  // Removes and returns an element from the specified end (HEAD or TAIL).
  std::string Pop(QList::Where where);

  // Adds an element to the specified end (HEAD or TAIL).
  void Push(std::string_view value, QList::Where where);

  // Returns the first element from the specified end without removing it.
  std::string First(QList::Where where) const;

  // Returns the element at the specified index, or std::nullopt if out of bounds.
  std::optional<std::string> At(long index) const;

  // Finds positions of an element matching the given criteria.
  std::vector<uint32_t> Pos(std::string_view element, uint32_t rank, uint32_t count,
                            uint32_t max_len, QList::Where where) const;

  // Inserts an element before or after the specified pivot element.
  bool Insert(std::string_view pivot, std::string_view elem, QList::InsertOpt insert_opt);

  // Removes up to count occurrences of elem from the specified direction.
  unsigned Remove(std::string_view elem, unsigned count, QList::Where where);

  // Replaces the element at the specified index with a new value.
  bool Replace(long index, std::string_view elem);

  // Removes count elements starting from the specified index.
  void Erase(long start, long count) {
    lp_ = lpDeleteRange(lp_, start, count);
  }

  // Returns the raw listpack pointer.
  uint8_t* GetPointer() const {
    return lp_;
  }

 private:
  static QList::Entry GetEntry(uint8_t* pos);

  uint8_t* GetFirst(QList::Where where) const {
    return (where == QList::HEAD) ? lpFirst(lp_) : lpLast(lp_);
  }

  uint8_t* lp_;
};

}  // namespace detail
}  // namespace dfly
