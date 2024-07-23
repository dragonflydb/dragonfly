// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "core/compact_object.h"

namespace dfly {

// Used to store "cold" items before erasing them from RAM.
//
class CoolQueue {
 public:
  ~CoolQueue();

  bool Empty() const {
    return head_ == nullptr;
  }

  detail::TieredColdRecord* PushFront(uint16_t db_index, uint64_t key_hash, uint32_t page_index,
                                      CompactObj obj);

  // The ownership is passed to the caller. The record must be deleted with
  // CompactObj::DeleteMR<detail::TieredColdRecord>.
  detail::TieredColdRecord* PopBack();

  CompactObj Erase(detail::TieredColdRecord* record);

  size_t UsedMemory() const;

 private:
  detail::TieredColdRecord* head_ = nullptr;
  detail::TieredColdRecord* tail_ = nullptr;
  size_t used_memory_ = 0;
};

}  // namespace dfly
