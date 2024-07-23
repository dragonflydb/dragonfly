// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "src/core/cool_queue.h"

#include "base/logging.h"

namespace dfly {

CoolQueue::~CoolQueue() {
  while (!Empty()) {
    auto* record = PopBack();
    CompactObj::DeleteMR<detail::TieredColdRecord>(record);
  }
}

detail::TieredColdRecord* CoolQueue::PushFront(uint16_t db_index, uint64_t key_hash,
                                               uint32_t page_index, CompactObj obj) {
  detail::TieredColdRecord* record = CompactObj::AllocateMR<detail::TieredColdRecord>();
  record->key_hash = key_hash;
  record->db_index = db_index;
  record->page_index = page_index;
  record->value = std::move(obj);

  record->next = head_;
  if (head_) {
    head_->prev = record;
  } else {
    DCHECK(tail_ == nullptr);
    tail_ = record;
  }
  head_ = record;
  used_memory_ += (sizeof(detail::TieredColdRecord) + record->value.MallocUsed());
  return record;
}

detail::TieredColdRecord* CoolQueue::PopBack() {
  auto* res = tail_;
  if (tail_) {
    auto* prev = tail_->prev;
    tail_->prev = nullptr;
    if (prev) {
      prev->next = nullptr;
    } else {
      DCHECK(tail_ == head_);
      head_ = nullptr;
    }
    tail_ = prev;
    used_memory_ -= (sizeof(detail::TieredColdRecord) + res->value.MallocUsed());
  }
  return res;
}

CompactObj CoolQueue::Erase(detail::TieredColdRecord* record) {
  DCHECK(record);

  if (record == tail_) {
    PopBack();
  } else {
    used_memory_ -= (sizeof(detail::TieredColdRecord) + record->value.MallocUsed());
    record->next->prev = record->prev;
    if (record->prev) {
      record->prev->next = record->next;
    } else {
      DCHECK(record == head_);
      head_ = record->next;
    }
  }

  CompactObj res = std::move(record->value);
  CompactObj::DeleteMR<detail::TieredColdRecord>(record);
  return res;
}
}  // namespace dfly
