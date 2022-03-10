// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <absl/container/flat_hash_map.h>
#include <mimalloc.h>

/***
 * This class is tightly coupled with mimalloc segment allocation logic and is designed to provide
 * a compact pointer representation (4bytes ptr) over 64bit address space that gives you
 * 32GB of allocations with option to extend it to 32*256GB if needed.
 *
 */

namespace dfly {

/**
 * @brief Tightly coupled with mi_malloc 2.x implementation.
 *        Fetches 8MB segment pointers from the allocated pointers.
 *        Provides own indexing of small pointers to real address space using the segment ptrs/
 */

class SegmentAllocator {
  static constexpr uint32_t kSegmentIdBits = 12;
  static constexpr uint32_t kSegmentIdMask = (1 << kSegmentIdBits) - 1;
  static constexpr uint64_t kSegmentAlignMask = ~((1 << 23) - 1);

 public:
  using Ptr = uint32_t;

  SegmentAllocator(mi_heap_t* heap);

  uint8_t* Translate(Ptr p) const {
    return address_table_[p & kSegmentIdMask] + Offset(p);
  }

  std::pair<Ptr, uint8_t*> Allocate(uint32_t size);

  void Free(Ptr ptr) {
    void* p = Translate(ptr);
    used_ -= mi_usable_size(p);
    mi_free(p);
  }

  mi_heap_t* heap() {
    return heap_;
  }

  size_t used() const { return used_; }

 private:
  static uint32_t Offset(Ptr p) {
    return (p >> kSegmentIdBits) * 8;
  }

  void ValidateMapSize();

  std::vector<uint8_t*> address_table_;
  absl::flat_hash_map<uint64_t, uint16_t> rev_indx_;
  mi_heap_t* heap_;
  size_t used_ = 0;
};

inline auto SegmentAllocator::Allocate(uint32_t size) -> std::pair<Ptr, uint8_t*> {
  uint64_t ptr = (uint64_t)mi_heap_malloc(heap_, size);
  uint64_t seg_ptr = ptr & kSegmentAlignMask;

  // could be speed up using last used seg_ptr.
  auto [it, inserted] = rev_indx_.emplace(seg_ptr, address_table_.size());
  if (inserted) {
    ValidateMapSize();
    address_table_.push_back((uint8_t*)seg_ptr);
  }

  Ptr res = (((ptr - seg_ptr) / 8) << kSegmentIdBits) | it->second;
  used_ += mi_good_size(size);

  return std::make_pair(res, (uint8_t*)ptr);
}

}  // namespace dfly
