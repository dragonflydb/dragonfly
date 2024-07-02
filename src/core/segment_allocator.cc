// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "core/segment_allocator.h"

#include <mimalloc/types.h>

#include "base/logging.h"

namespace dfly {

SegmentAllocator::SegmentAllocator(mi_heap_t* heap) : heap_(heap) {
  // 256GB
  constexpr size_t limit = 1ULL << 35;
  static_assert((1ULL << (kSegmentIdBits + kSegmentShift)) == limit);
  // mimalloc uses 32MiB segments and we might need change this code if it changes.
  static_assert(kSegmentShift == MI_SEGMENT_SHIFT);
  static_assert((~kSegmentAlignMask) == (MI_SEGMENT_MASK));
}

void SegmentAllocator::ValidateMapSize() {
  if (address_table_.size() > (1u << kSegmentIdBits)) {
    // This can happen if we restrict dragonfly to small number of threads on high-memory machine,
    // for example.
    LOG(WARNING) << "address_table_ map is growing too large: " << address_table_.size();
  }
}

bool SegmentAllocator::CanAllocate() {
  return address_table_.size() < (1u << kSegmentIdBits);
}

}  // namespace dfly
