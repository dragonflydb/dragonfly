// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "core/segment_allocator.h"

#include <mimalloc/types.h>

#include "base/logging.h"

constexpr size_t kSegmentShift = MI_SEGMENT_SHIFT;

namespace dfly {

SegmentAllocator::SegmentAllocator(mi_heap_t* heap) : heap_(heap) {
  // mimalloc uses 4MiB segments and we might need change this code if it changes.
  constexpr size_t kSegLogSpan = 32 - kSegmentIdBits + 3;
  static_assert(kSegLogSpan == kSegmentShift);
  static_assert((~kSegmentAlignMask) == (MI_SEGMENT_SIZE - 1));
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
