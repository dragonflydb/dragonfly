// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "core/segment_allocator.h"

#include "base/logging.h"

namespace dfly {

SegmentAllocator::SegmentAllocator(mi_heap_t* heap) : heap_(heap) {
}

void SegmentAllocator::ValidateMapSize() {
  CHECK_LT(address_table_.size(), 1u << 12)
      << "TODO: to monitor address_table_ map, it should not grow to such sizes";

  // TODO: we should learn how large this maps can grow for very large databases.
  // We should learn if mimalloc drops (deallocates) segments and we need to perform GC
  // to protect ourselves from bloated address table.
}

}  // namespace dfly
