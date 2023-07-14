// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "core/segment_allocator.h"

#include <mimalloc-types.h>
#include <stdint.h>

#include <ostream>

#include "absl/hash/hash.h"
#include "glog/logging.h"

constexpr size_t kSegmentSize = MI_SEGMENT_SIZE;

// mimalloc uses 32MiB segments and we might need change this code if it changes.
static_assert(kSegmentSize == 1 << 25);

namespace dfly {

SegmentAllocator::SegmentAllocator(mi_heap_t* heap) : heap_(heap) {
}

void SegmentAllocator::ValidateMapSize() {
  if (address_table_.size() > 1u << 12) {
    // This can happen if we restrict dragonfly to small number of threads on high-memory machine,
    // for example.
    LOG(WARNING) << "address_table_ map is growing too large: " << address_table_.size();
  }
}

}  // namespace dfly
