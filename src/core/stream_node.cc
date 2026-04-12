// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.

#include "core/stream_node.h"

#include "base/logging.h"

extern "C" {
#include "redis/listpack.h"
#include "redis/zmalloc.h"
}

namespace dfly {

uint8_t* StreamNodeObj::GetListpack() const {
  DCHECK(IsRaw());
  return Ptr();
}

uint32_t StreamNodeObj::UncompressedSize() const {
  DCHECK(IsRaw());
  return static_cast<uint32_t>(lpBytes(Ptr()));
}

void StreamNodeObj::Free() const {
  zfree(Ptr());
}

size_t StreamNodeObj::MallocSize() const {
  return zmalloc_size(Ptr());
}

}  // namespace dfly
