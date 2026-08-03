// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/oah_ptr.h"

#include "core/oah_entry.h"
#include "core/oah_pair.h"

namespace dfly {

template <typename Entry>
ssize_t OAHPtr<Entry>::ReallocIfNeeded(PageUsage* page_usage, bool* realloced) {
  *realloced = false;
  if (Empty())
    return 0;
  if (!IsVector())
    return Entry(*slot_).ReallocIfNeeded(page_usage, realloced);

  ssize_t obj_alloc_delta = 0;
  Vector vec = AsVector();
  for (TaggedPtr& cell : vec) {
    Entry entry(cell);
    if (entry) {
      bool inner_moved = false;
      obj_alloc_delta += entry.ReallocIfNeeded(page_usage, &inner_moved);
      *realloced |= inner_moved;
    }
  }
  if (page_usage->IsPageForObjectUnderUtilized(Raw())) {
    vec.Realloc();
    *realloced = true;
  }
  return obj_alloc_delta;
}

// Keep these explicit instantiations in sync with the OAH table entry types.
template ssize_t OAHPtr<OAHEntry>::ReallocIfNeeded(PageUsage*, bool*);

template ssize_t OAHPtr<OAHPair>::ReallocIfNeeded(PageUsage*, bool*);

}  // namespace dfly
