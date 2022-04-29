// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/extent_tree.h"

#include "base/logging.h"

namespace dfly {

using namespace std;

// offset, len must be multiplies of 256MB.
void ExtentTree::Add(size_t start, size_t len) {
  DCHECK_GT(len, 0u);
  DCHECK_EQ(len_extents_.size(), extents_.size());

  size_t end = start + len;

  if (extents_.empty()) {
    extents_.emplace(start, end);
    len_extents_.emplace(len, start);

    return;
  }

  auto it = extents_.lower_bound(start);
  bool merged = false;

  if (it != extents_.begin()) {
    auto prev = it;
    --prev;

    DCHECK_LE(prev->second, start);
    if (prev->second == start) {  // [first, second = start, end)
      merged = true;
      len_extents_.erase(pair{prev->second - prev->first, prev->first});

      if (end == it->first) {  // [first, end = it->first, it->second)
        prev->second = it->second;
        len_extents_.erase(pair{it->second - it->first, it->first});
        extents_.erase(it);
      } else {
        prev->second = end;
      }
      len_extents_.emplace(prev->second - prev->first, prev->first);
    }
  }

  if (!merged) {
    if (end == it->first) {  // [start, end), [it->first, it->second]
      len_extents_.erase(pair{it->second - it->first, it->first});
      end = it->second;
      extents_.erase(it);
    }
    extents_.emplace(start, end);
    len_extents_.emplace(end - start, start);
  }
}

optional<pair<size_t, size_t>> ExtentTree::GetRange(size_t len, size_t align) {
  DCHECK_GT(align, 0u);
  DCHECK_EQ(0u, align & (align - 1));
  DCHECK_EQ(0u, len & (align - 1));

  auto it = len_extents_.lower_bound(pair{len, 0});
  if (it == len_extents_.end())
    return nullopt;
  size_t amask = align - 1;
  size_t aligned_start = 0;

  while (true) {
    aligned_start = it->second;
    if ((aligned_start & amask) == 0)
      break;
    size_t end = it->first + aligned_start;
    // round up to the next aligned address
    aligned_start = align + (aligned_start & (~amask));
    if (aligned_start + len <= end)
      break;
    ++it;

    if (it == len_extents_.end())
      return nullopt;
  }

  DCHECK_GE(aligned_start, it->second);
  // if we are here - we found the range starting at aligned_start.
  auto eit = extents_.find(it->second);
  DCHECK(eit != extents_.end());
  size_t end = eit->second;
  size_t range_end = aligned_start + len;

  len_extents_.erase(it);

  // we break the interval [eit->first, eit->second] to either 0, 1 or 2 intervals.
  if (aligned_start > eit->first) {
    eit->second = aligned_start;
    len_extents_.emplace(eit->second - eit->first, eit->first);
  } else {
    extents_.erase(eit);
  }

  if (aligned_start + len < end) {
    extents_.emplace(range_end, end);
    len_extents_.emplace(end - range_end, range_end);
  }

  return pair{aligned_start, range_end};
}

}  // namespace dfly
