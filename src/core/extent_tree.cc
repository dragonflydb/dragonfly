// Copyright 2022, DragonflyDB authors.  All rights reserved.
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

  auto it = extents_.lower_bound(start);
  optional<size_t> prev_extent_key;

  if (it != extents_.begin()) {
    auto prev = it;
    --prev;

    DCHECK_LE(prev->second, start);
    if (prev->second == start) {  // combine with the previous extent
      size_t prev_len = prev->second - prev->first;
      CHECK_EQ(1u, len_extents_.erase(pair{prev_len, prev->first}));
      prev->second += len;
      start = prev->first;
      len += prev_len;
      prev_extent_key = prev->first;
    }
  }

  if (it != extents_.end()) {
    DCHECK_GE(it->first, start + len);
    if (start + len == it->first) {  // merge with the next extent
      size_t it_len = it->second - it->first;
      CHECK_EQ(1u, len_extents_.erase(pair{it_len, it->first}));
      extents_.erase(it);
      len += it_len;
    }
  }

  len_extents_.emplace(len, start);
  if (prev_extent_key) {
    DCHECK(extents_.find(*prev_extent_key) != extents_.end());
    extents_[*prev_extent_key] = start + len;
  } else {
    extents_.emplace(start, start + len);
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
  size_t aligned_start = it->second;
  size_t extent_end = it->first + it->second;

  while (true) {
    if ((aligned_start & amask) == 0)  // aligned
      break;

    // round up to the next aligned address
    aligned_start = (aligned_start + amask) & (~amask);

    if (aligned_start + len <= extent_end)  // check if we still inside the extent
      break;
    ++it;

    if (it == len_extents_.end())
      return nullopt;

    aligned_start = it->second;
    extent_end = it->first + it->second;
  }

  DCHECK_GE(aligned_start, it->second);

  // if we are here - we found the range starting at aligned_start.
  // now we need to possibly break the existing extent to several parts or completely
  // delete it.
  auto eit = extents_.find(it->second);
  DCHECK(eit != extents_.end());
  size_t range_end = aligned_start + len;

  len_extents_.erase(it);

  // we break the extent [eit->first, eit->second] to either 0, 1 or 2 intervals.
  if (aligned_start > eit->first) {  // do we have prefix?
    eit->second = aligned_start;
    len_extents_.emplace(eit->second - eit->first, eit->first);
  } else {
    extents_.erase(eit);
  }

  if (range_end < extent_end) {  // do we have suffix?
    extents_.emplace(range_end, extent_end);
    len_extents_.emplace(extent_end - range_end, range_end);
  }

  DCHECK_EQ(range_end - aligned_start, len);

  return pair{aligned_start, range_end};
}

}  // namespace dfly
