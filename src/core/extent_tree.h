// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/btree_map.h>
#include <absl/container/btree_set.h>

#include <optional>

namespace dfly {

// represents a tree of disjoint extents.
// check-fails if overlapping ranges are added.
// automatically handles union of the consequent ranges that are added to the tree.
class ExtentTree {
 public:
  void Add(size_t start, size_t len);

  // in case of success, returns (start, end) pair, where (end-start) >= len and
  // start is aligned by align.
  std::optional<std::pair<size_t, size_t>> GetRange(size_t len, size_t align);

 private:
  absl::btree_map<size_t, size_t> extents_;                 // start -> end).
  absl::btree_set<std::pair<size_t, size_t>> len_extents_;  // (length, start)
};

}  // namespace dfly
