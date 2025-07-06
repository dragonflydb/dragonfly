// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/btree_map.h>

#include <memory>
#include <queue>
#include <vector>

#include "base/pmr/memory_resource.h"
#include "core/search/base.h"
#include "core/search/block_list.h"

namespace dfly::search {
class RangeResult;

/* RangeTree is an index structure for numeric fields that allows efficient range queries.
   It maps disjoint numeric ranges (e.g., [0, 5), [5, 10), [10, 15), ...) to sorted sets of document
   IDs.

   Internally, it uses absl::btree_map<std::pair<double, double>, RangeBlock>, where each key
   represents a numeric value range, and the corresponding RangeBlock (similar to std::vector)
   stores (DocId, value) pairs, sorted by DocId.

   The parameter `max_range_block_size_` defines the maximum number of entries in a single
   RangeBlock. When a block exceeds this limit, it is split into two to maintain balanced
   performance.
*/
class RangeTree {
 public:
  friend class RangeResult;

  using RangeNumber = double;
  using Key = RangeNumber;
  using Entry = std::pair<DocId, double>;
  using RangeBlock = BlockList<SortedVector<Entry>>;
  using Map = absl::btree_map<Key, RangeBlock, std::less<Key>,
                              PMR_NS::polymorphic_allocator<std::pair<const Key, RangeBlock>>>;

  static constexpr size_t kMaxRangeBlockSize = 500000;
  static constexpr size_t kBlockSize = 400;

  explicit RangeTree(PMR_NS::memory_resource* mr, size_t max_range_block_size = kMaxRangeBlockSize);

  // Adds a document with a value to the index.
  void Add(DocId id, double value);

  // Removes a document with a value from the index.
  void Remove(DocId id, double value);

  // Returns all documents with values in the range [l, r].
  RangeResult Range(double l, double r) const;

  RangeResult GetAllDocIds() const;

 private:
  Map::iterator FindRangeBlock(double value);
  Map::const_iterator FindRangeBlock(double value) const;

  void SplitBlock(Map::iterator it);

  // Used for DCHECKs
  bool TreeIsInCorrectState() const;

 private:
  // The maximum size of a range block. If a block exceeds this size, it will be split
  size_t max_range_block_size_;
  Map entries_;
};

class RangeResult {
 private:
  using RangeBlockPointer = const RangeTree::RangeBlock*;
  using RangeBlockIterator = RangeTree::RangeBlock::BlockListIterator;

 public:
  explicit RangeResult(absl::InlinedVector<RangeBlockPointer, 5> blocks);
  RangeResult(absl::InlinedVector<RangeBlockPointer, 5> blocks, double l, double r);

  std::vector<DocId> MergeAllResults() const;

  // Used in tests
  absl::InlinedVector<RangeBlockPointer, 5> GetBlocks() const {
    return blocks_;
  }

 private:
  double l_ = -std::numeric_limits<double>::infinity();
  double r_ = std::numeric_limits<double>::infinity();
  absl::InlinedVector<RangeBlockPointer, 5> blocks_;
};

}  // namespace dfly::search
