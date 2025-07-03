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

class RangeTree {
 private:
  friend class RangeResult;

  using RangeNumber = double;
  using Key = std::pair<RangeNumber, RangeNumber>;
  using Entry = std::pair<DocId, double>;
  using RangeBlock = BlockList<SortedVector<Entry>>;
  using Map = absl::btree_map<Key, RangeBlock, std::less<Key>,
                              PMR_NS::polymorphic_allocator<std::pair<const Key, RangeBlock>>>;

  static constexpr size_t kMaxRangeBlockSize = 500000;
  static constexpr size_t kBlockSize = 400;

 public:
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

  std::vector<DocId> MergeAllResults();

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
