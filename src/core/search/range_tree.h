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
  // Same as Range, but returns the blocks that contain the results.
  absl::InlinedVector<const RangeBlock*, 5> RangeBlocks(double l, double r) const;

  RangeResult GetAllDocIds() const;
  // Returns all blocks in the tree.
  absl::InlinedVector<const RangeBlock*, 5> GetAllBlocks() const;

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

/* This iterator filters out entries that are not in the range [l, r].
   It is used to iterate over the RangeBlock and return only the entries
   that are within the specified range.
   The iterator is initialized with a range [l, r] and will skip entries
   that are outside this range. */
class RangeFilterIterator : public MergeableIterator {
 private:
  static constexpr DocId kInvalidDocId = std::numeric_limits<DocId>::max();

  using RangeBlock = RangeTree::RangeBlock;
  using BaseIterator = RangeBlock::BlockListIterator;

 public:
  using iterator_category = BaseIterator::iterator_category;
  using difference_type = BaseIterator::difference_type;
  using value_type = DocId;
  using pointer = value_type*;
  using reference = value_type&;

  RangeFilterIterator(BaseIterator begin, BaseIterator end, double l, double r);

  value_type operator*() const;

  RangeFilterIterator& operator++();

  void SeakGE(DocId min_doc_id) override;

  bool operator==(const RangeFilterIterator& other) const;
  bool operator!=(const RangeFilterIterator& other) const;

  bool HasReachedEnd() const;

 private:
  void SkipInvalidEntries(DocId last_id);

  bool InRange(BaseIterator it) const;

  double l_, r_;
  BaseIterator current_, end_;
};

RangeFilterIterator MakeBegin(const RangeTree::RangeBlock& block, double l, double r);
RangeFilterIterator MakeEnd(const RangeTree::RangeBlock& block, double l, double r);

/* Separate class for merging results from a single RangeBlock.
   It provides an iterator interface to iterate over the entries in the block
   that are within the specified range [l, r].
   This is used when the result of a range query is contained within a single block.

   It is needed to avoid unnecessary complexity in the RangeResult class,
   which can handle both single and multiple blocks.
   It provides better performance and clarity when dealing with single block results. */
class SingleBlockRangeResult {
 public:
  SingleBlockRangeResult(const RangeTree::RangeBlock* block, double l, double r);

  RangeFilterIterator begin() const;
  RangeFilterIterator end() const;

  size_t size() const;

 private:
  double l_;
  double r_;
  const RangeTree::RangeBlock* block_ = nullptr;
};

/* Separate class for merging results from two RangeBlocks.
   It provides an iterator interface to iterate over the entries in both blocks
   that are within the specified range [l, r].
   It automatically merges the results from both blocks and provides a unified view.
   This is used when the result of a range query spans two blocks.

   It provides a more efficient way to handle results that span multiple blocks,
   avoiding unnecessary complexity in the RangeResult class.
   TODO: Implement efficient merging for more than two blocks and remove this class. */
class TwoBlocksRangeResult {
 public:
  TwoBlocksRangeResult(const RangeTree::RangeBlock* left_block,
                       const RangeTree::RangeBlock* right_block, double l, double r);

  size_t size() const;

  class MergingIterator : public MergeableIterator {
   private:
    static constexpr DocId kInvalidDocId = std::numeric_limits<DocId>::max();

   public:
    using iterator_category = RangeFilterIterator::iterator_category;
    using difference_type = RangeFilterIterator::difference_type;
    using value_type = RangeFilterIterator::value_type;
    using pointer = RangeFilterIterator::pointer;
    using reference = RangeFilterIterator::reference;

    MergingIterator(RangeFilterIterator l, RangeFilterIterator r);

    value_type operator*() const;

    MergingIterator& operator++();

    void SeakGE(DocId min_doc_id) override;

    bool operator==(const MergingIterator& other) const;
    bool operator!=(const MergingIterator& other) const;

   private:
    void InitializeMin();

    DocId current_min_ = kInvalidDocId;
    RangeFilterIterator l_;
    RangeFilterIterator r_;
  };

  MergingIterator begin() const;
  MergingIterator end() const;

 private:
  double l_;
  double r_;
  const RangeTree::RangeBlock* left_block_ = nullptr;
  const RangeTree::RangeBlock* right_block_ = nullptr;
};

/* Represent the result of a range query on the RangeTree.
   It can contain results from a single block, two blocks, or several blocks.
   Several blocks are merged into a single result, which is represented by
   vector<DocId>.

   TODO: Implement efficient merging for more than two blocks */
class RangeResult {
 private:
  using RangeBlockPointer = const RangeTree::RangeBlock*;
  using RangeBlockIterator = RangeTree::RangeBlock::BlockListIterator;

  using DocsList = std::vector<DocId>;
  using Variant = std::variant<DocsList, SingleBlockRangeResult, TwoBlocksRangeResult>;

 public:
  RangeResult() = default;

  explicit RangeResult(std::vector<DocId> doc_ids);
  explicit RangeResult(absl::InlinedVector<RangeBlockPointer, 5> blocks);
  RangeResult(absl::InlinedVector<RangeBlockPointer, 5> blocks, double l, double r);

  std::vector<DocId> Take();

  Variant& GetResult();
  const Variant& GetResult() const;

 private:
  Variant result_;
};

// Implementation
/******************************************************************/
inline RangeFilterIterator::RangeFilterIterator(BaseIterator begin, BaseIterator end, double l,
                                                double r)
    : l_(l), r_(r), current_(begin), end_(end) {
  SkipInvalidEntries(kInvalidDocId);
}

inline RangeFilterIterator::value_type RangeFilterIterator::operator*() const {
  return (*current_).first;
}

inline RangeFilterIterator& RangeFilterIterator::operator++() {
  const DocId last_id = (*current_).first;
  ++current_;
  SkipInvalidEntries(last_id);
  return *this;
}

inline void RangeFilterIterator::SeakGE(DocId min_doc_id) {
  while (current_ != end_ && (!InRange(current_) || (*current_).first < min_doc_id)) {
    ++current_;
  }
}

inline bool RangeFilterIterator::operator==(const RangeFilterIterator& other) const {
  return current_ == other.current_;
}

inline bool RangeFilterIterator::operator!=(const RangeFilterIterator& other) const {
  return current_ != other.current_;
}

inline bool RangeFilterIterator::HasReachedEnd() const {
  return current_ == end_;
}

inline void RangeFilterIterator::SkipInvalidEntries(DocId last_id) {
  // Faster than using std::find_if
  while (current_ != end_ && (!InRange(current_) || (*current_).first == last_id)) {
    ++current_;
  }
}

inline bool RangeFilterIterator::InRange(BaseIterator it) const {
  return l_ <= (*it).second && (*it).second <= r_;
}

inline RangeFilterIterator MakeBegin(const RangeTree::RangeBlock& block, double l, double r) {
  return {block.begin(), block.end(), l, r};
}

inline RangeFilterIterator MakeEnd(const RangeTree::RangeBlock& block, double l, double r) {
  return {block.end(), block.end(), l, r};
}

inline SingleBlockRangeResult::SingleBlockRangeResult(const RangeTree::RangeBlock* block, double l,
                                                      double r)
    : l_(l), r_(r), block_(block) {
  DCHECK(block_ != nullptr);
}

inline RangeFilterIterator SingleBlockRangeResult::begin() const {
  return MakeBegin(*block_, l_, r_);
}

inline RangeFilterIterator SingleBlockRangeResult::end() const {
  return MakeEnd(*block_, l_, r_);
}

inline size_t SingleBlockRangeResult::size() const {
  return block_->Size();
}

inline TwoBlocksRangeResult::TwoBlocksRangeResult(const RangeTree::RangeBlock* left_block,
                                                  const RangeTree::RangeBlock* right_block,
                                                  double l, double r)
    : l_(l), r_(r), left_block_(left_block), right_block_(right_block) {
  DCHECK(left_block_ != nullptr);
  DCHECK(right_block_ != nullptr);
}

inline size_t TwoBlocksRangeResult::size() const {
  return left_block_->Size() + right_block_->Size();
}

inline TwoBlocksRangeResult::MergingIterator::MergingIterator(RangeFilterIterator l,
                                                              RangeFilterIterator r)
    : l_(std::move(l)), r_(std::move(r)) {
  InitializeMin();
}

inline TwoBlocksRangeResult::MergingIterator::value_type
TwoBlocksRangeResult::MergingIterator::operator*() const {
  return current_min_;
}

inline TwoBlocksRangeResult::MergingIterator& TwoBlocksRangeResult::MergingIterator::operator++() {
  auto increase_iterator = [&](RangeFilterIterator& it) {
    ++it;
    current_min_ = !it.HasReachedEnd() ? *it : std::numeric_limits<DocId>::max();
  };

  if (l_.HasReachedEnd()) {
    increase_iterator(r_);
  } else if (r_.HasReachedEnd()) {
    increase_iterator(l_);
  } else {
    DCHECK(!l_.HasReachedEnd() && !r_.HasReachedEnd());
    if (*l_ == current_min_) {
      ++l_;
    }
    if (*r_ == current_min_) {
      ++r_;
    }
    InitializeMin();
  }

  return *this;
}

inline void TwoBlocksRangeResult::MergingIterator::SeakGE(DocId min_doc_id) {
  l_.SeakGE(min_doc_id);
  r_.SeakGE(min_doc_id);
  InitializeMin();
}

inline bool TwoBlocksRangeResult::MergingIterator::operator==(
    const TwoBlocksRangeResult::MergingIterator& other) const {
  return l_ == other.l_ && r_ == other.r_;
}

inline bool TwoBlocksRangeResult::MergingIterator::operator!=(
    const TwoBlocksRangeResult::MergingIterator& other) const {
  return !(*this == other);
}

inline void TwoBlocksRangeResult::MergingIterator::InitializeMin() {
  DocId left_value = !l_.HasReachedEnd() ? *l_ : std::numeric_limits<DocId>::max();
  DocId right_value = !r_.HasReachedEnd() ? *r_ : std::numeric_limits<DocId>::max();
  current_min_ = std::min(left_value, right_value);
}

inline TwoBlocksRangeResult::MergingIterator TwoBlocksRangeResult::begin() const {
  return MergingIterator{MakeBegin(*left_block_, l_, r_), MakeBegin(*right_block_, l_, r_)};
}

inline TwoBlocksRangeResult::MergingIterator TwoBlocksRangeResult::end() const {
  return MergingIterator{MakeEnd(*left_block_, l_, r_), MakeEnd(*right_block_, l_, r_)};
}

inline RangeResult::Variant& RangeResult::GetResult() {
  return result_;
}

inline const RangeResult::Variant& RangeResult::GetResult() const {
  return result_;
}

}  // namespace dfly::search
