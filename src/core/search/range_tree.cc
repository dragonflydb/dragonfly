// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/range_tree.h"

namespace dfly::search {

namespace {

// This iterator filters out entries that are not in the range [l, r].
// It is used to iterate over the RangeBlock and return only the entries
// that are within the specified range.
// The iterator is initialized with a range [l, r] and will skip entries
// that are outside this range.
class RangeFilterIterator {
 private:
  using RangeBlock = RangeTree::RangeBlock;
  using BaseIterator = RangeBlock::BlockListIterator;

 public:
  using iterator_category = BaseIterator::iterator_category;
  using difference_type = BaseIterator::difference_type;
  using value_type = DocId;
  using pointer = value_type*;
  using reference = value_type&;

  RangeFilterIterator(BaseIterator begin, BaseIterator end, double l, double r)
      : l_(l), r_(r), current_(begin), end_(end) {
    SkipUnvalidEntries(std::numeric_limits<DocId>::max());
  }

  value_type operator*() const {
    return (*current_).first;
  }

  RangeFilterIterator& operator++() {
    const DocId last_id = (*current_).first;
    ++current_;
    SkipUnvalidEntries(last_id);
    return *this;
  }

  bool operator==(const RangeFilterIterator& other) const {
    return current_ == other.current_;
  }

  bool operator!=(const RangeFilterIterator& other) const {
    return current_ != other.current_;
  }

  bool HasReachedEnd() const {
    return current_ == end_;
  }

 private:
  void SkipUnvalidEntries(DocId last_id) {
    // Faster than using std::find_if
    while (current_ != end_ && (!InRange(current_) || (*current_).first == last_id)) {
      ++current_;
    }
  }

  bool InRange(BaseIterator it) const {
    return l_ <= (*it).second && (*it).second <= r_;
  }

  double l_, r_;
  BaseIterator current_, end_;
};

RangeFilterIterator MakeBegin(const RangeTree::RangeBlock& block, double l, double r) {
  return {block.begin(), block.end(), l, r};
}

RangeFilterIterator MakeEnd(const RangeTree::RangeBlock& block, double l, double r) {
  return {block.end(), block.end(), l, r};
}

std::vector<DocId> MergeSingleBlock(const RangeTree::RangeBlock& block, double l, double r) {
  std::vector<DocId> result;
  result.reserve(block.Size() / 16);
  std::copy(MakeBegin(block, l, r), MakeEnd(block, l, r), std::back_inserter(result));
  return result;
}

std::vector<DocId> MergeTwoBlocks(const RangeTree::RangeBlock& left_block,
                                  const RangeTree::RangeBlock& right_block, double l, double r) {
  std::vector<DocId> result;
  result.reserve(left_block.Size() / 3 + right_block.Size() / 3);
  std::set_union(MakeBegin(left_block, l, r), MakeEnd(left_block, l, r),
                 MakeBegin(right_block, l, r), MakeEnd(right_block, l, r),
                 std::back_inserter(result));
  return result;
}

template <typename MapT> auto FindRangeBlockImpl(MapT& entries, double value) {
  DCHECK(!entries.empty());

  auto it = entries.lower_bound(value);
  if (it != entries.begin() && (it == entries.end() || it->first > value)) {
    // TODO: remove this, we do log N here
    // we can use negative left bouding to find the block
    --it;  // Move to the block that contains the value
  }

  DCHECK(it != entries.end() && it->first <= value);
  return it;
}
}  // namespace

RangeTree::RangeTree(PMR_NS::memory_resource* mr, size_t max_range_block_size)
    : max_range_block_size_(max_range_block_size), entries_(mr) {
  // TODO: at the beggining create more blocks
  entries_.insert({{-std::numeric_limits<RangeNumber>::infinity()},
                   RangeBlock{entries_.get_allocator().resource(), max_range_block_size_}});
}

void RangeTree::Add(DocId id, double value) {
  DCHECK(std::isfinite(value));

  auto it = FindRangeBlock(value);
  RangeBlock& block = it->second;

  auto insert_result = block.Insert({id, value});
  LOG_IF(ERROR, !insert_result) << "RangeTree: Failed to insert id: " << id << ", value: " << value;

  if (block.Size() <= max_range_block_size_) {
    return;
  }

  SplitBlock(std::move(it));
}

void RangeTree::Remove(DocId id, double value) {
  DCHECK(std::isfinite(value));

  auto it = FindRangeBlock(value);
  RangeBlock& block = it->second;

  auto remove_result = block.Remove({id, value});
  LOG_IF(ERROR, !remove_result) << "RangeTree: Failed to remove id: " << id << ", value: " << value;

  // TODO: maybe merging blocks if they are too small
  // The problem that for each mutable operation we do Remove and then Add,
  // So we can do merge and split for one operation.
  // Or in common cases users do not remove a lot of documents?
}

RangeResult RangeTree::Range(double l, double r) const {
  DCHECK(l <= r);

  auto it_l = FindRangeBlock(l);
  auto it_r = FindRangeBlock(r);

  absl::InlinedVector<const RangeBlock*, 5> blocks;
  for (auto it = it_l;; ++it) {
    blocks.push_back(&it->second);
    if (it == it_r) {
      break;
    }
  }

  DCHECK(!blocks.empty());

  return {std::move(blocks), l, r};
}

RangeResult RangeTree::GetAllDocIds() const {
  absl::InlinedVector<const RangeBlock*, 5> blocks;
  blocks.reserve(entries_.size());

  for (const auto& entry : entries_) {
    blocks.push_back(&entry.second);
  }

  return RangeResult{std::move(blocks)};
}

RangeTree::Map::iterator RangeTree::FindRangeBlock(double value) {
  return FindRangeBlockImpl(entries_, value);
}

RangeTree::Map::const_iterator RangeTree::FindRangeBlock(double value) const {
  return FindRangeBlockImpl(entries_, value);
}

/*
There is an edge case in the SplitBlock method:
If split_result.left.Size() == 0, it means that all values in the block
were equal to the median value.
Because split works like this:
  - at the beginning it does not insert median values into the left or right block,
  - then it checks if left block is smaller than right block, if so, it adds
    median values to the left block, otherwise it adds it to the right block.
So if left block is empty, it means that left.Size() < right.Size() was false,
what means that right.Size() was also zero.
After that all median entries were added to the right block.

That means that we have equal values in the whole block,
and their count is greater than max_range_block_size_.
So we will do cascade splits of the right block.
TODO: we can optimize this case by splitting to three blocks:
 - empty left block with range [l, m),
 - middle block with range [m, std::nextafter(m, +inf)),
 - empty right block with range [std::nextafter(m, +inf), r)
*/
void RangeTree::SplitBlock(Map::iterator it) {
  const RangeNumber l = it->first;

  auto split_result = Split(std::move(it->second));

  const RangeNumber m = split_result.median;
  DCHECK(split_result.right.Size() > 0);

  entries_.erase(it);

  if (l != m) {
    // If l == m, it means that all values in the block were equal to the median value
    // We can not insert an empty block with range [l, l) because it is not valid.
    entries_.emplace(std::piecewise_construct, std::forward_as_tuple(l),
                     std::forward_as_tuple(std::move(split_result.left)));
  }

  entries_.emplace(std::piecewise_construct, std::forward_as_tuple(m),
                   std::forward_as_tuple(std::move(split_result.right)));

  DCHECK(TreeIsInCorrectState());
}

// Used for DCHECKs to check that the tree is in a correct state.
[[maybe_unused]] bool RangeTree::TreeIsInCorrectState() const {
  if (entries_.empty()) {
    return false;
  }

  Key prev_range = entries_.begin()->first;
  for (auto it = std::next(entries_.begin()); it != entries_.end(); ++it) {
    const Key& current_range = it->first;

    // Check that ranges are non-overlapping and sorted
    // Also there can not be gaps between ranges
    if (prev_range >= current_range) {
      return false;
    }

    prev_range = current_range;
  }

  return true;
}

RangeResult::RangeResult(absl::InlinedVector<RangeBlockPointer, 5> blocks)
    : blocks_(std::move(blocks)) {
  DCHECK(!blocks_.empty());
}

RangeResult::RangeResult(absl::InlinedVector<RangeBlockPointer, 5> blocks, double l, double r)
    : l_(l), r_(r), blocks_(std::move(blocks)) {
  DCHECK(!blocks_.empty());
}

std::vector<DocId> RangeResult::MergeAllResults() const {
  if (blocks_.size() == 1) {
    // If there is only one block, we can return its result directly
    return MergeSingleBlock(*blocks_[0], l_, r_);
  } else if (blocks_.size() == 2) {
    // If there are two blocks, we can merge them directly
    return MergeTwoBlocks(*blocks_[0], *blocks_[1], l_, r_);
  }

  // After the benchmarking, it is better to use inlined vector
  // than std::priority_queue
  absl::InlinedVector<RangeFilterIterator, 10> heap;
  heap.reserve(blocks_.size());

  size_t doc_ids_count = 0;
  for (const auto* block : blocks_) {
    auto it = MakeBegin(*block, l_, r_);
    if (!it.HasReachedEnd()) {
      heap.emplace_back(it);
      doc_ids_count += block->Size();
    }
  }

  std::vector<DocId> result;
  result.reserve(doc_ids_count);

  size_t size = heap.size();
  while (size) {
    DCHECK(!heap[0].HasReachedEnd());

    size_t min_doc_id_index = 0;
    for (size_t i = 1; i < size; ++i) {
      DCHECK(!heap[i].HasReachedEnd());

      if (*heap[i] < *heap[min_doc_id_index]) {
        min_doc_id_index = i;
      }
    }

    auto& it = heap[min_doc_id_index];
    result.push_back(*it);
    ++it;

    if (it.HasReachedEnd()) {
      // If we reached the end of the current block, remove it from the heap
      std::swap(heap[min_doc_id_index], heap[size - 1]);
      --size;
    }
  }

  DCHECK(std::is_sorted(result.begin(), result.end()));
  return result;
}

}  // namespace dfly::search
