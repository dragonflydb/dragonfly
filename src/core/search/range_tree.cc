// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/range_tree.h"

namespace dfly::search {

namespace {

std::vector<DocId> MergeAllResults(absl::Span<const RangeTree::RangeBlock*> blocks, double l,
                                   double r) {
  DCHECK(blocks.size() != 1 && blocks.size() != 2);

  // After the benchmarking, it is better to use inlined vector
  // than std::priority_queue
  absl::InlinedVector<RangeFilterIterator, 10> heap;
  heap.reserve(blocks.size());

  size_t doc_ids_count = 0;
  for (const auto* block : blocks) {
    auto it = MakeBegin(*block, l, r);
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

RangeTree::RangeTree(PMR_NS::memory_resource* mr, size_t max_range_block_size,
                     bool enable_splitting)
    : max_range_block_size_(max_range_block_size),
      entries_(mr),
      enable_splitting_(enable_splitting) {
  // The tree has at least always a block with a negative infinity bound, so that any new insertion
  // goes at least somewhere
  CreateEmptyBlock(-std::numeric_limits<double>::infinity());
}

void RangeTree::Add(DocId id, double value) {
  DCHECK(std::isfinite(value));

  auto it = FindRangeBlock(value);
  auto& [lower_bound, block] = *it;

  // Don't disrupt large monovalue blocks, instead create new nextafter block
  if (enable_splitting_ && block.Size() >= max_range_block_size_ &&
      lower_bound == block.max_seen /* monovalue */ &&
      value != lower_bound /* but new value is different*/
  ) {
    // We use nextafter as the lower bound to "catch" all other possible inserts into the block,
    // as a decreasing `value` sequence would otherwise create lots of single-value blocks
    double lb2 = std::nextafter(lower_bound, std::numeric_limits<double>::infinity());
    CreateEmptyBlock(lb2)->second.Insert({id, value});
    return;
  }

  auto insert_result = block.Insert({id, value});
  LOG_IF(ERROR, !insert_result) << "RangeTree: Failed to insert id: " << id << ", value: " << value;

  if (!enable_splitting_ || block.Size() <= max_range_block_size_)
    return;

  // Large monovalue block, not reducable by splitting
  if (lower_bound == block.max_seen)
    return;

  SplitBlock(it);
}

void RangeTree::Remove(DocId id, double value) {
  DCHECK(std::isfinite(value));

  auto it = FindRangeBlock(value);
  RangeBlock& block = it->second;

  auto remove_result = block.Remove({id, value});
  LOG_IF(ERROR, !remove_result) << "RangeTree: Failed to remove id: " << id << ", value: " << value;

  // Merge with left block if both are relatively small and won't be forced to split soon
  if (block.size() < max_range_block_size_ / 4 && it != entries_.begin()) {
    auto lit = it;
    --lit;

    auto& lblock = lit->second;
    if (block.Size() + lblock.Size() < max_range_block_size_ / 2) {
      for (auto e : block)
        lblock.Insert(e);
      entries_.erase(it);
      stats_.merges++;
    }
  }
}

RangeResult RangeTree::Range(double l, double r) const {
  return {RangeBlocks(l, r), l, r};
}

absl::InlinedVector<const RangeTree::RangeBlock*, 5> RangeTree::RangeBlocks(double l,
                                                                            double r) const {
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
  return blocks;
}

RangeResult RangeTree::GetAllDocIds() const {
  return RangeResult{GetAllBlocks()};
}

absl::InlinedVector<const RangeTree::RangeBlock*, 5> RangeTree::GetAllBlocks() const {
  absl::InlinedVector<const RangeBlock*, 5> blocks;
  blocks.reserve(entries_.size());

  for (const auto& entry : entries_) {
    blocks.push_back(&entry.second);
  }

  return blocks;
}

void RangeTree::FinalizeInitialization() {
  DCHECK(!enable_splitting_);
  DCHECK_EQ(entries_.size(), 1u);

  auto& block = entries_.begin()->second;
  const size_t total_size = block.Size();

  std::vector<Entry> entries(total_size);
  size_t index = 0;
  for (const auto& [id, value] : block) {
    entries[index++] = {id, value};
  }

  enable_splitting_ = true;
  block.Clear();

  std::sort(entries.begin(), entries.end(),
            [](const auto& a, const auto& b) { return a.second < b.second; });

  for (size_t b = 0; b < entries.size(); b += max_range_block_size_) {
    RangeBlock* range_block;
    if (b) {
      range_block = &CreateEmptyBlock(entries[b].second)->second;
    } else {
      range_block = &block;
    }

    DCHECK(range_block);

    const size_t end = std::min(b + max_range_block_size_, total_size);
    for (size_t i = b; i < end; ++i) {
      range_block->Insert(entries[i]);
    }
  }
}

RangeTree::Map::iterator RangeTree::FindRangeBlock(double value) {
  return FindRangeBlockImpl(entries_, value);
}

RangeTree::Map::const_iterator RangeTree::FindRangeBlock(double value) const {
  return FindRangeBlockImpl(entries_, value);
}

RangeTree::Map::iterator RangeTree::CreateEmptyBlock(double lb) {
  return entries_
      .emplace(std::piecewise_construct, std::forward_as_tuple(lb),
               std::forward_as_tuple(entries_.get_allocator().resource(), max_range_block_size_))
      .first;
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
  double lower_bound = it->first;

  auto split_result = Split(std::move(it->second));

  const double m = split_result.median;
  DCHECK(!split_result.right.Empty());

  entries_.erase(it);
  stats_.splits++;

  // Insert left block if it's not empty or if its the first one (negative inf bound)
  if (!split_result.left.Empty() || std::isinf(lower_bound)) {
    if (!std::isinf(lower_bound))  // keep negative inf bound
      lower_bound = split_result.lmin;

    entries_.emplace(std::piecewise_construct, std::forward_as_tuple(lower_bound),
                     std::forward_as_tuple(std::move(split_result.left), split_result.lmax));
  }

  entries_.emplace(std::piecewise_construct, std::forward_as_tuple(m),
                   std::forward_as_tuple(std::move(split_result.right), split_result.rmax));

  DCHECK(TreeIsInCorrectState());
}

RangeTree::Stats RangeTree::GetStats() const {
  return Stats{.splits = stats_.splits, .merges = stats_.merges, .block_count = entries_.size()};
}

// Used for DCHECKs to check that the tree is in a correct state.
[[maybe_unused]] bool RangeTree::TreeIsInCorrectState() const {
  if (entries_.empty()) {
    return false;
  }

  double prev_range = entries_.begin()->first;
  for (auto it = std::next(entries_.begin()); it != entries_.end(); ++it) {
    const double& current_range = it->first;

    // Check that ranges are non-overlapping and sorted
    // Also there can not be gaps between ranges
    if (prev_range >= current_range) {
      return false;
    }

    prev_range = current_range;
  }

  return true;
}

RangeResult::RangeResult(std::vector<DocId> doc_ids) : result_(std::move(doc_ids)) {
}

RangeResult::RangeResult(absl::InlinedVector<RangeBlockPointer, 5> blocks)
    : RangeResult(std::move(blocks), -std::numeric_limits<double>::infinity(),
                  std::numeric_limits<double>::infinity()) {
}

RangeResult::RangeResult(absl::InlinedVector<RangeBlockPointer, 5> blocks, double l, double r) {
  if (blocks.size() == 1) {
    result_ = SingleBlockRangeResult(blocks[0], l, r);
  } else if (blocks.size() == 2) {
    result_ = TwoBlocksRangeResult(blocks[0], blocks[1], l, r);
  } else {
    result_ = MergeAllResults(absl::MakeSpan(blocks), l, r);
  }
}

std::vector<DocId> RangeResult::Take() {
  if (std::holds_alternative<DocsList>(result_)) {
    DCHECK(std::is_sorted(std::get<DocsList>(result_).begin(), std::get<DocsList>(result_).end()));
    return std::get<DocsList>(std::move(result_));
  }

  auto cb = [](const auto& v) {
    std::vector<DocId> result;
    result.reserve(v.size());
    std::copy(v.begin(), v.end(), std::back_inserter(result));
    DCHECK(std::is_sorted(result.begin(), result.end()));
    return result;
  };

  return std::visit(cb, result_);
}

}  // namespace dfly::search
