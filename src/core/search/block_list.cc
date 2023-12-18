#include "core/search/block_list.h"

namespace dfly::search {

using namespace std;

namespace {}  // namespace

template <typename C> bool BlockList<C>::Insert(IntType t) {
  auto block = FindBlock(t);
  if (block == blocks_.end())
    block = blocks_.insert(blocks_.end(), C{mr_});

  if (!block->Insert(t))
    return false;

  size_++;
  TrySplit(block);
  return true;
}

template <typename C> bool BlockList<C>::Remove(IntType t) {
  if (auto block = FindBlock(t); block != blocks_.end() && block->Remove(t)) {
    size_--;
    TryMerge(block);
    return true;
  }

  return false;
}

template <typename C> typename BlockList<C>::BlockIt BlockList<C>::FindBlock(IntType t) {
  if (!blocks_.empty() && t >= *blocks_.back().begin())
    return --blocks_.end();

  // Find first block that can't contain t
  auto it = std::upper_bound(blocks_.begin(), blocks_.end(), t,
                             [](IntType t, const C& l) { return *l.begin() > t; });

  // Move to previous if possible
  if (it != blocks_.begin() && blocks_.size() > 0)
    --it;

  return it;
}

template <typename C> void BlockList<C>::TryMerge(BlockIt block) {
  if (block->Size() == 0) {
    blocks_.erase(block);
    return;
  }

  if (block->Size() >= block_size_ / 2 || block == blocks_.begin())
    return;

  size_t idx = std::distance(blocks_.begin(), block);
  blocks_[idx - 1].Merge(std::move(*block));
  blocks_.erase(block);

  TrySplit(blocks_.begin() + (idx - 1));  // to not overgrow it
}

template <typename C> void BlockList<C>::TrySplit(BlockIt block) {
  if (block->Size() < block_size_ * 2)
    return;

  auto [left, right] = std::move(*block).Split();

  *block = std::move(right);
  blocks_.insert(block, std::move(left));
}

template <typename C>
typename BlockList<C>::BlockListIterator& BlockList<C>::BlockListIterator::operator++() {
  ++*block_it;
  if (block_it == block_end) {
    ++it;
    if (it != it_end) {
      block_it = it->begin();
      block_end = it->end();
    } else {
      block_it = std::nullopt;
      block_end = std::nullopt;
    }
  }
  return *this;
}

template class BlockList<CompressedSortedSet>;
template class BlockList<SortedVectorContainer>;

}  // namespace dfly::search
