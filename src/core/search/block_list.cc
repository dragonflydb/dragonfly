#include "core/search/block_list.h"

namespace dfly::search {

using namespace std;

template <typename C> bool BlockList<C>::Insert(DocId t) {
  auto block = FindBlock(t);
  if (block == blocks_.end())
    block = blocks_.insert(blocks_.end(), C{blocks_.get_allocator().resource()});

  if (!block->Insert(t))
    return false;

  size_++;
  TrySplit(block);
  return true;
}

template <typename C> bool BlockList<C>::Remove(DocId t) {
  if (auto block = FindBlock(t); block != blocks_.end() && block->Remove(t)) {
    size_--;
    TryMerge(block);
    return true;
  }

  return false;
}

template <typename C> typename BlockList<C>::BlockIt BlockList<C>::FindBlock(DocId t) {
  DCHECK(blocks_.empty() || blocks_.back().Size() > 0u);

  if (!blocks_.empty() && t >= *blocks_.back().begin())
    return --blocks_.end();

  // Find first block that can't contain t
  auto it = std::upper_bound(blocks_.begin(), blocks_.end(), t,
                             [](DocId t, const C& l) { return *l.begin() > t; });

  // Move to previous if possible
  if (it != blocks_.begin())
    --it;

  DCHECK(it == blocks_.begin() || it->Size() * 2 >= block_size_);
  DCHECK(it == blocks_.end() || it->Size() <= 2 * block_size_);
  return it;
}

template <typename C> void BlockList<C>::TryMerge(BlockIt block) {
  if (block->Size() == 0) {
    blocks_.erase(block);
    return;
  }

  if (block->Size() >= block_size_ / 2 || block == blocks_.begin())
    return;

  // Merge strictly right with left to benefit from tail insert optimizations
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
template class BlockList<SortedVector>;

bool SortedVector::Insert(DocId t) {
  if (entries_.size() > 0 && t > entries_.back()) {
    entries_.push_back(t);
    return true;
  }

  auto it = std::lower_bound(entries_.begin(), entries_.end(), t);
  if (it != entries_.end() && *it == t)
    return false;

  entries_.insert(it, t);
  return true;
}

bool SortedVector::Remove(DocId t) {
  auto it = std::lower_bound(entries_.begin(), entries_.end(), t);
  if (it != entries_.end() && *it == t) {
    entries_.erase(it);
    return true;
  }
  return false;
}

void SortedVector::Merge(SortedVector&& other) {
  // NLog compexity in theory, but in practice used only to merge with larger values.
  // Tail insert optimization makes it linear
  entries_.reserve(entries_.size() + other.entries_.size());
  for (int t : other.entries_)
    Insert(t);
}

std::pair<SortedVector, SortedVector> SortedVector::Split() && {
  PMR_NS::vector<DocId> tail(entries_.begin() + entries_.size() / 2, entries_.end());
  entries_.resize(entries_.size() / 2);

  return std::make_pair(std::move(*this), SortedVector{std::move(tail)});
}

}  // namespace dfly::search
