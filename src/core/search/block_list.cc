#include "core/search/block_list.h"

namespace dfly::search {

using namespace std;

SplitResult Split(BlockList<SortedVector<std::pair<DocId, double>>>&& block_list) {
  using Entry = std::pair<DocId, double>;
  DCHECK(!block_list.Empty());

  const size_t elements_count = block_list.Size();

  using EntryIndex = std::pair<size_t /*block id*/, size_t /*entry id*/>;
  std::vector<EntryIndex> entries_indexes(elements_count);
  size_t index = 0;
  for (size_t i = 0; i < block_list.blocks_.size(); ++i) {
    auto& block = block_list.blocks_[i];
    for (size_t j = 0; j < block.Size(); ++j) {
      entries_indexes[index++] = {i, j};
    }
  }

  std::nth_element(entries_indexes.begin(), entries_indexes.begin() + elements_count / 2,
                   entries_indexes.end(), [&](const EntryIndex& l, const EntryIndex& r) {
                     return block_list.blocks_[l.first][l.second].second <
                            block_list.blocks_[r.first][r.second].second;
                   });

  const EntryIndex median_index = entries_indexes[elements_count / 2];
  double median_value = block_list.blocks_[median_index.first][median_index.second].second;

  BlockList<SortedVector<Entry>> left(block_list.blocks_.get_allocator().resource(),
                                      block_list.block_size_);
  BlockList<SortedVector<Entry>> right(block_list.blocks_.get_allocator().resource(),
                                       block_list.block_size_);
  absl::InlinedVector<Entry, 1> median_entries;

  left.ReserveBlocks(block_list.blocks_.size() / 2 + 1);
  right.ReserveBlocks(block_list.blocks_.size() / 2 + 1);

  double min_value_in_right_part = std::numeric_limits<double>::infinity();
  for (const auto& entry : block_list) {
    if (entry.second < median_value) {
      left.PushBack(entry);
    } else if (entry.second > median_value) {
      right.PushBack(entry);
      if (entry.second < min_value_in_right_part) {
        min_value_in_right_part = entry.second;
      }
    } else {
      median_entries.push_back(entry);
    }
  }
  block_list.Clear();

  if (left.Size() < right.Size()) {
    // If left is smaller, we can add median entries to it
    // We need to change median value to the right part
    median_value = min_value_in_right_part;
    for (const auto& entry : median_entries) {
      left.Insert(entry);
    }
  } else {
    // If right part is smaller, we can add median entries to it
    // Median value is still the same
    for (const auto& entry : median_entries) {
      right.Insert(entry);
    }
  }

  return {std::move(left), std::move(right), median_value};
}

template <typename C> bool BlockList<C>::Insert(ElementType t) {
  auto block = FindBlock(t);
  if (block == blocks_.end())
    block = blocks_.insert(blocks_.end(), C{blocks_.get_allocator().resource()});

  if (!block->Insert(std::move(t)))
    return false;

  size_++;
  TrySplit(block);
  return true;
}

template <typename C> bool BlockList<C>::PushBack(ElementType t) {
  if (blocks_.empty() || (blocks_.back().Size() >= block_size_ * 2 - 1)) {
    blocks_.insert(blocks_.end(), C{blocks_.get_allocator().resource()});
  }

  if (!blocks_.back().Insert(std::move(t)))
    return false;

  size_++;
  return true;
}

template <typename C> bool BlockList<C>::Remove(ElementType t) {
  if (auto block = FindBlock(t); block != blocks_.end() && block->Remove(std::move(t))) {
    size_--;
    TryMerge(block);
    return true;
  }

  return false;
}

template <typename C> typename BlockList<C>::BlockIt BlockList<C>::FindBlock(const ElementType& t) {
  DCHECK(blocks_.empty() || !blocks_.back().Empty());

  if (!blocks_.empty() && t >= *blocks_.back().begin())
    return --blocks_.end();

  // Find first block that can't contain t
  auto it = std::upper_bound(blocks_.begin(), blocks_.end(), t,
                             [](const ElementType& t, const C& l) { return *l.begin() > t; });

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

template <typename C> void BlockList<C>::ReserveBlocks(size_t n) {
  blocks_.reserve(n);
}

template <typename C>
typename BlockList<C>::BlockListIterator& BlockList<C>::BlockListIterator::operator++() {
  ++block_it;
  if (block_it == block_end) {
    ++it;
    if (it != it_end) {
      block_it = it->begin();
      block_end = it->end();
    } else {
      block_it = {};
      block_end = {};
    }
  }
  return *this;
}

template <typename C> void BlockList<C>::BlockListIterator::SeekGE(DocId min_doc_id) {
  if (it == it_end) {
    block_it = {};
    block_end = {};
    return;
  }

  auto extract_doc_id = [](const auto& value) {
    using T = std::decay_t<decltype(value)>;
    if constexpr (std::is_same_v<T, DocId>) {
      return value;
    } else {
      return value.first;
    }
  };

  auto needed_block = [&](const auto& it) {
    return it->begin() != it->end() && min_doc_id <= extract_doc_id(it->Back());
  };

  // Choose the first block that has the last element >= min_doc_id
  if (!needed_block(it)) {
    while (++it != it_end) {
      if (needed_block(it)) {
        block_it = it->begin();
        block_end = it->end();
        break;
      }
    }
    if (it == it_end) {
      block_it = {};
      block_end = {};
      return;
    }
  }

  BasicSeekGE(min_doc_id, block_end, &block_it);
  DCHECK(block_it != block_end && min_doc_id <= extract_doc_id(*block_it));
}

template class BlockList<CompressedSortedSet>;
template class BlockList<SortedVector<DocId>>;
template class BlockList<SortedVector<std::pair<DocId, double>>>;

template <typename T> bool SortedVector<T>::Insert(T t) {
  if (entries_.empty() || t > entries_.back()) {
    entries_.push_back(t);
    return true;
  }

  auto it = std::lower_bound(entries_.begin(), entries_.end(), t);
  if (it != entries_.end() && *it == t)
    return false;

  entries_.insert(it, t);
  return true;
}

template <typename T> bool SortedVector<T>::Remove(T t) {
  auto it = std::lower_bound(entries_.begin(), entries_.end(), t);
  if (it != entries_.end() && *it == t) {
    entries_.erase(it);
    return true;
  }
  return false;
}

template <typename T> void SortedVector<T>::Merge(SortedVector&& other) {
  // NLog compexity in theory, but in practice used only to merge with larger values.
  // Tail insert optimization makes it linear
  entries_.reserve(entries_.size() + other.entries_.size());
  for (T& t : other.entries_)
    Insert(std::move(t));
}

template <typename T> std::pair<SortedVector<T>, SortedVector<T>> SortedVector<T>::Split() && {
  PMR_NS::vector<T> tail(entries_.begin() + entries_.size() / 2, entries_.end());
  entries_.resize(entries_.size() / 2);

  return std::make_pair(std::move(*this), SortedVector<T>{std::move(tail)});
}

template class SortedVector<DocId>;
template class SortedVector<std::pair<DocId, double>>;

}  // namespace dfly::search
