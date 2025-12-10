#include "core/search/block_list.h"

#include "core/page_usage/page_usage_stats.h"

namespace {

template <typename T> bool DefragmentVector(PMR_NS::vector<T>& vec, dfly::PageUsage* page_usage) {
  if (vec.empty() || !page_usage->IsPageForObjectUnderUtilized(vec.data())) {
    return false;
  }

  PMR_NS::vector<T> new_vec(vec.get_allocator());
  new_vec.reserve(vec.size());
  for (auto&& element : vec) {
    new_vec.push_back(std::move(element));
  }
  vec = std::move(new_vec);
  return true;
}

}  // namespace

namespace dfly::search {

using namespace std;

SplitResult Split(BlockList<SortedVector<std::pair<DocId, double>>>&& block_list) {
  using Entry = std::pair<DocId, double>;
  DCHECK(!block_list.Empty());

  const size_t elements_count = block_list.Size();

  // Extract values to find median
  std::vector<double> entries_values(elements_count);
  size_t index = 0;
  for (const Entry& entry : block_list) {
    entries_values[index++] = entry.second;
  }

  // Find median value
  std::nth_element(entries_values.begin(), entries_values.begin() + elements_count / 2,
                   entries_values.end());
  double median_value = entries_values[elements_count / 2];

  /* Now we need to split entries into two parts, left and right, so that:
   1) left has values < median_value
   2) right has values >= median_value
   3) both parts have approximately the same number of elements

   To achieve this, we first split entries into three parts: < median_value (left blocklist), ==
   median_value (median_entries), > median_value (righ blocklist). Then we add == median_value part
   to the smaller of the two parts (< or >). This guarantees that both parts have approximately the
   same number of elements */
  BlockList<SortedVector<Entry>> left(block_list.blocks_.get_allocator().resource(),
                                      block_list.block_size_);
  BlockList<SortedVector<Entry>> right(block_list.blocks_.get_allocator().resource(),
                                       block_list.block_size_);
  absl::InlinedVector<Entry, 1> median_entries;

  left.ReserveBlocks(block_list.blocks_.size() / 2 + 1);
  right.ReserveBlocks(block_list.blocks_.size() / 2 + 1);

  double lmin = std::numeric_limits<double>::infinity(), rmin = lmin;
  double lmax = -std::numeric_limits<double>::infinity(), rmax = lmax;

  for (const Entry& entry : block_list) {
    if (entry.second < median_value) {
      left.PushBack(entry);
      lmin = std::min(lmin, entry.second);
      lmax = std::max(lmax, entry.second);
    } else if (entry.second > median_value) {
      right.PushBack(entry);
      rmin = std::min(rmin, entry.second);
      rmax = std::max(rmax, entry.second);
    } else {
      median_entries.push_back(entry);
    }
  }
  block_list.Clear();

  if (left.Size() < right.Size()) {
    // If left is smaller, we can add median entries to it
    // We need to change median value to the right part and update lmax
    lmax = median_value;
    lmin = std::min(lmin, median_value);
    median_value = rmin;
    for (const auto& entry : median_entries) {
      left.Insert(entry);
    }
  } else {
    // If right part is smaller, we can add median entries to it
    // Median value is still the same
    rmax = std::max(rmax, median_value);
    for (const auto& entry : median_entries) {
      right.Insert(entry);
    }
  }

  return {std::move(left), std::move(right), median_value, lmin, lmax, rmax};
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
  // If the last block is full, after insert we will need to split it
  // So we can prevent split by creating a new block and inserting there
  if (blocks_.empty() || ShouldSplit(blocks_.back().Size() + 1)) {
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

template <typename Container>
DefragmentResult BlockList<Container>::Defragment(PageUsage* page_usage) {
  if (page_usage->QuotaDepleted()) {
    return DefragmentResult{.quota_depleted = true, .objects_moved = 0};
  }

  DefragmentResult result;
  if (DefragmentVector(blocks_, page_usage)) {
    result.objects_moved += 1;
  }

  for (Container& block : blocks_) {
    if (result.Merge(block.Defragment(page_usage)).quota_depleted) {
      break;
    }
  }
  return result;
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

template <typename C> bool BlockList<C>::ShouldSplit(size_t block_size) const {
  return block_size >= block_size_ * 2;
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
  if (!ShouldSplit(block->Size() + 1))
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

template <typename T> DefragmentResult SortedVector<T>::Defragment(PageUsage* page_usage) {
  if (DefragmentVector(entries_, page_usage)) {
    return DefragmentResult{.quota_depleted = false, .objects_moved = 1};
  }
  return DefragmentResult{};
}

template class SortedVector<DocId>;
template class SortedVector<std::pair<DocId, double>>;

}  // namespace dfly::search
