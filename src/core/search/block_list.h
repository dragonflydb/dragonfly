#pragma once

#include <absl/types/span.h>

#include <algorithm>
#include <cstdint>
#include <iterator>
#include <optional>
#include <type_traits>
#include <vector>

#include "core/search/base.h"
#include "core/search/compressed_sorted_set.h"

namespace dfly::search {

// Forward declarations
struct SplitResult;
template <typename Container> class BlockList;
template <typename T> class SortedVector;

/* Split into two blocks, left and right, so that both blocks have approximately the same number
   of elements. Returns median value of the split. Garantees that median present in the right
   block and not present in the left block. Does not work for empty BlockList. */
// TODO: Move to RangeTree logic
SplitResult Split(BlockList<SortedVector<std::pair<DocId, double>>>&& result);

// BlockList is a container wrapper for CompressedSortedSet / vector<DocId>
// to divide the full sorted id range into separate blocks. This reduces modification
// complexity from O(N) to O(logN + K), where K is the max block size.
//
// It tries to balance block sizes in the range [block_size / 2, block_size * 2]
// by splitting or merging nodes when needed.
// container must have declare ElementType typename
template <typename Container /* underlying container */> class BlockList {
 private:
  using BlockIt = typename PMR_NS::vector<Container>::iterator;
  using ConstBlockIt = typename PMR_NS::vector<Container>::const_iterator;
  using ElementType = typename Container::ElementType;

 public:
  BlockList(PMR_NS::memory_resource* mr, size_t block_size = 1000)
      : block_size_{block_size}, blocks_(mr) {
  }

  BlockList(const BlockList& other) = default;

  BlockList(BlockList&& other) noexcept {
    // Consider not to do move if block_size_ is different
    // DCHECK(block_size_ == other.block_size_);
    // It seams there is bugs in BaseStringIndex
    // because this check fails for it

    size_ = other.size_;
    blocks_ = std::move(other.blocks_);
    other.Clear();
  }

  BlockList& operator=(const BlockList& other) = delete;
  BlockList& operator=(BlockList&& other) = delete;

  ~BlockList() = default;

  // Insert element, returns true if inserted, false if already present.
  bool Insert(ElementType t);
  bool PushBack(ElementType t);

  // Remove element, returns true if removed, false if not found.
  bool Remove(ElementType t);

  size_t Size() const {
    return size_;
  }

  size_t size() const {
    return size_;
  }

  bool Empty() const {
    return size_ == 0;
  }

  void Clear() {
    size_ = 0;
    blocks_.clear();
  }

  struct BlockListIterator : public SeekableTag {
    // To make it work with std container contructors
    using iterator_category = std::forward_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using value_type = ElementType;
    using pointer = ElementType*;
    using reference = ElementType&;

    ElementType operator*() const {
      return *block_it;
    }

    BlockListIterator& operator++();
    void SeekGE(DocId min_doc_id);

    friend class BlockList;

    bool operator==(const BlockListIterator& other) const {
      return it == other.it && block_it == other.block_it;
    }

    bool operator!=(const BlockListIterator& other) const {
      return !operator==(other);
    }

   private:
    BlockListIterator(ConstBlockIt begin, ConstBlockIt end) : it(begin), it_end(end) {
      if (it != it_end) {
        block_it = it->begin();
        block_end = it->end();
      }
    }

    ConstBlockIt it, it_end;
    typename Container::iterator block_it, block_end;
  };

  BlockListIterator begin() const {
    return BlockListIterator{blocks_.begin(), blocks_.end()};
  }

  BlockListIterator end() const {
    return BlockListIterator{blocks_.end(), blocks_.end()};
  }

 private:
  // Find block that should contain t. Returns end() only if empty
  BlockIt FindBlock(const ElementType& t);

  bool ShouldSplit(size_t block_size) const;

  void TryMerge(BlockIt block);  // If needed, merge with previous block
  void TrySplit(BlockIt block);  // If needed, split into two blocks

  void ReserveBlocks(size_t n);

  friend SplitResult Split(BlockList<SortedVector<std::pair<DocId, double>>>&& block_list);

 private:
  const size_t block_size_ = 1000;
  size_t size_ = 0;
  PMR_NS::vector<Container> blocks_;
};

// Supports Insert and Remove operations for keeping a sorted vector internally.
// Wrapper to use vectors with BlockList
template <typename T> class SortedVector {
 public:
  using ElementType = T;

  explicit SortedVector(PMR_NS::memory_resource* mr) : entries_(mr) {
  }

  bool Insert(T t);
  bool Remove(T t);
  void Merge(SortedVector<T>&& other);
  std::pair<SortedVector<T>, SortedVector<T>> Split() &&;

  T& operator[](size_t idx) {
    return entries_[idx];
  }

  const T& operator[](size_t idx) const {
    return entries_[idx];
  }

  size_t Size() const {
    return entries_.size();
  }

  bool Empty() const {
    return entries_.empty();
  }

  void Clear() {
    entries_.clear();
  }

  const T& Back() const {
    return entries_.back();
  }

  using iterator = typename PMR_NS::vector<T>::const_iterator;

  iterator begin() const {
    return entries_.cbegin();
  }

  iterator end() const {
    return entries_.cend();
  }

 private:
  SortedVector(PMR_NS::vector<T>&& v) : entries_{std::move(v)} {
  }

  PMR_NS::vector<T> entries_;
};

extern template class SortedVector<DocId>;
extern template class SortedVector<std::pair<DocId, double>>;

extern template class BlockList<CompressedSortedSet>;
extern template class BlockList<SortedVector<DocId>>;
extern template class BlockList<SortedVector<std::pair<DocId, double>>>;

// Used by Split method
struct SplitResult {
  using Container = BlockList<SortedVector<std::pair<DocId, double>>>;

  Container left;
  Container right;

  // Median value of split, used as minimum value of right block
  double median;

  // Min/max values of left (lmin, lmax) and right (rmin=median, rmax) blocks
  double lmin, lmax, rmax;
};
}  // namespace dfly::search
