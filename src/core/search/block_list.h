#pragma once

#include <absl/types/span.h>

#include <algorithm>
#include <cstdint>
#include <iterator>
#include <optional>
#include <vector>

#include "core/search/base.h"
#include "core/search/compressed_sorted_set.h"

namespace dfly::search {

// Supports Insert and Remove operations for keeping a sorted vector internally.
// Wrapper to use vectors with BlockList
struct SortedVector {
  SortedVector(PMR_NS::memory_resource* mr) : entries_{mr} {
  }

  bool Insert(DocId t);

  bool Remove(DocId t);

  void Merge(SortedVector&& other);

  std::pair<SortedVector, SortedVector> Split();

  size_t Size() {
    return entries_.size();
  }

  using iterator = typename std::vector<DocId>::const_iterator;

  auto begin() const {
    return entries_.cbegin();
  }

  auto end() const {
    return entries_.cend();
  }

 private:
  SortedVector(PMR_NS::vector<DocId>&& v) : entries_{std::move(v)} {
  }

  PMR_NS::vector<DocId> entries_;
};

// BlockList is a container for CompressedSortedSet / vector<DocId> to separate the full sorted
// range between a list of those containers. This reduces any update compexity from O(N)
// to O(K), where K is the max block size.
//
// It tires to balance their sizes in the range [block_size / 2, block_size * 2].
template <typename C /* underlying container type */> class BlockList {
  using BlockIt = typename std::vector<C>::iterator;

 public:
  BlockList(PMR_NS::memory_resource* mr, size_t block_size = 1000)
      : block_size_{block_size}, mr_{mr} {
  }

  // Insert element, returns true if inserted, false if already present.
  bool Insert(DocId t);

  // Remove element, returns true if removed, false if not found.
  bool Remove(DocId t);

  size_t Size() {
    return size_;
  }

  size_t size() {
    return size_;
  }

  struct BlockListIterator {
    // To make it work with std container contructors
    using iterator_category = std::forward_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using value_type = DocId;
    using pointer = DocId*;
    using reference = DocId&;

    DocId operator*() const {
      return **block_it;
    }

    BlockListIterator& operator++();

    friend class BlockList;

    friend bool operator==(const BlockListIterator& l, const BlockListIterator& r) {
      return l.block_it == r.block_it;
    }

    friend bool operator!=(const BlockListIterator& l, const BlockListIterator& r) {
      return !(l == r);
    }

   private:
    BlockListIterator(BlockIt begin, BlockIt end) : it(begin), it_end(end) {
      if (it != it_end) {
        block_it = it->begin();
        block_end = it->end();
      }
    }

    BlockIt it, it_end;
    std::optional<typename C::iterator> block_it, block_end;
  };

  BlockListIterator begin() {
    return BlockListIterator{blocks_.begin(), blocks_.end()};
  }

  BlockListIterator end() {
    return BlockListIterator{blocks_.end(), blocks_.end()};
  }

 private:
  // Find block that should contain t. Returns end() only if empty
  BlockIt FindBlock(DocId t);

  // If needed, try merging with previous block
  void TryMerge(BlockIt block);

  // If needed, try splitting
  void TrySplit(BlockIt block);

 private:
  const size_t block_size_ = 1000;

  size_t size_ = 0;
  PMR_NS::vector<C> blocks_;
};

}  // namespace dfly::search
