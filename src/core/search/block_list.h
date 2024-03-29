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
// BlockList is a container wrapper for CompressedSortedSet / vector<DocId>
// to divide the full sorted id range into separate blocks. This reduces modification
// complexity from O(N) to O(logN + K), where K is the max block size.
//
// It tries to balance block sizes in the range [block_size / 2, block_size * 2]
// by splitting or merging nodes when needed.
template <typename Container /* underlying container */> class BlockList {
  using BlockIt = typename PMR_NS::vector<Container>::iterator;
  using ConstBlockIt = typename PMR_NS::vector<Container>::const_iterator;

 public:
  BlockList(PMR_NS::memory_resource* mr, size_t block_size = 1000)
      : block_size_{block_size}, blocks_(mr) {
  }

  // Insert element, returns true if inserted, false if already present.
  bool Insert(DocId t);

  // Remove element, returns true if removed, false if not found.
  bool Remove(DocId t);

  size_t Size() const {
    return size_;
  }

  size_t size() const {
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
    std::optional<typename Container::iterator> block_it, block_end;
  };

  BlockListIterator begin() const {
    return BlockListIterator{blocks_.begin(), blocks_.end()};
  }

  BlockListIterator end() const {
    return BlockListIterator{blocks_.end(), blocks_.end()};
  }

 private:
  // Find block that should contain t. Returns end() only if empty
  BlockIt FindBlock(DocId t);

  void TryMerge(BlockIt block);  // If needed, merge with previous block
  void TrySplit(BlockIt block);  // If needed, split into two blocks

 private:
  const size_t block_size_ = 1000;
  size_t size_ = 0;
  PMR_NS::vector<Container> blocks_;
};

// Supports Insert and Remove operations for keeping a sorted vector internally.
// Wrapper to use vectors with BlockList
struct SortedVector {
  explicit SortedVector(PMR_NS::memory_resource* mr) : entries_(mr) {
  }

  bool Insert(DocId t);
  bool Remove(DocId t);
  void Merge(SortedVector&& other);
  std::pair<SortedVector, SortedVector> Split() &&;

  size_t Size() {
    return entries_.size();
  }

  using iterator = typename PMR_NS::vector<DocId>::const_iterator;

  iterator begin() const {
    return entries_.cbegin();
  }

  iterator end() const {
    return entries_.cend();
  }

 private:
  SortedVector(PMR_NS::vector<DocId>&& v) : entries_{std::move(v)} {
  }

  PMR_NS::vector<DocId> entries_;
};

extern template class BlockList<CompressedSortedSet>;
extern template class BlockList<SortedVector>;

}  // namespace dfly::search
