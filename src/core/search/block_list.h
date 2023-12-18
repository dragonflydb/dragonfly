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

using IntType = DocId;

struct SortedVectorContainer {
  SortedVectorContainer() = default;
  SortedVectorContainer(PMR_NS::memory_resource*) : entries_{} {
  }

  bool Insert(IntType t) {
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

  bool Remove(IntType t) {
    auto it = std::lower_bound(entries_.begin(), entries_.end(), t);
    if (it != entries_.end() && *it == t) {
      entries_.erase(it);
      return true;
    }
    return false;
  }

  void Merge(SortedVectorContainer&& other) {
    for (int t : other.entries_)
      Insert(t);
  }

  std::pair<SortedVectorContainer, SortedVectorContainer> Split() && {
    std::vector<IntType> tail(entries_.begin() + entries_.size() / 2, entries_.end());
    entries_.resize(entries_.size() / 2);

    return std::make_pair(std::move(*this), SortedVectorContainer{std::move(tail)});
  }

  size_t Size() {
    return entries_.size();
  }

  using iterator = typename std::vector<IntType>::const_iterator;

  auto begin() const {
    return entries_.cbegin();
  }

  auto end() const {
    return entries_.cend();
  }

 private:
  SortedVectorContainer(std::vector<IntType>&& v) : entries_{std::move(v)} {
  }

  std::vector<IntType> entries_;
};

template <typename C /* underlying container type */> class BlockList {
  using BlockIt = typename std::vector<C>::iterator;

 public:
  BlockList(PMR_NS::memory_resource* mr, const size_t block_size = 1000)
      : block_size_{block_size}, mr_{mr} {
  }

  // Insert element, returns true if inserted, false if already present.
  bool Insert(IntType t);

  // Remove element, returns true if removed, false if not found.
  bool Remove(IntType t);

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
    using value_type = IntType;
    using pointer = IntType*;
    using reference = IntType&;

    IntType operator*() const {
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
  BlockIt FindBlock(IntType t);

  // If needed, try merging with previous block
  void TryMerge(BlockIt block);

  // If needed, try splitting
  void TrySplit(BlockIt block);

 private:
  const size_t block_size_ = 1000;

  PMR_NS::memory_resource* mr_;
  size_t size_ = 0;
  std::vector<C> blocks_;
};

}  // namespace dfly::search
