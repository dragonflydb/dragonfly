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

using C = CompressedSortedSet;
using IntType = DocId;

class BlockList {
  using BlockIt = std::vector<C>::iterator;

 public:
  void Insert(IntType t) {
    auto block = FindBlock(t);
    if (block == blocks_.end())
      block = blocks_.insert(blocks_.end(), C{nullptr});

    if (!block->Insert(t))
      return;

    size_++;
    TrySplit(block);
  }

  void Remove(IntType t) {
    if (auto block = FindBlock(t); block != blocks_.end() && block->Remove(t)) {
      size_--;
      TryMerge(block);
    }
  }

  size_t Size() {
    return size_;
  }

  struct BlockListIterator {
    IntType operator*() const {
      return **block_it;
    }

    BlockListIterator& operator++() {
      if (it == it_end)
        return *this;

      ++*block_it;
      if (block_it == block_end) {
        ++it;
        if (it != it_end) {
          block_it = it->begin();
          block_end = it->end();
        }
      }

      return *this;
    }

    friend class BlockList;
    friend bool operator==(const BlockListIterator& l, const BlockListIterator& r) {
      return l.it == r.it && l.block_it == r.block_it;
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
  BlockIt FindBlock(IntType t) {
    // Find first block that can't contain t
    auto it = lower_bound(blocks_.begin(), blocks_.end(), t,
                          [](const C& l, IntType t) { return *l.begin() <= t; });
    // Move to previous if possible
    if (it != blocks_.begin() && blocks_.size() > 0)
      --it;
    return it;
  }

  // If needed, try merging with previous block
  void TryMerge(BlockIt block) {
    if (block->Size() == 0) {
      blocks_.erase(block);
      return;
    }

    if (block->Size() >= kBaseblocksize / 2 || block == blocks_.begin())
      return;

    size_t idx = std::distance(blocks_.begin(), block);
    blocks_[idx - 1].Merge(std::move(*block));
    blocks_.erase(block);

    TrySplit(blocks_.begin() + (idx - 1));  // to not overgrow it
  }

  // If needed, try splitting
  void TrySplit(BlockIt block) {
    if (block->Size() < kBaseblocksize * 2)
      return;

    auto [left, right] = std::move(*block).Split();

    *block = std::move(left);
    blocks_.insert(block, std::move(right));
  }

 private:
  const size_t kBaseblocksize = 1000;

  size_t size_ = 0;
  std::vector<C> blocks_;
};

}  // namespace dfly::search
