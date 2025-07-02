#pragma once

#include <absl/container/btree_map.h>

#include <memory>
#include <queue>
#include <vector>

#include "base/pmr/memory_resource.h"
#include "core/search/base.h"
#include "core/search/block_list.h"

namespace dfly::search {
class RangeResult;

class RangeTree {
 private:
  friend class RangeResult;
  using RangeNumber = long long;
  using Key = std::pair<RangeNumber, RangeNumber>;
  using Entry = std::pair<DocId, double>;
  using RangeBlock = BlockList<SortedVector<Entry>>;
  using Map = absl::btree_map<Key, RangeBlock, std::less<Key>,
                              PMR_NS::polymorphic_allocator<std::pair<const Key, RangeBlock>>>;

  // static constexpr size_t kMinRangeBlockSize = 200;
  static constexpr size_t kMaxRangeBlockSize = 500000;
  static constexpr size_t kBlockSize = 400;

 public:
  explicit RangeTree(PMR_NS::memory_resource* mr);

  // Adds a document with a value to the index.
  void Add(DocId id, double value);

  // Removes a document with a value from the index.
  void Remove(DocId id, double value);

  // Returns all documents with values in the range [l, r).
  RangeResult Range(double l, double r) const;

  RangeResult GetAllDocIds() const;

 private:
  Map::iterator FindRangeBlock(double value);
  Map::const_iterator FindRangeBlock(double value) const;

  void SplitBlock(Map::iterator it);

  // void PrintTheWholeTree() const;

 private:
  Map entries_;
};

class RangeResult {
 private:
  using RangeBlockPointer = const RangeTree::RangeBlock*;
  // TODO: use variant<vector<DocId>, const RangeTree::RangeBlock*>
  // OR just insert it to middle_blocks_ and let leftmost_block_ and rightmost_block_ be empty
  using BorderBlock = std::vector<DocId>;
  using BorderBlockIterator = std::vector<DocId>::const_iterator;
  using RangeBlockIterator = RangeTree::RangeBlock::BlockListIterator;

  // Utils methods to extract DocIds from RangeBlock
  static std::vector<DocId> LessOrEqual(const RangeTree::RangeBlock& range_block, double r);
  static std::vector<DocId> GreaterOrEqual(const RangeTree::RangeBlock& range_block, double l);
  static std::vector<DocId> Range(const RangeTree::RangeBlock& range_block, double l, double r);
  static std::vector<DocId> GetAllDocIds(const RangeTree::RangeBlock& range_block);

 public:
  class ResultsMerger {
   private:
    using MiddleBlockIteratorList = absl::InlinedVector<RangeBlockIterator, 10>;

    struct Compare {
      const MiddleBlockIteratorList& iters;
      bool operator()(size_t a, size_t b) const {
        return (*iters[a]).first > (*iters[b]).first;
      }
    };

   public:
    static constexpr size_t kInlavidDocId = std::numeric_limits<DocId>::max();

    explicit ResultsMerger(const RangeResult& result);

    bool HasNext() const {
      return (leftmost_it_ != range_result_.leftmost_block_.end()) ||
             (rightmost_it_ != range_result_.rightmost_block_.end()) || !heap_.empty();
    }

    // Returns min DocId that is bigger or equal to min_value
    // Can return kInlavidDocId if no more results are available.
    DocId GetMin(DocId min_value);

   private:
    const RangeResult& range_result_;
    BorderBlockIterator leftmost_it_;
    BorderBlockIterator rightmost_it_;
    MiddleBlockIteratorList middle_blocks_iters_;
    std::priority_queue<size_t, std::vector<size_t>, Compare> heap_{Compare{middle_blocks_iters_}};
  };

  RangeResult() = default;

  explicit RangeResult(absl::InlinedVector<RangeBlockPointer, 10> blocks);

  RangeResult(const RangeTree::RangeBlock& block, std::pair<double, double> range);

  RangeResult(absl::InlinedVector<RangeBlockPointer, 10> blocks,
              std::optional<double> leftmost_block_range,
              std::optional<double> rightmost_block_range);

  std::vector<DocId> MergeAllResults();

  friend class ResultsMerger;

 private:
  BorderBlock leftmost_block_;
  BorderBlock rightmost_block_;
  absl::InlinedVector<const RangeTree::RangeBlock*, 10> middle_blocks_;
};

}  // namespace dfly::search
