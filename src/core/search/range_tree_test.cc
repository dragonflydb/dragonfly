// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/range_tree.h"

#include <absl/random/random.h>
#include <benchmark/benchmark.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <utility>

#include "base/gtest.h"
#include "base/logging.h"

namespace dfly::search {

class RangeTreeTest : public testing::Test {
 protected:
};

static constexpr double kMinRangeValue = std::numeric_limits<double>::min();
static constexpr double kMaxRangeValue = std::numeric_limits<double>::max();

using Entry = std::pair<DocId, double>;
using BlocksList = absl::InlinedVector<const RangeTree::RangeBlock*, 5>;

std::vector<Entry> ExtractDocPairs(const BlocksList& result) {
  std::vector<Entry> out;
  for (const auto& block : result) {
    for (const auto& entry : *block) {
      out.push_back(entry);
    }
  }
  return out;
}

std::vector<std::vector<Entry>> ExtractAllBlocks(const BlocksList& result) {
  std::vector<std::vector<Entry>> all;
  for (const auto& block : result) {
    std::vector<Entry> block_entries;
    for (const auto& entry : *block) {
      block_entries.push_back(entry);
    }
    all.push_back(std::move(block_entries));
  }
  return all;
}

MATCHER_P(UnorderedElementsAreDocPairsMatcher, expected_matchers, "") {
  return testing::ExplainMatchResult(testing::UnorderedElementsAreArray(expected_matchers),
                                     ExtractDocPairs(arg), result_listener);
}

MATCHER_P(BlocksAreMatcher, expected_blocks, "") {
  std::vector<testing::Matcher<std::vector<Entry>>> matchers;
  for (const auto& expected_entries : expected_blocks) {
    matchers.push_back(testing::UnorderedElementsAreArray(expected_entries));
  }
  return testing::ExplainMatchResult(testing::ElementsAreArray(matchers), ExtractAllBlocks(arg),
                                     result_listener);
}

auto UnorderedElementsAreDocPairs(std::vector<Entry> list) {
  return UnorderedElementsAreDocPairsMatcher(std::move(list));
}

auto BlocksAre(std::initializer_list<std::vector<Entry>> blocks) {
  return BlocksAreMatcher(std::vector<std::vector<Entry>>(blocks));
}

std::vector<DocId> ExtractDocIdsFromRange(const std::vector<Entry>& entries, double l, double r) {
  std::vector<DocId> result;
  for (const auto& entry : entries) {
    if (entry.second >= l && entry.second <= r) {
      result.push_back(entry.first);
    }
  }

  std::sort(result.begin(), result.end());
  result.erase(std::unique(result.begin(), result.end()), result.end());
  return result;
}

std::vector<DocId> MergeTwoBlocksRangeResult(const RangeTree& tree, double l, double r) {
  auto result = tree.Range(l, r).GetResult();
  DCHECK(std::holds_alternative<TwoBlocksRangeResult>(result));
  auto& two_blocks_result = std::get<TwoBlocksRangeResult>(result);
  return {two_blocks_result.begin(), two_blocks_result.end()};
}

TEST_F(RangeTreeTest, AddSimple) {
  RangeTree tree{PMR_NS::get_default_resource()};

  // Add some values
  tree.Add(1, 10.0);
  tree.Add(2, 20.0);
  tree.Add(2, 10.0);
  tree.Add(3, 30.0);
  tree.Add(4, 40.0);
  tree.Add(4, 60.0);

  auto result = tree.GetAllBlocks();
  EXPECT_THAT(result, UnorderedElementsAreDocPairs(
                          {{1, 10.0}, {2, 10.0}, {2, 20.0}, {3, 30.0}, {4, 40.0}, {4, 60.0}}));
}

TEST_F(RangeTreeTest, Add) {
  RangeTree tree{PMR_NS::get_default_resource(), 2};

  // Add some values
  tree.Add(1, 10.0);
  tree.Add(1, 20.0);
  tree.Add(2, 20.0);
  tree.Add(3, 20.0);
  tree.Add(4, 30.0);
  tree.Add(5, 30.0);
  tree.Add(6, 30.0);

  auto result = tree.RangeBlocks(10.0, 30.0);
  EXPECT_THAT(result,
              UnorderedElementsAreDocPairs(
                  {{1, 10.0}, {1, 20.0}, {2, 20.0}, {3, 20.0}, {4, 30.0}, {5, 30.0}, {6, 30.0}}));

  // Test that the ranges was split correctly
  result = tree.RangeBlocks(kMinRangeValue, 19.0);
  EXPECT_THAT(result, UnorderedElementsAreDocPairs({{1, 10.0}}));

  result = tree.RangeBlocks(20.0, 29.0);
  EXPECT_THAT(result, UnorderedElementsAreDocPairs({{1, 20.0}, {2, 20.0}, {3, 20.0}}));

  result = tree.RangeBlocks(30.0, kMaxRangeValue);
  EXPECT_THAT(result, UnorderedElementsAreDocPairs({{4, 30.0}, {5, 30.0}, {6, 30.0}}));
}

TEST_F(RangeTreeTest, RemoveSimple) {
  RangeTree tree{PMR_NS::get_default_resource(), 2};

  // Add some values
  tree.Add(1, 10.0);
  tree.Add(2, 20.0);
  tree.Add(3, 30.0);
  tree.Add(4, 40.0);

  // Remove some values
  tree.Remove(1, 10.0);
  tree.Remove(2, 20.0);

  auto result = tree.GetAllBlocks();
  EXPECT_THAT(result, UnorderedElementsAreDocPairs({{3, 30.0}, {4, 40.0}}));
}

TEST_F(RangeTreeTest, Remove) {
  using Container = std::vector<Entry>;

  Container expected_values;
  RangeTree tree{PMR_NS::get_default_resource(), 2};

  const long long max_value = 100;
  long long step = 23;
  long long current_value = max_value;

  auto do_add = [&](DocId i) {
    const double value = static_cast<double>(current_value);
    auto it = std::find(expected_values.begin(), expected_values.end(), std::make_pair(i, value));

    if (it != expected_values.end()) {
      // If the value already exists, we do not add it again
      // The problem is that for now RangeTree does not support duplicates
      // TODO: fix this
      return;
    }

    // Otherwise, we add it to the expected values and to the tree
    expected_values.emplace_back(i, value);
    tree.Add(i, value);
    current_value = (max_value + current_value - step) % max_value;
  };

  auto add_entries_with_step = [&](size_t step) {
    for (size_t i = 0; i < 100; i += step) {
      do_add(i);
    }
  };

  auto do_remove = [&](size_t i) {
    auto pair = expected_values[i];
    tree.Remove(pair.first, pair.second);
  };

  auto remove_entries_with_step = [&](size_t step) {
    Container expected_values_copy;
    for (size_t i = 0; i < expected_values.size(); i++) {
      if (i % step == 0) {
        do_remove(i);
      } else {
        expected_values_copy.push_back(expected_values[i]);
      }
    }
    expected_values = std::move(expected_values_copy);
  };

  // First wave of Add and Remove
  add_entries_with_step(1);

  step = 37;
  current_value = max_value;
  add_entries_with_step(3);

  // Remove some values
  remove_entries_with_step(3);

  auto result = tree.GetAllBlocks();
  EXPECT_THAT(result, UnorderedElementsAreDocPairs(expected_values));

  // Second wave of Add and Remove
  step = 31;
  current_value = max_value;
  add_entries_with_step(5);

  // Remove a first half of the values
  remove_entries_with_step(2);

  result = tree.GetAllBlocks();
  EXPECT_THAT(result, UnorderedElementsAreDocPairs(expected_values));

  // Remove all values
  remove_entries_with_step(1);

  result = tree.GetAllBlocks();
  EXPECT_THAT(result, UnorderedElementsAreDocPairs({}));
}

TEST_F(RangeTreeTest, RangeSimple) {
  RangeTree tree{PMR_NS::get_default_resource(), 1};

  // Add some values
  tree.Add(1, 10.0);
  tree.Add(1, 20.0);
  tree.Add(2, 20.0);
  tree.Add(2, 30.0);
  tree.Add(3, 30.0);
  tree.Add(3, 40.0);
  tree.Add(4, 40.0);

  auto result = tree.RangeBlocks(10.0, 10.0);
  EXPECT_THAT(result, BlocksAre({{{1, 10.0}}}));

  result = tree.RangeBlocks(20.0, 20.0);
  EXPECT_THAT(result, BlocksAre({{{1, 20.0}, {2, 20.0}}}));

  result = tree.RangeBlocks(30.0, 30.0);
  EXPECT_THAT(result, BlocksAre({{{2, 30.0}, {3, 30.0}}}));

  result = tree.RangeBlocks(40.0, 40.0);
  EXPECT_THAT(result, BlocksAre({{{3, 40.0}, {4, 40.0}}}));

  result = tree.RangeBlocks(10.0, 30.0);
  EXPECT_THAT(result, BlocksAre({{{1, 10.0}}, {{1, 20.0}, {2, 20.0}}, {{2, 30.0}, {3, 30.0}}}));

  result = tree.RangeBlocks(20.0, 40.0);
  EXPECT_THAT(result,
              BlocksAre({{{1, 20.0}, {2, 20.0}}, {{2, 30.0}, {3, 30.0}}, {{3, 40.0}, {4, 40.0}}}));

  result = tree.RangeBlocks(10.0, 40.0);
  EXPECT_THAT(
      result,
      BlocksAre(
          {{{1, 10.0}}, {{1, 20.0}, {2, 20.0}}, {{2, 30.0}, {3, 30.0}}, {{3, 40.0}, {4, 40.0}}}));
}

TEST_F(RangeTreeTest, Range) {
  {
    RangeTree tree{PMR_NS::get_default_resource(), 4};

    tree.Add(1, 10.0);
    tree.Add(1, 20.0);
    tree.Add(2, 20.0);
    tree.Add(3, 30.0);
    tree.Add(4, 20.0);
    tree.Add(4, 30.0);

    auto result = tree.RangeBlocks(10.0, 30.0);
    EXPECT_THAT(
        result,
        BlocksAre({{{1, 10.0}}, {{1, 20.0}, {2, 20.0}, {4, 20.0}}, {{3, 30.0}, {4, 30.0}}}));
  }

  {
    RangeTree tree{PMR_NS::get_default_resource(), 4};

    tree.Add(1, 10.0);
    tree.Add(1, 20.0);
    tree.Add(2, 20.0);
    tree.Add(3, 20.0);
    tree.Add(4, 20.0);

    auto result = tree.RangeBlocks(10.0, 20.0);
    EXPECT_THAT(result, BlocksAre({{{1, 10.0}}, {{1, 20.0}, {2, 20.0}, {3, 20.0}, {4, 20.0}}}));
  }

  {
    RangeTree tree{PMR_NS::get_default_resource(), 4};

    tree.Add(1, 10.0);
    tree.Add(2, 10.0);
    tree.Add(3, 10.0);
    tree.Add(4, 20.0);
    tree.Add(4, 10.0);

    auto result = tree.RangeBlocks(10.0, 20.0);
    EXPECT_THAT(result, BlocksAre({{{1, 10.0}, {2, 10.0}, {3, 10.0}, {4, 10.0}}, {{4, 20.0}}}));
  }
}

// Don't split single block with same value
TEST_F(RangeTreeTest, SingleBlockSplit) {
  RangeTree tree{PMR_NS::get_default_resource(), 4};

  for (DocId id = 1; id <= 16; id++)
    tree.Add(id, 5.0);

  // One split was made to create an empty leftmost block
  auto stats = tree.GetStats();
  EXPECT_EQ(stats.splits, 1u);
  EXPECT_EQ(stats.block_count, 2u);

  // Add value that causes a new block to be started
  tree.Add(20, 6.0);

  stats = tree.GetStats();
  EXPECT_EQ(stats.splits, 1u);       // detected ahead, so no split
  EXPECT_EQ(stats.block_count, 3u);  // but new block

  // No more splits with same 5.0
  tree.Add(17, 5.0);
  stats = tree.GetStats();
  EXPECT_EQ(stats.splits, 1u);

  // Verify block sizes
  auto blocks = tree.GetAllBlocks();
  EXPECT_EQ(blocks[0]->Size(), 0u);
  EXPECT_EQ(blocks[1]->Size(), 17u);
  EXPECT_EQ(blocks[2]->Size(), 1u);
}

// Make tree split and then delete every nth value to see if blocks merge properly
TEST_F(RangeTreeTest, BlockMerge) {
  RangeTree tree{PMR_NS::get_default_resource(), 8};
  for (DocId id = 1; id <= 64; id++)
    tree.Add(id, id);

  auto stats = tree.GetStats();
  uint64_t splits = stats.splits;
  EXPECT_GT(splits, 8u);

  // Blocks have at least half occupancy
  EXPECT_GT(stats.block_count, 64 / 8);
  EXPECT_LT(stats.block_count, 2 * 64 / 8);

  // Delete all except  %8 = 0, should trigger merge
  std::vector<Entry> expected;
  for (DocId id = 1; id <= 64; id++) {
    if (id % 8)
      tree.Remove(id, id);
    else
      expected.emplace_back(id, id);
  }

  // Only one block left now
  stats = tree.GetStats();
  size_t blocks = stats.block_count;
  EXPECT_LT(blocks, 4u);
  EXPECT_EQ(stats.merges + blocks - 1, splits);

  // Check the two entries remained
  auto result = tree.GetAllBlocks();
  EXPECT_THAT(result, UnorderedElementsAreDocPairs(expected));
}

TEST_F(RangeTreeTest, BugNotUniqueDoubleValues) {
  // TODO: fix the bug
  GTEST_SKIP() << "Bug not fixed yet";

  RangeTree tree{PMR_NS::get_default_resource()};

  tree.Add(1, 10.0);
  tree.Add(1, 10.0);
  tree.Remove(1, 10.0);

  auto result = tree.GetAllBlocks();
  EXPECT_THAT(result, BlocksAre({{{1, 10.0}}}));
}

TEST_F(RangeTreeTest, RangeResultTwoBlocksSimple) {
  RangeTree tree{PMR_NS::get_default_resource(), 4};

  // First block: [[1, 10.0], [16, 12.0], [12, 15.0], [5, 17.0]]
  // Second block: [[8, 20.0], [5, 30.0], [12, 50.0], [20, 55.0]]
  // [10.0, 12.0, 15.0, 17.0] | [20.0, 30.0, 50.0, 55.0]
  tree.Add(1, 10.0);   // 1
  tree.Add(5, 30.0);   // 2
  tree.Add(20, 55.0);  // 2
  tree.Add(5, 17.0);   // 1
  tree.Add(8, 20.0);   // 2
  tree.Add(16, 12.0);  // 1
  tree.Add(12, 15.0);  // 1
  tree.Add(12, 50.0);  // 2

  EXPECT_THAT(tree.RangeBlocks(10.0, 55.0),
              BlocksAre({{{1, 10.0}, {16, 12.0}, {12, 15.0}, {5, 17.0}},
                         {{8, 20.0}, {5, 30.0}, {12, 50.0}, {20, 55.0}}}));

  std::vector<Entry> entries = {{1, 10.0}, {16, 12.0}, {12, 15.0}, {5, 17.0},
                                {8, 20.0}, {5, 30.0},  {12, 50.0}, {20, 55.0}};

  for (size_t i = 0; i < entries.size() / 2; i++) {
    const double l = entries[i].second;
    for (size_t j = entries.size() / 2; j < entries.size(); j++) {
      const double r = entries[j].second;
      auto range_result = MergeTwoBlocksRangeResult(tree, l, r);
      EXPECT_THAT(range_result, testing::ElementsAreArray(ExtractDocIdsFromRange(entries, l, r)));
    }
  }
}

TEST_F(RangeTreeTest, RangeResultTwoBlocks) {
  RangeTree tree{PMR_NS::get_default_resource(), 50};

  const long long max_value = 100;
  long long step = 23;
  long long current_value = max_value;

  std::vector<Entry> entries;
  for (size_t i = 0; i < 20; i++) {
    const double value = static_cast<double>(current_value);
    entries.emplace_back(i, value);
    entries.emplace_back(i, value + 100.0);
    current_value = (max_value + current_value - step) % max_value;
  }
  for (size_t i = 20; i < 80; i++) {
    const double value = static_cast<double>(current_value);
    entries.emplace_back(i, value);
    current_value = (max_value + current_value - step) % max_value;
  }

  DCHECK(entries.size() == 100);

  std::sort(entries.begin(), entries.end(),
            [](const Entry& a, const Entry& b) { return a.second < b.second; });

  auto add_entries = [&tree, &entries](size_t start, size_t end) {
    for (size_t i = start; i < end; i++) {
      tree.Add(entries[i].first, entries[i].second);
    }
  };

  add_entries(0, 25);
  add_entries(50, 76);
  add_entries(25, 50);
  add_entries(76, entries.size());

  for (size_t i = 0; i < 50; i++) {
    const double l = entries[i].second;
    for (size_t j = 50; j < entries.size(); j++) {
      const double r = entries[j].second;
      auto range_result = MergeTwoBlocksRangeResult(tree, l, r);
      EXPECT_THAT(range_result, testing::ElementsAreArray(ExtractDocIdsFromRange(entries, l, r)));
    }
  }
}

TEST_F(RangeTreeTest, FinalizeInitialization) {
  RangeTree tree{PMR_NS::get_default_resource(), 1, false};

  // Add some values
  tree.Add(1, 10.0);
  tree.Add(2, 20.0);
  tree.Add(3, 20.0);
  tree.Add(4, 30.0);
  tree.Add(5, 20.0);
  tree.Add(6, 30.0);
  tree.Add(7, 40.0);

  auto result = tree.RangeBlocks(10.0, 40.0);
  EXPECT_THAT(
      result,
      BlocksAre({{{1, 10.0}, {2, 20.0}, {3, 20.0}, {4, 30.0}, {5, 20.0}, {6, 30.0}, {7, 40.0}}}));

  tree.FinalizeInitialization();

  result = tree.RangeBlocks(10.0, 40.0);
  EXPECT_THAT(
      result,
      BlocksAre(
          {{{1, 10.0}}, {{2, 20.0}, {3, 20.0}, {5, 20.0}}, {{4, 30.0}, {6, 30.0}}, {{7, 40.0}}}));
}

// Benchmark tree insertion performance with set of discrete values
static void BM_DiscreteInsertion(benchmark::State& state) {
  RangeTree tree{PMR_NS::get_default_resource()};

  absl::InsecureBitGen gen{};
  size_t variety = state.range(0);

  DocId id = 0;
  for (auto _ : state) {
    double v = absl::Uniform(gen, 0u, variety);
    tree.Add(id++, v);
  }
}

BENCHMARK(BM_DiscreteInsertion)->Arg(2)->Arg(12)->Arg(128)->Arg(1024);

}  // namespace dfly::search
