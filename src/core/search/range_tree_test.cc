// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/range_tree.h"

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

std::vector<Entry> ExtractDocPairs(const RangeResult& result) {
  std::vector<Entry> out;
  for (const auto& block : result.GetBlocks()) {
    for (const auto& entry : *block) {
      out.push_back(entry);
    }
  }
  return out;
}

std::vector<std::vector<Entry>> ExtractAllBlocks(const RangeResult& result) {
  std::vector<std::vector<Entry>> all;
  for (const auto& block : result.GetBlocks()) {
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

TEST_F(RangeTreeTest, AddSimple) {
  RangeTree tree{PMR_NS::get_default_resource()};

  // Add some values
  tree.Add(1, 10.0);
  tree.Add(2, 20.0);
  tree.Add(2, 10.0);
  tree.Add(3, 30.0);
  tree.Add(4, 40.0);
  tree.Add(4, 60.0);

  auto result = tree.GetAllDocIds();
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

  auto result = tree.Range(10.0, 30.0);
  EXPECT_THAT(result, UnorderedElementsAreDocPairs(
                          {{1, 10.0}, {1, 20.0}, {2, 20.0}, {3, 20.0}, {4, 30.0}, {5, 30.0}}));

  // Test that the ranges was split correctly
  result = tree.Range(kMinRangeValue, 19.0);
  EXPECT_THAT(result, UnorderedElementsAreDocPairs({{1, 10.0}}));

  result = tree.Range(20.0, 29.0);
  EXPECT_THAT(result, UnorderedElementsAreDocPairs({{1, 20.0}, {2, 20.0}, {3, 20.0}}));

  result = tree.Range(30.0, kMaxRangeValue);
  EXPECT_THAT(result, UnorderedElementsAreDocPairs({{4, 30.0}, {5, 30.0}}));
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

  auto result = tree.GetAllDocIds();
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

  auto result = tree.GetAllDocIds();
  EXPECT_THAT(result, UnorderedElementsAreDocPairs(expected_values));

  // Second wave of Add and Remove
  step = 31;
  current_value = max_value;
  add_entries_with_step(5);

  // Remove a first half of the values
  remove_entries_with_step(2);

  result = tree.GetAllDocIds();
  EXPECT_THAT(result, UnorderedElementsAreDocPairs(expected_values));

  // Remove all values
  remove_entries_with_step(1);

  result = tree.GetAllDocIds();
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

  auto result = tree.Range(10.0, 10.0);
  EXPECT_THAT(result, BlocksAre({{{1, 10.0}}}));

  result = tree.Range(20.0, 20.0);
  EXPECT_THAT(result, BlocksAre({{{1, 20.0}, {2, 20.0}}}));

  result = tree.Range(30.0, 30.0);
  EXPECT_THAT(result, BlocksAre({{{2, 30.0}, {3, 30.0}}}));

  result = tree.Range(40.0, 40.0);
  EXPECT_THAT(result, BlocksAre({{{3, 40.0}, {4, 40.0}}}));

  result = tree.Range(10.0, 30.0);
  EXPECT_THAT(result, BlocksAre({{{1, 10.0}}, {{1, 20.0}, {2, 20.0}}, {{2, 30.0}, {3, 30.0}}}));

  result = tree.Range(20.0, 40.0);
  EXPECT_THAT(result,
              BlocksAre({{{1, 20.0}, {2, 20.0}}, {{2, 30.0}, {3, 30.0}}, {{3, 40.0}, {4, 40.0}}}));

  result = tree.Range(10.0, 40.0);
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

    auto result = tree.Range(10.0, 30.0);
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

    auto result = tree.Range(10.0, 20.0);
    EXPECT_THAT(result, BlocksAre({{{1, 10.0}}, {{1, 20.0}, {2, 20.0}, {3, 20.0}, {4, 20.0}}}));
  }

  {
    RangeTree tree{PMR_NS::get_default_resource(), 4};

    tree.Add(1, 10.0);
    tree.Add(2, 10.0);
    tree.Add(3, 10.0);
    tree.Add(4, 20.0);
    tree.Add(4, 10.0);

    auto result = tree.Range(10.0, 20.0);
    EXPECT_THAT(result, BlocksAre({{{1, 10.0}, {2, 10.0}, {3, 10.0}, {4, 10.0}}, {{4, 20.0}}}));
  }
}

TEST_F(RangeTreeTest, BugNotUniqueDoubleValues) {
  // TODO: fix the bug
  GTEST_SKIP() << "Bug not fixed yet";

  RangeTree tree{PMR_NS::get_default_resource()};

  tree.Add(1, 10.0);
  tree.Add(1, 10.0);
  tree.Remove(1, 10.0);

  auto result = tree.GetAllDocIds();
  EXPECT_THAT(result, BlocksAre({{{1, 10.0}}}));
}

}  // namespace dfly::search
