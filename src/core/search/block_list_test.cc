// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/block_list.h"

#include <absl/container/btree_set.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <random>
#include <set>

#include "base/gtest.h"
#include "base/logging.h"

namespace dfly::search {

using namespace std;

template <typename C> class TemplatedBlockListTest : public testing::Test {
 private:
  using NumericType = long long;

 public:
  using ElementType = typename C::ElementType;

  auto Make() {
    // Create list with small block size to test blocking mechanism more extensively
    return BlockList<C>{PMR_NS::get_default_resource(), 10};
  }

  auto AddNewBlockListElement(DocId doc_id) {
    if constexpr (std::is_same_v<ElementType, DocId>) {
      return ElementType{doc_id};
    } else {
      static_assert(std::is_same_v<ElementType, std::pair<DocId, double>>,
                    "Unsupported ElementType for BlockListTest");

      const NumericType number = dist_(rnd_);
      id_to_values_[doc_id].push_back(number);
      return ElementType{doc_id, static_cast<double>(number)};
    }
  }

  auto RemoveBlockListElement(DocId doc_id) {
    if constexpr (std::is_same_v<ElementType, DocId>) {
      return ElementType{doc_id};
    } else {
      static_assert(std::is_same_v<ElementType, std::pair<DocId, double>>,
                    "Unsupported ElementType for BlockListTest");

      const NumericType number = id_to_values_[doc_id].back();
      id_to_values_[doc_id].pop_back();
      return ElementType{doc_id, static_cast<double>(number)};
    }
  }

  DocId GetDocId(const ElementType& element) {
    if constexpr (std::is_same_v<ElementType, DocId>) {
      return element;
    } else {
      static_assert(std::is_same_v<ElementType, std::pair<DocId, double>>,
                    "Unsupported ElementType for GetDocId");
      return element.first;
    }
  }

 private:
  // Used to save doubles for std::pair<DocId, double>
  std::unordered_map<DocId, std::vector<NumericType>> id_to_values_;

  // Used to generate random numbers for std::pair<DocId, double>
  default_random_engine rnd_;
  uniform_int_distribution<NumericType> dist_{std::numeric_limits<NumericType>::min(),
                                              std::numeric_limits<NumericType>::max()};
};

using ContainerTypes = ::testing::Types<CompressedSortedSet, SortedVector<DocId>,
                                        SortedVector<std::pair<DocId, double>>>;
TYPED_TEST_SUITE(TemplatedBlockListTest, ContainerTypes);

TYPED_TEST(TemplatedBlockListTest, LoopMidInsertErase) {
  using ElementType = typename TypeParam::ElementType;

  const size_t kNumElements = 50;
  auto list = this->Make();

  for (size_t i = 0; i < kNumElements / 2; i++) {
    list.Insert(this->AddNewBlockListElement(i));
    list.Insert(this->AddNewBlockListElement(i + kNumElements / 2));
  }

  vector<ElementType> out(list.begin(), list.end());
  ASSERT_EQ(list.Size(), kNumElements);
  ASSERT_EQ(out.size(), kNumElements);
  for (size_t i = 0; i < kNumElements; i++)
    ASSERT_EQ(this->GetDocId(out[i]), i);

  for (size_t i = 0; i < kNumElements / 2; i++) {
    list.Remove(this->RemoveBlockListElement(i));
    list.Remove(this->RemoveBlockListElement(i + kNumElements / 2));
  }

  out = {list.begin(), list.end()};
  EXPECT_EQ(out.size(), 0u);
}

TYPED_TEST(TemplatedBlockListTest, InsertReverseRemoveSteps) {
  using ElementType = typename TypeParam::ElementType;

  const size_t kNumElements = 1000;
  auto list = this->Make();

  for (size_t i = 0; i < kNumElements; i++) {
    list.Insert(this->AddNewBlockListElement(kNumElements - i - 1));
  }

  for (size_t deleted_pref = 0; deleted_pref < 10; deleted_pref++) {
    vector<ElementType> out{list.begin(), list.end()};
    reverse(out.begin(), out.end());

    EXPECT_EQ(out.size(), kNumElements / 10 * (10 - deleted_pref));
    for (size_t i = 0; i < kNumElements; i++) {
      if (i % 10 >= deleted_pref) {
        EXPECT_EQ(this->GetDocId(out.back()), DocId(i));
        out.pop_back();
      }
    }

    for (size_t i = 0; i < kNumElements; i++) {
      if (i % 10 == deleted_pref)
        list.Remove(this->RemoveBlockListElement(i));
    }
  }

  EXPECT_EQ(list.Size(), 0u);
}

TYPED_TEST(TemplatedBlockListTest, RandomNumbers) {
  using ElementType = typename TypeParam::ElementType;

  const size_t kNumIterations = 1'000;
  auto list = this->Make();
  std::set<ElementType> list_copy;

  for (size_t i = 0; i < kNumIterations; i++) {
    if (list_copy.size() > 100 && rand() % 5 == 0) {
      auto it = list_copy.begin();
      std::advance(it, rand() % list_copy.size());
      list.Remove(*it);
      list_copy.erase(it);
    } else {
      const ElementType t = this->AddNewBlockListElement(rand() % 1'000'000);
      list.Insert(t);
      list_copy.insert(t);
    }

    ASSERT_TRUE(std::equal(list.begin(), list.end(), list_copy.begin(), list_copy.end()));
  }
}

class BlockListTest : public testing::Test {
 protected:
};

TEST_F(BlockListTest, Split) {
  BlockList<SortedVector<std::pair<DocId, double>>> bl{PMR_NS::get_default_resource(), 20};

  const size_t max_value = 100.0;
  const size_t step = 23.0;
  size_t value = max_value;
  for (size_t i = 0; i < 100; i++) {
    bl.Insert({i, static_cast<double>(value)});
    value = (max_value + value - step) % max_value;
  }

  auto split_result = Split(std::move(bl));
  auto& left = split_result.left;
  auto& right = split_result.right;

  EXPECT_EQ(left.Size(), 50);
  EXPECT_EQ(right.Size(), 50);

  // Test that all values in the left part are less than or equal to max_value
  for (const auto& [_, left_value] : left) {
    for (const auto& [__, right_value] : right) {
      EXPECT_LE(left_value, right_value);
    }
  }

  double median = split_result.median;

  // Test that left part values do not have this median
  for (const auto& [_, left_value] : left) {
    EXPECT_NE(left_value, median);
  }

  // Test that right part values do have this median
  bool is_median_found = false;
  for (const auto& [_, right_value] : right) {
    if (right_value == median) {
      is_median_found = true;
      break;
    }
  }

  EXPECT_TRUE(is_median_found);

  // Test that doc_ids in both parts are sorted
  DocId prev_doc_id = std::numeric_limits<DocId>::min();
  for (const auto& [doc_id, _] : left) {
    EXPECT_GE(doc_id, prev_doc_id);
    prev_doc_id = doc_id;
  }

  prev_doc_id = std::numeric_limits<DocId>::min();
  for (const auto& [doc_id, _] : right) {
    EXPECT_GE(doc_id, prev_doc_id);
    prev_doc_id = doc_id;
  }
}

TEST_F(BlockListTest, SplitHard) {
  // First test 70 values on the left and 30 on the right
  BlockList<SortedVector<std::pair<DocId, double>>> bl1{PMR_NS::get_default_resource(), 20};

  for (size_t i = 0; i < 70; i++) {
    bl1.Insert({i, 1.0});
  }
  for (size_t i = 70; i < 100; i++) {
    bl1.Insert({i, 2.0});
  }

  auto split_result1 = Split(std::move(bl1));

  EXPECT_EQ(split_result1.median, 2.0);
  EXPECT_EQ(split_result1.left.Size(), 70u);
  EXPECT_EQ(split_result1.right.Size(), 30u);

  for (const auto& [_, value] : split_result1.left) {
    EXPECT_EQ(value, 1.0);
  }

  for (const auto& [_, value] : split_result1.right) {
    EXPECT_EQ(value, 2.0);
  }

  // Now test 30 values on the left and 70 on the right
  BlockList<SortedVector<std::pair<DocId, double>>> bl2{PMR_NS::get_default_resource(), 20};
  for (size_t i = 0; i < 30; i++) {
    bl2.Insert({i, 1.0});
  }
  for (size_t i = 30; i < 100; i++) {
    bl2.Insert({i, 2.0});
  }
  auto split_result2 = Split(std::move(bl2));

  EXPECT_EQ(split_result2.median, 2.0);
  EXPECT_EQ(split_result2.left.Size(), 30u);
  EXPECT_EQ(split_result2.right.Size(), 70u);

  for (const auto& [_, value] : split_result2.left) {
    EXPECT_EQ(value, 1.0);
  }

  for (const auto& [_, value] : split_result2.right) {
    EXPECT_EQ(value, 2.0);
  }
}

TEST_F(BlockListTest, SplitSingleDoubleValue) {
  BlockList<SortedVector<std::pair<DocId, double>>> bl{PMR_NS::get_default_resource(), 20};

  for (size_t i = 0; i < 100; i++) {
    bl.Insert({i, 1.0});
  }

  auto split_result = Split(std::move(bl));
  auto& left = split_result.left;
  auto& right = split_result.right;

  EXPECT_EQ(left.Size(), 0u);
  EXPECT_EQ(right.Size(), 100u);
  EXPECT_EQ(split_result.median, 1.0);
}

static void BM_Erase90PctTail(benchmark::State& state) {
  BlockList<CompressedSortedSet> bl{PMR_NS::get_default_resource()};

  unsigned size = state.range(0);
  for (size_t i = 0; i < size; i++)
    bl.Insert(i);

  size_t base = size / 10;
  size_t i = 0;
  while (state.KeepRunning()) {
    benchmark::DoNotOptimize(bl.Remove(base + i));
    i = (i + 1) % (size * 9 / 10);
  }
}

BENCHMARK(BM_Erase90PctTail)->Args({100'000});

}  // namespace dfly::search
