// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/block_list.h"

#include <absl/container/btree_set.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <map>

#include "base/gtest.h"
#include "base/logging.h"

namespace dfly::search {

using namespace std;

class BlockListTest : public ::testing::Test {
 protected:
};

TEST_F(BlockListTest, LoopMidInsertErase) {
  const size_t kNumElements = 1000;

  // Create list with small block size to test blocking mechanism more extensively
  BlockList<CompressedSortedSet> list{PMR_NS::get_default_resource(), 10};
  for (size_t i = 0; i < kNumElements / 2; i++) {
    list.Insert(i);
    list.Insert(i + kNumElements / 2);
  }

  vector<int> out(list.begin(), list.end());
  ASSERT_EQ(out.size(), kNumElements);
  for (size_t i = 0; i < kNumElements; i++)
    ASSERT_EQ(out[i], i);

  for (size_t i = 0; i < kNumElements / 2; i++) {
    list.Remove(i);
    list.Remove(i + kNumElements / 2);
  }

  out = {list.begin(), list.end()};
  EXPECT_EQ(out.size(), 0u);
}

TEST_F(BlockListTest, InsertReverseRemoveSteps) {
  const size_t kNumElements = 1000;

  BlockList<CompressedSortedSet> list{PMR_NS::get_default_resource(), 10};

  for (size_t i = 0; i < kNumElements; i++) {
    list.Insert(kNumElements - i - 1);
  }

  for (size_t deleted_pref = 0; deleted_pref < 10; deleted_pref++) {
    vector<DocId> out{list.begin(), list.end()};
    reverse(out.begin(), out.end());

    EXPECT_EQ(out.size(), kNumElements / 10 * (10 - deleted_pref));
    for (size_t i = 0; i < kNumElements; i++) {
      if (i % 10 >= deleted_pref) {
        EXPECT_EQ(out.back(), DocId(i));
        out.pop_back();
      }
    }

    for (size_t i = 0; i < kNumElements; i++) {
      if (i % 10 == deleted_pref)
        list.Remove(i);
    }
  }

  EXPECT_EQ(list.size(), 0u);
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
