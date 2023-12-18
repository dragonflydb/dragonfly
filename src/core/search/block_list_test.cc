// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/block_list.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>

#include "base/gtest.h"
#include "base/logging.h"

namespace dfly::search {

using namespace std;

class BlockListTest : public ::testing::Test {
 protected:
};

TEST_F(BlockListTest, LoopInsertErase) {
  const size_t kNumElements = 1000;

  BlockList<CompressedSortedSet> list{PMR_NS::get_default_resource()};
  for (int i = 0; i < kNumElements / 2; i++) {
    list.Insert(i);
    list.Insert(i + kNumElements / 2);
  }

  vector<int> out(list.begin(), list.end());
  ASSERT_EQ(out.size(), kNumElements);
  for (int i = 0; i < kNumElements; i++)
    ASSERT_EQ(out[i], i);

  for (int i = 0; i < kNumElements / 2; i++) {
    list.Remove(i);
    list.Remove(i + kNumElements / 2);
  }

  out = {list.begin(), list.end()};
  EXPECT_EQ(out.size(), 0);
}

static void BM_Erase90PctTail(benchmark::State& state) {
  BlockList<CompressedSortedSet> bl{PMR_NS::get_default_resource()};

  unsigned size = state.range(0);
  for (int i = 0; i < size; i++)
    bl.Insert(i);

  size_t base = size / 10;
  size_t i = 0;
  while (state.KeepRunning()) {
    benchmark::DoNotOptimize(bl.Remove(base + i));
    i = (i + 1) % (size * 9 / 10);
  }
}

BENCHMARK(BM_Erase90PctTail)->Args({100'000});

/*
TEST_F(BlockListTest, MergeBench) {
  CompressedSortedSet l1{PMR_NS::get_default_resource()}, l2{PMR_NS::get_default_resource()};
  vector<int> v1, v2;
  BlockList bl1, bl2;

  auto start = clock();

  for (int i = 0; i < 1'000'000; i++) {
    //l1.Insert(i);
    //l2.Insert(i);
    //v1.push_back(i);
    //v2.push_back(i);
    bl1.Insert(i);
    bl2.Insert(i);
  }

  VLOG(0) << "Filling: " << (clock() - start);

  vector<int> vout;
  vout.reserve(v1.size());

  for (int i = 0; i < 10; i++) {
    vout.clear();
    start = clock();
    //set_intersection(l1.begin(), l1.end(), l2.begin(), l2.end(), back_inserter(vout));
    //set_intersection(v1.begin(), v1.end(), v2.begin(), v2.end(), back_inserter(vout));
    set_intersection(bl1.begin(), bl1.end(), bl2.begin(), bl2.end(), back_inserter(vout));
    VLOG(0) << (clock() - start);
  }
}
*/

}  // namespace dfly::search
