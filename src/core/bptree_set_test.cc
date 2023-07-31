// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "core/bptree_set.h"

#include <mimalloc.h>

#include <random>

#include "base/gtest.h"
#include "base/logging.h"
#include "core/mi_memory_resource.h"

using namespace std;

namespace dfly {

class BPTreeSetTest : public ::testing::Test {
  using Node = detail::BPTreeNode<uint64_t>;

 protected:
  BPTreeSetTest() : mi_alloc_(mi_heap_get_backing()), bptree_(&mi_alloc_) {
  }
  static void SetUpTestSuite() {
  }

  bool Validate();

  static bool Validate(const Node* node, uint64_t ubound);

  MiMemoryResource mi_alloc_;
  BPTree<uint64_t> bptree_;
};

bool BPTreeSetTest::Validate(const Node* node, uint64_t ubound) {
  for (unsigned i = 1; i < node->NumItems(); ++i) {
    if (node->Key(i - 1) >= node->Key(i))
      return false;
  }

  if (!node->IsLeaf()) {
    unsigned mask = 0;
    uint32_t subtree_cnt = node->NumItems();
    for (unsigned i = 0; i <= node->NumItems(); ++i) {
      mask |= (1 << node->Child(i)->IsLeaf());
      DCHECK_EQ(node->Child(i)->DEBUG_TreeCount(), node->Child(i)->TreeCount());
      subtree_cnt += node->Child(i)->TreeCount();
    }
    if (mask == 3)
      return false;

    if (subtree_cnt != node->TreeCount()) {
      LOG(ERROR) << "Expected " << subtree_cnt << " got " << node->TreeCount();
      return false;
    }
  }

  return node->Key(node->NumItems() - 1) < ubound;
}

bool BPTreeSetTest::Validate() {
  auto* root = bptree_.DEBUG_root();
  if (!root)
    return true;

  // node, upper bound
  std::vector<pair<Node*, uint64_t>> stack;

  stack.emplace_back(root, UINT64_MAX);

  while (!stack.empty()) {
    Node* node = stack.back().first;
    uint64_t ubound = stack.back().second;
    stack.pop_back();

    if (!Validate(node, ubound))
      return false;

    if (!node->IsLeaf()) {
      for (unsigned i = 0; i < node->NumItems(); ++i) {
        stack.emplace_back(node->Child(i), node->Key(i));
      }
      stack.emplace_back(node->Child(node->NumItems()), ubound);
    }
  }
  return true;
}

TEST_F(BPTreeSetTest, BPtreeInsert) {
  mt19937 generator(1);

  for (unsigned i = 1; i < 7000; ++i) {
    ASSERT_TRUE(bptree_.Insert(i));
    ASSERT_EQ(i, bptree_.Size());
    ASSERT_EQ(i - 1, bptree_.GetRank(i));
    // ASSERT_TRUE(Validate()) << i;
  }
  ASSERT_TRUE(Validate());

  ASSERT_GT(mi_alloc_.used(), 56000u);
  ASSERT_LT(mi_alloc_.used(), 66000u);

  for (unsigned i = 1; i < 7000; ++i) {
    ASSERT_TRUE(bptree_.Contains(i));
  }

  bptree_.Clear();
  ASSERT_EQ(mi_alloc_.used(), 0u);

  uniform_int_distribution<uint64_t> dist(0, 100000);
  for (unsigned i = 0; i < 20000; ++i) {
    bptree_.Insert(dist(generator));
    // ASSERT_TRUE(Validate()) << i;
  }
  ASSERT_TRUE(Validate());
  ASSERT_GT(mi_alloc_.used(), 10000u);
  LOG(INFO) << bptree_.Height() << " " << bptree_.Size();

  bptree_.Clear();
  ASSERT_EQ(mi_alloc_.used(), 0u);

  for (unsigned i = 20000; i > 1; --i) {
    bptree_.Insert(i);
  }
  ASSERT_TRUE(Validate());
  for (unsigned i = 2; i <= 20000; ++i) {
    ASSERT_EQ(i - 2, bptree_.GetRank(i));
  }

  LOG(INFO) << bptree_.Height() << " " << bptree_.Size();
  ASSERT_GT(mi_alloc_.used(), 20000 * 8);
  ASSERT_LT(mi_alloc_.used(), 20000 * 10);
  bptree_.Clear();
  ASSERT_EQ(mi_alloc_.used(), 0u);
}

TEST_F(BPTreeSetTest, Delete) {
  for (unsigned i = 31; i > 10; --i) {
    bptree_.Insert(i);
  }

  for (unsigned i = 1; i < 10; ++i) {
    ASSERT_FALSE(bptree_.Delete(i));
  }

  for (unsigned i = 11; i < 32; ++i) {
    ASSERT_TRUE(bptree_.Delete(i));
  }
  ASSERT_EQ(mi_alloc_.used(), 0u);
  ASSERT_EQ(bptree_.Size(), 0u);

  constexpr size_t kNumElems = 7000;
  for (unsigned i = 0; i < kNumElems; ++i) {
    bptree_.Insert(i);
  }

  ASSERT_GT(bptree_.NodeCount(), 2u);
  unsigned sz = bptree_.Size();
  for (unsigned i = 0; i < kNumElems; ++i) {
    ASSERT_TRUE(bptree_.Delete(i));
    ASSERT_EQ(bptree_.Size(), --sz);
  }

  ASSERT_EQ(mi_alloc_.used(), 0u);
  ASSERT_EQ(bptree_.Size(), 0u);
  ASSERT_EQ(bptree_.Height(), 0u);
  ASSERT_EQ(bptree_.NodeCount(), 0u);
}

}  // namespace dfly
