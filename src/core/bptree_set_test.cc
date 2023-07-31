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
  BPTreeSetTest() : mi_alloc_(mi_heap_get_backing()), bPtree_(&mi_alloc_) {
  }
  static void SetUpTestSuite() {
  }

  bool Validate();

  static bool Validate(Node* node, uint64_t ubound);

  MiMemoryResource mi_alloc_;
  BPTree<uint64_t> bPtree_;
};

bool BPTreeSetTest::Validate(Node* node, uint64_t ubound) {
  if (node->NumItems() <= 1)
    return false;

  for (unsigned i = 1; i < node->NumItems(); ++i) {
    if (node->Key(i - 1) >= node->Key(i))
      return false;
  }

  return node->Key(node->NumItems() - 1) < ubound;
}

bool BPTreeSetTest::Validate() {
  auto* root = bPtree_.DEBUG_root();
  if (!root)
    return true;

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
    bPtree_.Insert(i);
  }
  ASSERT_TRUE(Validate());

  ASSERT_GT(mi_alloc_.used(), 56000u);
  ASSERT_LT(mi_alloc_.used(), 66000u);

  for (unsigned i = 1; i < 7000; ++i) {
    ASSERT_TRUE(bPtree_.Contains(i));
  }

  bPtree_.Clear();
  ASSERT_EQ(mi_alloc_.used(), 0u);

  uniform_int_distribution<uint64_t> dist(0, 100000);
  for (unsigned i = 0; i < 20000; ++i) {
    bPtree_.Insert(dist(generator));
  }
  LOG(INFO) << bPtree_.Height() << " " << bPtree_.Size();

  ASSERT_TRUE(Validate());
  ASSERT_GT(mi_alloc_.used(), 10000u);
  bPtree_.Clear();
  ASSERT_EQ(mi_alloc_.used(), 0u);

  for (unsigned i = 20000; i > 1; --i) {
    bPtree_.Insert(i);
  }
  ASSERT_TRUE(Validate());

  LOG(INFO) << bPtree_.Height() << " " << bPtree_.Size();
  ASSERT_GT(mi_alloc_.used(), 20000 * 8);
  ASSERT_LT(mi_alloc_.used(), 20000 * 10);
  bPtree_.Clear();
  ASSERT_EQ(mi_alloc_.used(), 0u);
}

}  // namespace dfly
