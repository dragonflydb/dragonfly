// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/detail/listpack.h"

#include <gmock/gmock.h>
#include <mimalloc.h>

#include "base/gtest.h"
#include "base/logging.h"

extern "C" {
#include "redis/listpack.h"
#include "redis/zmalloc.h"
}

namespace dfly {
namespace detail {

using namespace std;
using namespace testing;

class ListPackTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    init_zmalloc_threadlocal(mi_heap_get_backing());
  }

  void SetUp() override {
    ptr_ = lpNew(0);
    lp_ = ListPack(ptr_);
  }

  void TearDown() override {
    ptr_ = lp_.GetPointer();
    lpFree(ptr_);
    // Ensure there are no memory leaks after every test
    EXPECT_EQ(zmalloc_used_memory_tl, 0);
  }

  unsigned Remove(string_view elem, unsigned count, QList::Where where) {
    return lp_.Remove(CollectionEntry{elem.data(), elem.size()}, count, where);
  }

  ListPack lp_;
  uint8_t* ptr_ = nullptr;
};

TEST_F(ListPackTest, InsertPivotNotFound) {
  lp_.Push("first", QList::TAIL);
  lp_.Push("third", QList::TAIL);

  // Try to insert with non-existent pivot
  EXPECT_FALSE(lp_.Insert("notfound", "second", QList::BEFORE));
  EXPECT_EQ(2, lp_.Size());
}

TEST_F(ListPackTest, RemoveIntegerFromHead) {
  lp_.Push("1", QList::TAIL);
  lp_.Push("2", QList::TAIL);
  lp_.Push("1", QList::TAIL);
  lp_.Push("3", QList::TAIL);

  // Remove integer value "1" from head
  unsigned removed = Remove("1", 0, QList::HEAD);
  EXPECT_EQ(2, removed);
  EXPECT_EQ(2, lp_.Size());

  EXPECT_EQ("2", lp_.At(0));
  EXPECT_EQ("3", lp_.At(1));
}

TEST_F(ListPackTest, RemoveFromTailAll) {
  // List: a, b, a, c, a
  lp_.Push("a", QList::TAIL);
  lp_.Push("b", QList::TAIL);
  lp_.Push("a", QList::TAIL);
  lp_.Push("c", QList::TAIL);
  lp_.Push("a", QList::TAIL);

  // Remove all "a" from tail direction
  unsigned removed = Remove("a", 0, QList::TAIL);
  EXPECT_EQ(3, removed);
  EXPECT_EQ(2, lp_.Size());

  // Remaining elements: b, c
  EXPECT_EQ("b", lp_.At(0));
  EXPECT_EQ("c", lp_.At(1));
}

TEST_F(ListPackTest, RemoveFromTailWithCount) {
  // List: a, b, a, c, a
  lp_.Push("a", QList::TAIL);
  lp_.Push("b", QList::TAIL);
  lp_.Push("a", QList::TAIL);
  lp_.Push("c", QList::TAIL);
  lp_.Push("a", QList::TAIL);

  // Remove only 2 occurrences of "a" from tail (removes indices 4 and 2)
  unsigned removed = Remove("a", 2, QList::TAIL);
  EXPECT_EQ(2, removed);
  EXPECT_EQ(3, lp_.Size());

  // Remaining elements: a, b, c
  EXPECT_EQ("a", lp_.At(0));
  EXPECT_EQ("b", lp_.At(1));
  EXPECT_EQ("c", lp_.At(2));
}

// Test removing consecutive tail elements - verifies lpLast is called correctly
// after deleting the tail element to continue finding remaining matches.
TEST_F(ListPackTest, RemoveFromTailConsecutive) {
  // List: x, target, target, target - three consecutive at tail
  lp_.Push("x", QList::TAIL);
  lp_.Push("target", QList::TAIL);
  lp_.Push("target", QList::TAIL);
  lp_.Push("target", QList::TAIL);

  unsigned removed = Remove("target", 0, QList::TAIL);
  EXPECT_EQ(3, removed);
  EXPECT_EQ(1, lp_.Size());
  EXPECT_EQ("x", lp_.At(0));
}

// Test removing the head element while iterating from TAIL direction.
// After checking all elements from tail to head and deleting the head,
// lpDelete returns pointer to element after head, and lpPrev on that returns nullptr,
// correctly ending iteration.
TEST_F(ListPackTest, RemoveFromTailDeletesHead) {
  // List: a, b, c - removing "a" (at head) while iterating from tail
  lp_.Push("a", QList::TAIL);
  lp_.Push("b", QList::TAIL);
  lp_.Push("c", QList::TAIL);

  unsigned removed = Remove("a", 0, QList::TAIL);
  EXPECT_EQ(1, removed);
  EXPECT_EQ(2, lp_.Size());

  EXPECT_EQ("b", lp_.At(0));
  EXPECT_EQ("c", lp_.At(1));
}

TEST_F(ListPackTest, ReplaceAtIndex) {
  lp_.Push("first", QList::TAIL);
  lp_.Push("second", QList::TAIL);
  lp_.Push("third", QList::TAIL);

  // Replace element at index 1
  EXPECT_TRUE(lp_.Replace(1, "replaced"));
  EXPECT_EQ(3, lp_.Size());

  EXPECT_EQ("first", lp_.At(0));
  EXPECT_EQ("replaced", lp_.At(1));
  EXPECT_EQ("third", lp_.At(2));
}

TEST_F(ListPackTest, ReplaceAtNegativeIndex) {
  lp_.Push("first", QList::TAIL);
  lp_.Push("second", QList::TAIL);
  lp_.Push("third", QList::TAIL);

  // Replace element at index -1 (last element)
  EXPECT_TRUE(lp_.Replace(-1, "new_last"));
  EXPECT_EQ(3, lp_.Size());

  EXPECT_EQ("first", lp_.At(0));
  EXPECT_EQ("second", lp_.At(1));
  EXPECT_EQ("new_last", lp_.At(2));
}

TEST_F(ListPackTest, ReplaceOutOfBounds) {
  lp_.Push("first", QList::TAIL);
  lp_.Push("second", QList::TAIL);

  // Replace at out-of-bounds index should return false
  EXPECT_FALSE(lp_.Replace(5, "nope"));
  EXPECT_FALSE(lp_.Replace(-5, "nope"));
  EXPECT_EQ(2, lp_.Size());

  // Original elements unchanged
  EXPECT_EQ("first", lp_.At(0));
  EXPECT_EQ("second", lp_.At(1));
}

TEST_F(ListPackTest, ReplaceWithLargerString) {
  lp_.Push("a", QList::TAIL);
  lp_.Push("b", QList::TAIL);

  // Replace with a much larger string
  string large(500, 'x');
  EXPECT_TRUE(lp_.Replace(0, large));
  EXPECT_EQ(2, lp_.Size());

  EXPECT_EQ(large, lp_.At(0));
  EXPECT_EQ("b", lp_.At(1));
}

TEST_F(ListPackTest, ReplaceWithEmptyString) {
  lp_.Push("first", QList::TAIL);
  lp_.Push("second", QList::TAIL);

  // Replace with empty string
  EXPECT_TRUE(lp_.Replace(0, ""));
  EXPECT_EQ(2, lp_.Size());

  EXPECT_EQ("", lp_.At(0));
  EXPECT_EQ("second", lp_.At(1));
}

}  // namespace detail
}  // namespace dfly
