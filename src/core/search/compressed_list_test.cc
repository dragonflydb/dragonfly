// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/compressed_list.h"

#include <algorithm>

#include "base/gtest.h"
#include "base/logging.h"

namespace dfly::search {

using namespace std;

class CompressedListTest : public ::testing::Test {
 protected:
};

using IdVec = vector<uint32_t>;

TEST_F(CompressedListTest, BasicInsert) {
  CompressedList list;
  IdVec list_copy;

  auto current = [&list]() { return IdVec{list.begin(), list.end()}; };
  auto add = [&list, &list_copy](uint32_t value) {
    list.Insert(value);
    set<uint32_t> list_copy_set{list_copy.begin(), list_copy.end()};
    list_copy_set.insert(value);
    list_copy = IdVec{list_copy_set.begin(), list_copy_set.end()};
  };

  // Check empty list is empty
  EXPECT_EQ(current(), list_copy);

  // Insert some numbers in sorted order
  add(10);
  EXPECT_EQ(current(), list_copy);
  add(15);
  EXPECT_EQ(current(), list_copy);
  add(22);
  EXPECT_EQ(current(), list_copy);
  add(25);
  EXPECT_EQ(current(), list_copy);
  add(31);
  EXPECT_EQ(current(), list_copy);

  // Now insert front
  add(7);
  EXPECT_EQ(current(), list_copy);
  add(2);
  EXPECT_EQ(current(), list_copy);

  // Insert in-between
  add(13);
  EXPECT_EQ(current(), list_copy);
  add(23);
  EXPECT_EQ(current(), list_copy);
  add(19);
  EXPECT_EQ(current(), list_copy);
  add(30);
  EXPECT_EQ(current(), list_copy);
  add(27);
  EXPECT_EQ(current(), list_copy);

  // Make sure all small integers fit into a single byte
  EXPECT_EQ(list.ByteSize(), list.Size());
}

TEST_F(CompressedListTest, BasicLarger) {
  CompressedList list;

  uint32_t base = 1'000'000;
  while (base > 0) {
    list.Insert(base);
    base /= 10;
  }

  EXPECT_EQ(IdVec(list.begin(), list.end()),
            IdVec({1, 10, 100, 100'0, 100'00, 100'000, 1'000'000}));

  // Make sure we use at least twice less memory
  EXPECT_LE(list.ByteSize() * 2, list.Size() * sizeof(uint32_t));
}

TEST_F(CompressedListTest, SortedBackInserter) {
  CompressedList list;

  vector<uint32_t> v1 = {1, 3, 5};
  vector<uint32_t> v2 = {2, 4, 6};

  merge(v1.begin(), v1.end(), v2.begin(), v2.end(), CompressedList::SortedBackInserter{&list});

  EXPECT_EQ(IdVec(list.begin(), list.end()), IdVec({1, 2, 3, 4, 5, 6}));
}

}  // namespace dfly::search
