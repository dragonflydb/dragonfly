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
  add(19);
  EXPECT_EQ(current(), list_copy);
  add(30);
  add(27);
  EXPECT_EQ(current(), list_copy);

  // Now add some numbers in reverse order
  add(41);
  add(40);
  add(37);
  add(34);
  EXPECT_EQ(current(), list_copy);

  // Now add a 0
  add(0);
  EXPECT_EQ(current(), list_copy);

  // Make sure all test integers fit into a single byte
  EXPECT_EQ(list.ByteSize(), list.Size());
}

TEST_F(CompressedListTest, BasicInsertLargeValues) {
  CompressedList list;
  IdVec list_copy;

  const uint32_t kBase = 1'000'000'000;

  // Add big integers in reverse order
  uint32_t base = kBase;
  while (base > 0) {
    list.Insert(base);
    list_copy.insert(list_copy.begin(), base);
    base /= 10;
  }

  EXPECT_EQ(IdVec(list.begin(), list.end()), list_copy);

  // Now add neighboring  integers with an offset of one
  base = kBase;
  while (base > 0) {
    list.Insert(base + 1);
    list_copy.push_back(base + 1);
    base /= 10;
  }
  sort(list_copy.begin(), list_copy.end());

  EXPECT_EQ(IdVec(list.begin(), list.end()), list_copy);

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

TEST_F(CompressedListTest, BasicRemove) {
  CompressedList list;

  IdVec values = {1, 3, 4, 7, 8, 11, 15, 17, 20, 22, 27};
  copy(values.begin(), values.end(), CompressedList::SortedBackInserter(&list));
  EXPECT_EQ(IdVec(list.begin(), list.end()), values);

  auto remove = [&list, &values](uint32_t value) {
    values.erase(find(values.begin(), values.end(), value));
    list.Remove(value);
  };

  // Remove back and front
  remove(27);
  EXPECT_EQ(IdVec(list.begin(), list.end()), values);
  remove(1);
  EXPECT_EQ(IdVec(list.begin(), list.end()), values);

  // Remove from middle
  remove(11);
  remove(4);
  EXPECT_EQ(IdVec(list.begin(), list.end()), values);
  remove(17);
  remove(8);
  EXPECT_EQ(IdVec(list.begin(), list.end()), values);

  // Remove non existing
  list.Remove(16);
  EXPECT_EQ(IdVec(list.begin(), list.end()), values);
}

TEST_F(CompressedListTest, BasicRemoveLargeValues) {
  CompressedList list;

  IdVec values = {1, 12, 123, 123'4, 123'45, 123'456, 1'234'567, 12'345'678};
  copy(values.begin(), values.end(), CompressedList::SortedBackInserter(&list));
  EXPECT_EQ(IdVec(list.begin(), list.end()), values);

  auto remove = [&list, &values](uint32_t value) {
    values.erase(find(values.begin(), values.end(), value));
    list.Remove(value);
  };

  // Remove from middle
  remove(123'45);
  EXPECT_EQ(IdVec(list.begin(), list.end()), values);
  remove(12);
  EXPECT_EQ(IdVec(list.begin(), list.end()), values);
  remove(1'234'567);
  EXPECT_EQ(IdVec(list.begin(), list.end()), values);

  // Remove front
  remove(1);
  EXPECT_EQ(IdVec(list.begin(), list.end()), values);

  // Remove back
  remove(12'345'678);
  EXPECT_EQ(IdVec(list.begin(), list.end()), values);
}

}  // namespace dfly::search
