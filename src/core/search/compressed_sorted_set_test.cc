// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/compressed_sorted_set.h"

#include <absl/container/btree_set.h>
#include <gmock/gmock.h>

#include <algorithm>

#include "base/gtest.h"
#include "base/logging.h"
#include "core/bptree_set.h"

namespace dfly::search {

using namespace std;

namespace {

struct SetInserter {
  using iterator_category = std::forward_iterator_tag;
  using difference_type = std::ptrdiff_t;
  using value_type = CompressedSortedSet::IntType;
  using pointer = value_type*;
  using reference = value_type&;

  explicit SetInserter(CompressedSortedSet* set) : set_{set} {};

  SetInserter& operator*() {
    return *this;
  }
  SetInserter& operator++() {
    return *this;
  }

  SetInserter& operator=(value_type value) {
    set_->Insert(value);
    return *this;
  }

 private:
  CompressedSortedSet* set_;
};

}  // namespace

class CompressedSortedSetTest : public ::testing::Test {
 protected:
};

using IdVec = vector<uint32_t>;

TEST_F(CompressedSortedSetTest, BasicInsert) {
  CompressedSortedSet list{PMR_NS::get_default_resource()};
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

  // Without store_freq, each entry is a single varint (1 byte for small diff values)
  EXPECT_EQ(list.ByteSize(), list.Size());
}

TEST_F(CompressedSortedSetTest, BasicInsertLargeValues) {
  CompressedSortedSet list{PMR_NS::get_default_resource()};
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

  // Without store_freq, verify compressed representation is still compact
  EXPECT_LE(list.ByteSize() * 2, list.Size() * sizeof(uint32_t));
}

TEST_F(CompressedSortedSetTest, SortedBackInserter) {
  CompressedSortedSet list{PMR_NS::get_default_resource()};

  vector<uint32_t> v1 = {1, 3, 5};
  vector<uint32_t> v2 = {2, 4, 6};

  merge(v1.begin(), v1.end(), v2.begin(), v2.end(), SetInserter{&list});

  EXPECT_EQ(IdVec(list.begin(), list.end()), IdVec({1, 2, 3, 4, 5, 6}));
}

TEST_F(CompressedSortedSetTest, BasicRemove) {
  CompressedSortedSet list{PMR_NS::get_default_resource()};

  IdVec values = {1, 3, 4, 7, 8, 11, 15, 17, 20, 22, 27};
  copy(values.begin(), values.end(), SetInserter{&list});
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

TEST_F(CompressedSortedSetTest, BasicRemoveLargeValues) {
  CompressedSortedSet list{PMR_NS::get_default_resource()};

  IdVec values = {1, 12, 123, 123'4, 123'45, 123'456, 1'234'567, 12'345'678};
  copy(values.begin(), values.end(), SetInserter{&list});
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

TEST_F(CompressedSortedSetTest, InsertRemoveLargeValues) {
  CompressedSortedSet list{PMR_NS::get_default_resource()};

  for (int shift = 3; shift < 30; shift++) {
    uint32_t value = 1u << shift;

    IdVec values{value + 3, value, value - 5};
    for (auto v : values)
      list.Insert(v);

    sort(values.begin(), values.end());
    EXPECT_EQ(IdVec(list.begin(), list.end()), values);

    for (auto v : values)
      list.Remove(v);

    EXPECT_EQ(IdVec(list.begin(), list.end()), IdVec({}));
  }
}

TEST_F(CompressedSortedSetTest, FreqBasic) {
  CompressedSortedSet list{PMR_NS::get_default_resource(), /*store_freq=*/true};

  // Insert with explicit frequencies
  list.Insert(10, 3);
  list.Insert(20, 5);
  list.Insert(30, 1);

  auto it = list.begin();
  EXPECT_EQ(*it, 10u);
  EXPECT_EQ(it.Freq(), 3u);
  ++it;
  EXPECT_EQ(*it, 20u);
  EXPECT_EQ(it.Freq(), 5u);
  ++it;
  EXPECT_EQ(*it, 30u);
  EXPECT_EQ(it.Freq(), 1u);
  ++it;
  EXPECT_EQ(it, list.end());
}

TEST_F(CompressedSortedSetTest, FreqDefaultIsOne) {
  CompressedSortedSet list{PMR_NS::get_default_resource(), /*store_freq=*/true};

  // Insert without explicit freq (default = 1)
  list.Insert(5);
  list.Insert(15);

  auto it = list.begin();
  EXPECT_EQ(*it, 5u);
  EXPECT_EQ(it.Freq(), 1u);
  ++it;
  EXPECT_EQ(*it, 15u);
  EXPECT_EQ(it.Freq(), 1u);
}

TEST_F(CompressedSortedSetTest, FreqInsertMiddle) {
  CompressedSortedSet list{PMR_NS::get_default_resource(), /*store_freq=*/true};

  // Insert out of order to test mid-list insertion preserves freq
  list.Insert(10, 2);
  list.Insert(30, 4);
  list.Insert(20, 3);  // Inserted between 10 and 30

  vector<pair<uint32_t, uint32_t>> result;
  for (auto it = list.begin(); it != list.end(); ++it)
    result.emplace_back(*it, it.Freq());

  EXPECT_EQ(result, (vector<pair<uint32_t, uint32_t>>{{10, 2}, {20, 3}, {30, 4}}));
}

TEST_F(CompressedSortedSetTest, FreqRemove) {
  CompressedSortedSet list{PMR_NS::get_default_resource(), /*store_freq=*/true};

  list.Insert(10, 2);
  list.Insert(20, 3);
  list.Insert(30, 4);

  // Remove middle — freq of neighbors should be preserved
  list.Remove(20);

  vector<pair<uint32_t, uint32_t>> result;
  for (auto it = list.begin(); it != list.end(); ++it)
    result.emplace_back(*it, it.Freq());

  EXPECT_EQ(result, (vector<pair<uint32_t, uint32_t>>{{10, 2}, {30, 4}}));

  // Remove front
  list.Remove(10);
  result.clear();
  for (auto it = list.begin(); it != list.end(); ++it)
    result.emplace_back(*it, it.Freq());

  EXPECT_EQ(result, (vector<pair<uint32_t, uint32_t>>{{30, 4}}));
}

TEST_F(CompressedSortedSetTest, FreqMerge) {
  CompressedSortedSet list1{PMR_NS::get_default_resource(), /*store_freq=*/true};
  CompressedSortedSet list2{PMR_NS::get_default_resource(), /*store_freq=*/true};

  list1.Insert(10, 2);
  list1.Insert(30, 4);

  list2.Insert(20, 3);
  list2.Insert(40, 5);

  list1.Merge(std::move(list2));

  vector<pair<uint32_t, uint32_t>> result;
  for (auto it = list1.begin(); it != list1.end(); ++it)
    result.emplace_back(*it, it.Freq());

  EXPECT_EQ(result, (vector<pair<uint32_t, uint32_t>>{{10, 2}, {20, 3}, {30, 4}, {40, 5}}));
}

TEST_F(CompressedSortedSetTest, FreqSplit) {
  CompressedSortedSet list{PMR_NS::get_default_resource(), /*store_freq=*/true};

  for (uint32_t i = 0; i < 20; i++)
    list.Insert(i * 10, i + 1);

  auto [first, second] = std::move(list).Split();

  // Verify freqs are preserved in both halves
  uint32_t expected_id = 0;
  for (auto it = first.begin(); it != first.end(); ++it) {
    EXPECT_EQ(*it, expected_id * 10);
    EXPECT_EQ(it.Freq(), expected_id + 1);
    expected_id++;
  }
  for (auto it = second.begin(); it != second.end(); ++it) {
    EXPECT_EQ(*it, expected_id * 10);
    EXPECT_EQ(it.Freq(), expected_id + 1);
    expected_id++;
  }
  EXPECT_EQ(expected_id, 20u);
}

TEST_F(CompressedSortedSetTest, FreqLargeValues) {
  CompressedSortedSet list{PMR_NS::get_default_resource(), /*store_freq=*/true};

  // Test large frequency values that stress varint encoding
  vector<pair<uint32_t, uint32_t>> entries = {
      {10, 1},       // minimal
      {20, 127},     // max 1-byte varint
      {30, 128},     // min 2-byte varint
      {40, 16383},   // max 2-byte varint
      {50, 16384},   // min 3-byte varint
      {60, 100000},  // large value
  };

  for (auto [id, freq] : entries)
    list.Insert(id, freq);

  EXPECT_EQ(list.Size(), entries.size());

  auto it = list.begin();
  for (size_t i = 0; i < entries.size(); i++, ++it) {
    EXPECT_EQ(*it, entries[i].first) << "DocId mismatch at index " << i;
    EXPECT_EQ(it.Freq(), entries[i].second) << "Freq mismatch at index " << i;
  }
}

TEST_F(CompressedSortedSetTest, FreqZeroHandling) {
  CompressedSortedSet list{PMR_NS::get_default_resource(), /*store_freq=*/true};

  // freq=0 is used for synonym entries
  list.Insert(10, 0);
  list.Insert(20, 3);
  list.Insert(30, 0);

  auto it = list.begin();
  EXPECT_EQ(*it, 10u);
  EXPECT_EQ(it.Freq(), 0u);
  ++it;
  EXPECT_EQ(*it, 20u);
  EXPECT_EQ(it.Freq(), 3u);
  ++it;
  EXPECT_EQ(*it, 30u);
  EXPECT_EQ(it.Freq(), 0u);
}

TEST_F(CompressedSortedSetTest, PositionsBasic) {
  CompressedSortedSet list{PMR_NS::get_default_resource(), /*store_freq=*/true,
                           /*store_positions=*/true};
  EXPECT_TRUE(list.StoresFreq());
  EXPECT_TRUE(list.StoresPositions());

  std::vector<uint32_t> p1{0, 3, 17};
  std::vector<uint32_t> p2{5};
  std::vector<uint32_t> p3{2, 4, 6, 8};
  list.Insert(10, p1.size(), p1);
  list.Insert(20, p2.size(), p2);
  list.Insert(30, p3.size(), p3);

  auto it = list.begin();
  EXPECT_EQ(*it, 10u);
  EXPECT_EQ(it.Freq(), 3u);
  EXPECT_THAT(std::vector<uint32_t>(it.Positions().begin(), it.Positions().end()),
              ::testing::ElementsAreArray(p1));
  ++it;
  EXPECT_EQ(*it, 20u);
  EXPECT_THAT(std::vector<uint32_t>(it.Positions().begin(), it.Positions().end()),
              ::testing::ElementsAreArray(p2));
  ++it;
  EXPECT_EQ(*it, 30u);
  EXPECT_THAT(std::vector<uint32_t>(it.Positions().begin(), it.Positions().end()),
              ::testing::ElementsAreArray(p3));
}

TEST_F(CompressedSortedSetTest, PositionsInsertMiddle) {
  CompressedSortedSet list{PMR_NS::get_default_resource(), true, true};

  std::vector<uint32_t> p10{1, 5};
  std::vector<uint32_t> p30{2, 4, 7};
  std::vector<uint32_t> p20{0, 10};
  list.Insert(10, p10.size(), p10);
  list.Insert(30, p30.size(), p30);
  // Triggers the middle-insert path: bound is 30, prev is 10.
  list.Insert(20, p20.size(), p20);

  auto it = list.begin();
  EXPECT_EQ(*it, 10u);
  EXPECT_THAT(std::vector<uint32_t>(it.Positions().begin(), it.Positions().end()),
              ::testing::ElementsAreArray(p10));
  ++it;
  EXPECT_EQ(*it, 20u);
  EXPECT_THAT(std::vector<uint32_t>(it.Positions().begin(), it.Positions().end()),
              ::testing::ElementsAreArray(p20));
  ++it;
  EXPECT_EQ(*it, 30u);
  // Crucial: 30's positions must survive the middle-insert.
  EXPECT_THAT(std::vector<uint32_t>(it.Positions().begin(), it.Positions().end()),
              ::testing::ElementsAreArray(p30));
}

TEST_F(CompressedSortedSetTest, PositionsRemove) {
  CompressedSortedSet list{PMR_NS::get_default_resource(), true, true};

  std::vector<uint32_t> p10{0, 4};
  std::vector<uint32_t> p20{3};
  std::vector<uint32_t> p30{1, 9, 15};
  list.Insert(10, p10.size(), p10);
  list.Insert(20, p20.size(), p20);
  list.Insert(30, p30.size(), p30);

  EXPECT_TRUE(list.Remove(20));
  EXPECT_EQ(list.Size(), 2u);

  auto it = list.begin();
  EXPECT_EQ(*it, 10u);
  EXPECT_THAT(std::vector<uint32_t>(it.Positions().begin(), it.Positions().end()),
              ::testing::ElementsAreArray(p10));
  ++it;
  EXPECT_EQ(*it, 30u);
  // Crucial: removing middle entry must preserve 30's positions verbatim.
  EXPECT_THAT(std::vector<uint32_t>(it.Positions().begin(), it.Positions().end()),
              ::testing::ElementsAreArray(p30));
}

TEST_F(CompressedSortedSetTest, PositionsRemoveTail) {
  CompressedSortedSet list{PMR_NS::get_default_resource(), true, true};
  std::vector<uint32_t> p10{0, 5};
  std::vector<uint32_t> p20{2, 8};
  list.Insert(10, p10.size(), p10);
  list.Insert(20, p20.size(), p20);

  EXPECT_TRUE(list.Remove(20));
  auto it = list.begin();
  EXPECT_EQ(*it, 10u);
  EXPECT_THAT(std::vector<uint32_t>(it.Positions().begin(), it.Positions().end()),
              ::testing::ElementsAreArray(p10));
  ++it;
  EXPECT_EQ(it, list.end());
}

TEST_F(CompressedSortedSetTest, PositionsMerge) {
  CompressedSortedSet a{PMR_NS::get_default_resource(), true, true};
  CompressedSortedSet b{PMR_NS::get_default_resource(), true, true};

  std::vector<uint32_t> p10{1, 3};
  std::vector<uint32_t> p30{0};
  std::vector<uint32_t> p20{5, 7};
  std::vector<uint32_t> p40{2};
  a.Insert(10, p10.size(), p10);
  a.Insert(30, p30.size(), p30);
  b.Insert(20, p20.size(), p20);
  b.Insert(40, p40.size(), p40);

  a.Merge(std::move(b));

  auto it = a.begin();
  EXPECT_EQ(*it, 10u);
  EXPECT_THAT(std::vector<uint32_t>(it.Positions().begin(), it.Positions().end()),
              ::testing::ElementsAreArray(p10));
  ++it;
  EXPECT_EQ(*it, 20u);
  EXPECT_THAT(std::vector<uint32_t>(it.Positions().begin(), it.Positions().end()),
              ::testing::ElementsAreArray(p20));
  ++it;
  EXPECT_EQ(*it, 30u);
  EXPECT_THAT(std::vector<uint32_t>(it.Positions().begin(), it.Positions().end()),
              ::testing::ElementsAreArray(p30));
  ++it;
  EXPECT_EQ(*it, 40u);
  EXPECT_THAT(std::vector<uint32_t>(it.Positions().begin(), it.Positions().end()),
              ::testing::ElementsAreArray(p40));
}

TEST_F(CompressedSortedSetTest, PositionsSplit) {
  CompressedSortedSet list{PMR_NS::get_default_resource(), true, true};
  std::vector<std::vector<uint32_t>> all_pos;
  for (uint32_t i = 0; i < 10; ++i) {
    std::vector<uint32_t> p{i, i + 100, i + 200};
    all_pos.push_back(p);
    list.Insert(i * 10, p.size(), p);
  }

  auto [first, second] = std::move(list).Split();
  EXPECT_GT(first.Size(), 0u);
  EXPECT_GT(second.Size(), 0u);
  EXPECT_EQ(first.Size() + second.Size(), 10u);
  EXPECT_TRUE(first.StoresPositions());
  EXPECT_TRUE(second.StoresPositions());

  // Confirm each docid still has its original positions in whichever half it landed.
  for (const auto& set : {&first, &second}) {
    for (auto it = set->begin(); it != set->end(); ++it) {
      uint32_t docid = *it;
      ASSERT_EQ(docid % 10u, 0u);
      const auto& expected = all_pos[docid / 10];
      EXPECT_THAT(std::vector<uint32_t>(it.Positions().begin(), it.Positions().end()),
                  ::testing::ElementsAreArray(expected))
          << "docid=" << docid;
    }
  }
}

TEST_F(CompressedSortedSetTest, PositionsLargeValues) {
  CompressedSortedSet list{PMR_NS::get_default_resource(), true, true};

  std::vector<uint32_t> p1{0, 127, 128, 16383, 16384, 1u << 28};
  std::vector<uint32_t> p2{500};
  list.Insert(1000, p1.size(), p1);
  list.Insert(2000, p2.size(), p2);

  auto it = list.begin();
  EXPECT_EQ(*it, 1000u);
  EXPECT_THAT(std::vector<uint32_t>(it.Positions().begin(), it.Positions().end()),
              ::testing::ElementsAreArray(p1));
  ++it;
  EXPECT_EQ(*it, 2000u);
  EXPECT_THAT(std::vector<uint32_t>(it.Positions().begin(), it.Positions().end()),
              ::testing::ElementsAreArray(p2));
}

TEST_F(CompressedSortedSetTest, PositionsImpliesFreq) {
  // store_positions=true must imply store_freq=true regardless of the freq flag passed.
  CompressedSortedSet list{PMR_NS::get_default_resource(), /*store_freq=*/false,
                           /*store_positions=*/true};
  EXPECT_TRUE(list.StoresFreq());
  EXPECT_TRUE(list.StoresPositions());
}

}  // namespace dfly::search
