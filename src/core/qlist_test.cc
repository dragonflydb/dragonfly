// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/qlist.h"

#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>

#include "base/gtest.h"
#include "base/logging.h"
#include "core/mi_memory_resource.h"

extern "C" {
#include "redis/listpack.h"
#include "redis/zmalloc.h"
}

namespace dfly {

using namespace std;
using namespace testing;

class QListTest : public ::testing::Test {
 protected:
  QListTest() : mr_(mi_heap_get_backing()) {
  }

  static void SetUpTestSuite() {
    // configure redis lib zmalloc which requires mimalloc heap to work.
    auto* tlh = mi_heap_get_backing();
    init_zmalloc_threadlocal(tlh);
  }

  static void TearDownTestSuite() {
    mi_heap_collect(mi_heap_get_backing(), true);

    auto cb_visit = [](const mi_heap_t* heap, const mi_heap_area_t* area, void* block,
                       size_t block_size, void* arg) {
      LOG(ERROR) << "Unfreed allocations: block_size " << block_size
                 << ", allocated: " << area->used * block_size;
      return true;
    };

    mi_heap_visit_blocks(mi_heap_get_backing(), false /* do not visit all blocks*/, cb_visit,
                         nullptr);
  }

  vector<string> ToItems() const;

  MiMemoryResource mr_;
  QList ql_;
};

vector<string> QListTest::ToItems() const {
  vector<string> res;
  auto cb = [&](const QList::Entry& e) {
    res.push_back(e.to_string());
    return true;
  };

  ql_.Iterate(cb, 0, ql_.Size());
  return res;
}

TEST_F(QListTest, Basic) {
  EXPECT_EQ(0, ql_.Size());
  ql_.Push("abc", QList::HEAD);
  EXPECT_EQ(1, ql_.Size());

  auto it = ql_.GetIterator(QList::HEAD);
  ASSERT_TRUE(it.Next());  // Needed to initialize the iterator.

  EXPECT_EQ("abc", it.Get().view());

  ASSERT_FALSE(it.Next());

  ql_.Push("def", QList::TAIL);
  EXPECT_EQ(2, ql_.Size());

  it = ql_.GetIterator(QList::TAIL);
  ASSERT_TRUE(it.Next());
  EXPECT_EQ("def", it.Get().view());

  ASSERT_TRUE(it.Next());
  EXPECT_EQ("abc", it.Get().view());
  ASSERT_FALSE(it.Next());

  it = ql_.GetIterator(0);
  ASSERT_TRUE(it.Next());
  EXPECT_EQ("abc", it.Get().view());
  it = ql_.GetIterator(-1);
  ASSERT_TRUE(it.Next());
  EXPECT_EQ("def", it.Get().view());

  vector<string> items = ToItems();

  EXPECT_THAT(items, ElementsAre("abc", "def"));
}

TEST_F(QListTest, ListPack) {
  string_view sv = "abcded"sv;
  uint8_t* lp1 = lpPrepend(lpNew(0), (uint8_t*)sv.data(), sv.size());
  uint8_t* lp2 = lpAppend(lpNew(0), (uint8_t*)sv.data(), sv.size());
  ASSERT_EQ(lpBytes(lp1), lpBytes(lp2));
  ASSERT_EQ(0, memcmp(lp1, lp2, lpBytes(lp1)));
  lpFree(lp1);
  lpFree(lp2);
}

TEST_F(QListTest, InsertDelete) {
  EXPECT_FALSE(ql_.Insert("abc", "def", QList::BEFORE));
  ql_.Push("abc", QList::HEAD);
  EXPECT_TRUE(ql_.Insert("abc", "def", QList::BEFORE));
  auto items = ToItems();
  EXPECT_THAT(items, ElementsAre("def", "abc"));
  EXPECT_TRUE(ql_.Insert("abc", "123456", QList::AFTER));
  items = ToItems();
  EXPECT_THAT(items, ElementsAre("def", "abc", "123456"));

  auto it = ql_.GetIterator(QList::HEAD);
  ASSERT_TRUE(it.Next());

  // Erase the items one by one.
  it = ql_.Erase(it);
  items = ToItems();
  EXPECT_THAT(items, ElementsAre("abc", "123456"));
  ASSERT_TRUE(it.Next());
  ASSERT_EQ("abc", it.Get().view());

  it = ql_.Erase(it);
  items = ToItems();
  EXPECT_THAT(items, ElementsAre("123456"));
  ASSERT_TRUE(it.Next());
  ASSERT_EQ(123456, it.Get().ival());

  it = ql_.Erase(it);
  items = ToItems();
  EXPECT_THAT(items, ElementsAre());
  ASSERT_FALSE(it.Next());
  EXPECT_EQ(0, ql_.Size());
}

using FillCompress = tuple<int, unsigned>;

class PrintToFillCompress {
 public:
  std::string operator()(const TestParamInfo<FillCompress>& info) const {
    int fill = get<0>(info.param);
    int compress = get<1>(info.param);
    string fill_str = fill >= 0 ? absl::StrCat("f", fill) : absl::StrCat("fminus", -fill);
    return absl::StrCat(fill_str, "compress", compress);
  }
};

class OptionsTest : public QListTest, public WithParamInterface<FillCompress> {};

INSTANTIATE_TEST_SUITE_P(Matrix, OptionsTest,
                         Combine(Values(-5, -4, -3, -2, -1, 0, 1, 2, 32, 66, 128, 999),
                                 Values(0, 1, 2, 3, 4, 5, 6, 10)),
                         PrintToFillCompress());

TEST_P(OptionsTest, Numbers) {
  auto [fill, compress] = GetParam();
  ql_ = QList(fill, compress);
  array<int64_t, 5000> nums;

  for (unsigned i = 0; i < nums.size(); i++) {
    nums[i] = -5157318210846258176 + i;
    string val = absl::StrCat(nums[i]);
    ql_.Push(val, QList::TAIL);
  }
  ql_.Push("xxxxxxxxxxxxxxxxxxxx", QList::TAIL);

  for (unsigned i = 0; i < nums.size(); i++) {
    auto it = ql_.GetIterator(i);
    ASSERT_TRUE(it.Next());
    ASSERT_EQ(nums[i], it.Get().ival()) << i;
  }

  auto it = ql_.GetIterator(nums.size());
  ASSERT_TRUE(it.Next());
  EXPECT_EQ("xxxxxxxxxxxxxxxxxxxx", it.Get().view());
}

};  // namespace dfly
