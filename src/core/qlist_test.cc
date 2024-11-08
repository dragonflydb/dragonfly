// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/qlist.h"

#include <gmock/gmock.h>

#include "base/gtest.h"
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

  MiMemoryResource mr_;
  QList ql_;
};

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

  vector<string> items;
  ql_.Iterate(
      [&](const QList::Entry& e) {
        items.push_back(string(e.view()));
        return true;
      },
      0, 2);

  EXPECT_THAT(items, ElementsAre("abc", "def"));
}

TEST_F(QListTest, ListPack) {
  string_view sv = "abcded"sv;
  uint8_t* lp1 = lpPrepend(lpNew(0), (uint8_t*)sv.data(), sv.size());
  uint8_t* lp2 = lpAppend(lpNew(0), (uint8_t*)sv.data(), sv.size());
  ASSERT_EQ(lpBytes(lp1), lpBytes(lp2));
  ASSERT_EQ(0, memcmp(lp1, lp2, lpBytes(lp1)));
}

};  // namespace dfly
