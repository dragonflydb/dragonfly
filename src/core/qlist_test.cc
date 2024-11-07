// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/qlist.h"

#include "base/gtest.h"
#include "core/mi_memory_resource.h"

extern "C" {
#include "redis/zmalloc.h"
}

namespace dfly {
using namespace std;

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

  QList::Entry entry = it.Get();
  EXPECT_EQ("abc", entry.view());

  ASSERT_FALSE(it.Next());

  ql_.Push("def", QList::TAIL);
  EXPECT_EQ(2, ql_.Size());

  it = ql_.GetIterator(QList::TAIL);
  ASSERT_TRUE(it.Next());
  entry = it.Get();
  EXPECT_EQ("def", entry.view());
  ASSERT_TRUE(it.Next());

  entry = it.Get();
  EXPECT_EQ("abc", entry.view());
  ASSERT_FALSE(it.Next());
}

};  // namespace dfly
