// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/simple_lru_counter.h"

#include "base/gtest.h"
#include "base/logging.h"

using namespace std;

namespace dfly {

class SimpleLruTest : public ::testing::Test {
 protected:
  SimpleLruTest() : cache_(kSize) {
  }

  const size_t kSize = 4;
  SimpleLruCounter cache_;
};

TEST_F(SimpleLruTest, PutAndGet) {
  cache_.Put("a", 1);
  cache_.Put("a", 1);
  cache_.Put("b", 2);
  cache_.Put("c", 3);
  cache_.Put("d", 4);
  ASSERT_EQ(1, cache_.GetLast());
  cache_.Put("a", 1);
  ASSERT_EQ(2, cache_.GetLast());

  ASSERT_EQ(1, cache_.Get("a"));
  ASSERT_EQ(2, cache_.Get("b"));
  ASSERT_EQ(3, cache_.Get("c"));
  ASSERT_EQ(4, cache_.Get("d"));

  ASSERT_EQ(nullopt, cache_.Get("e"));
  cache_.Put("e", 5);

  ASSERT_EQ(2, cache_.Get("b"));
  ASSERT_EQ(3, cache_.Get("c"));
  ASSERT_EQ(4, cache_.Get("d"));
  ASSERT_EQ(5, cache_.Get("e"));

  cache_.Put("f", 6);
  ASSERT_EQ(2, cache_.GetLast());
  ASSERT_EQ(3, cache_.Get("c"));
  ASSERT_EQ(5, cache_.Get("e"));
  ASSERT_EQ(6, cache_.Get("f"));
}

TEST_F(SimpleLruTest, PutAndPutTail) {
  cache_.Put("a", 1);
  cache_.Put("a", 1);  // a
  cache_.Put("b", 2);  // b -> a
  cache_.Put("c", 3);  // c -> b -> a
  cache_.Put("d", 4);  // d-> c -> b -> a
  ASSERT_EQ(1, cache_.GetLast());
  cache_.Put("a", 1);  // a -> d -> c -> b
  ASSERT_EQ(2, cache_.GetLast());
  ASSERT_EQ(3, cache_.GetPrev("b"));
  ASSERT_EQ(4, cache_.GetPrev("c"));
  ASSERT_EQ(2, cache_.GetPrev("a"));
  cache_.Put("d", 4, SimpleLruCounter::Position::kTail);  // a -> c -> b -> d
  ASSERT_EQ(4, cache_.GetLast());
  ASSERT_EQ(2, cache_.GetPrev("d"));
  ASSERT_EQ(3, cache_.GetPrev("b"));
  ASSERT_EQ(1, cache_.GetPrev("c"));
  ASSERT_EQ(4, cache_.GetPrev("a"));
  cache_.Put("a", 1);  // a -> c -> b -> d
  ASSERT_EQ(4, cache_.GetLast());
  cache_.Put("e", 5, SimpleLruCounter::Position::kTail);  // a -> c -> b -> d -> e
  ASSERT_EQ(5, cache_.GetLast());
  ASSERT_EQ(4, cache_.GetPrev("e"));
  ASSERT_EQ(2, cache_.GetPrev("d"));
  ASSERT_EQ(1, cache_.GetPrev("c"));
  ASSERT_EQ(5, cache_.GetPrev("a"));
  cache_.Put("e", 6, SimpleLruCounter::Position::kTail);  // a -> c -> b -> d -> e
  ASSERT_EQ(6, cache_.GetLast());
  ASSERT_EQ(4, cache_.GetPrev("e"));
  ASSERT_EQ(2, cache_.GetPrev("d"));
  ASSERT_EQ(1, cache_.GetPrev("c"));
  ASSERT_EQ(6, cache_.GetPrev("a"));
}

TEST_F(SimpleLruTest, BumpTest) {
  cache_.Put("a", 1);
  cache_.Put("b", 2);
  cache_.Put("c", 3);
  cache_.Put("d", 4);
  ASSERT_EQ(1, cache_.GetLast());
  cache_.Put("c", 3);
  ASSERT_EQ(1, cache_.GetLast());
  ASSERT_EQ(4, cache_.GetPrev("b"));
  ASSERT_EQ(3, cache_.GetPrev("d"));
}

TEST_F(SimpleLruTest, DifferentOrder) {
  for (uint32_t i = 0; i < kSize * 2; ++i) {
    cache_.Put(absl::StrCat(i), i);
  }
  ASSERT_EQ(0, cache_.GetLast());

  for (uint32_t i = 0; i < kSize * 2; ++i) {
    EXPECT_EQ(i, cache_.Get(absl::StrCat(i)));
  }

  for (uint32_t i = kSize; i > 0; --i) {
    cache_.Put(absl::StrCat(i), i);
  }
  ASSERT_EQ(0, cache_.GetLast());
  cache_.Put("0", 0);
  ASSERT_EQ(kSize + 1, cache_.GetLast());

  for (uint32_t i = 0; i < kSize * 2; ++i) {
    EXPECT_EQ(i, cache_.Get(absl::StrCat(i)));
  }
  ASSERT_EQ(kSize + 1, cache_.GetLast());
}

TEST_F(SimpleLruTest, Delete) {
  cache_.Put("a", 1);  // a
  cache_.Put("b", 2);  // b -> a
  cache_.Put("c", 3);  // c -> b -> a
  cache_.Put("d", 4);  // d-> c -> b -> a
  cache_.Put("e", 5);  // e -> d-> c -> b -> a
  cache_.Remove("e");  // d-> c -> b -> a
  ASSERT_EQ(nullopt, cache_.Get("e"));
  ASSERT_EQ(1, cache_.GetLast());
  ASSERT_EQ(2, cache_.GetPrev("a"));
  ASSERT_EQ(3, cache_.GetPrev("b"));
  ASSERT_EQ(4, cache_.GetPrev("c"));
  ASSERT_EQ(1, cache_.GetPrev("d"));
  cache_.Remove("e");  // d-> c -> b -> a
  ASSERT_EQ(nullopt, cache_.Get("e"));
  cache_.Remove("c");  // d -> b -> a
  ASSERT_EQ(1, cache_.GetLast());
  ASSERT_EQ(2, cache_.GetPrev("a"));
  ASSERT_EQ(4, cache_.GetPrev("b"));
  ASSERT_EQ(1, cache_.GetPrev("d"));
  cache_.Put("c", 3);  // c -> d -> b -> a
  ASSERT_EQ(1, cache_.GetLast());
  ASSERT_EQ(2, cache_.GetPrev("a"));
  ASSERT_EQ(4, cache_.GetPrev("b"));
  ASSERT_EQ(3, cache_.GetPrev("d"));
  ASSERT_EQ(1, cache_.GetPrev("c"));
  cache_.Remove("a");  // c -> d -> b
  ASSERT_EQ(2, cache_.GetLast());
  ASSERT_EQ(4, cache_.GetPrev("b"));
  ASSERT_EQ(3, cache_.GetPrev("d"));
  ASSERT_EQ(2, cache_.GetPrev("c"));
}

}  // namespace dfly
