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

TEST_F(SimpleLruTest, Basic) {
  cache_.Put("a", 1);
  cache_.Put("b", 2);
  cache_.Put("c", 3);
  cache_.Put("d", 4);
  cache_.Put("a", 1);

  ASSERT_EQ(1, cache_.Get("a"));
  ASSERT_EQ(2, cache_.Get("b"));
  ASSERT_EQ(3, cache_.Get("c"));
  ASSERT_EQ(4, cache_.Get("d"));

  ASSERT_EQ(nullopt, cache_.Get("e"));
  cache_.Put("e", 5);

  ASSERT_EQ(nullopt, cache_.Get("b"));
  ASSERT_EQ(3, cache_.Get("c"));
  ASSERT_EQ(4, cache_.Get("d"));
  ASSERT_EQ(5, cache_.Get("e"));

  cache_.Put("f", 6);
  ASSERT_EQ(nullopt, cache_.Get("c"));
  ASSERT_EQ(5, cache_.Get("e"));
  ASSERT_EQ(6, cache_.Get("f"));
}

TEST_F(SimpleLruTest, DifferentOrder) {
  for (uint32_t i = 0; i < kSize * 2; ++i) {
    cache_.Put(absl::StrCat(i), i);
  }

  for (uint32_t i = 0; i < kSize; ++i) {
    EXPECT_EQ(nullopt, cache_.Get(absl::StrCat(i)));
  }
  for (uint32_t i = kSize; i < kSize * 2; ++i) {
    EXPECT_EQ(i, cache_.Get(absl::StrCat(i)));
  }

  for (uint32_t i = kSize; i > 0; --i) {
    cache_.Put(absl::StrCat(i), i);
  }
  cache_.Put("0", 0);

  for (uint32_t i = 0; i < kSize; ++i) {
    EXPECT_EQ(i, cache_.Get(absl::StrCat(i)));
  }
  for (uint32_t i = kSize; i < kSize * 2; ++i) {
    EXPECT_EQ(nullopt, cache_.Get(absl::StrCat(i)));
  }
}

}  // namespace dfly
