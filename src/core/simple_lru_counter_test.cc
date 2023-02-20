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
  SimpleLruTest() : cache_(4) {
  }

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

}  // namespace dfly
