// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/extent_tree.h"

#include <gmock/gmock.h>

#include "base/gtest.h"
#include "base/logging.h"

namespace dfly {

using namespace std;

class ExtentTreeTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
  }

  static void TearDownTestSuite() {
  }

  ExtentTree tree_;
};

TEST_F(ExtentTreeTest, Basic) {
  tree_.Add(0, 256);
  auto op = tree_.GetRange(64, 16);
  EXPECT_TRUE(op);
  EXPECT_THAT(*op, testing::Pair(0, 64));  // [64, 256)

  tree_.Add(56, 8);
  op = tree_.GetRange(64, 16);
  EXPECT_TRUE(op);
  EXPECT_THAT(*op, testing::Pair(64, 128));  // {[56, 64), [128, 256)}

  op = tree_.GetRange(18, 2);
  EXPECT_TRUE(op);
  EXPECT_THAT(*op, testing::Pair(128, 146));  // {[56, 64), [146, 256)}

  op = tree_.GetRange(80, 16);
  EXPECT_TRUE(op);
  EXPECT_THAT(*op, testing::Pair(160, 240));  // {[56, 64), [146, 160), [240, 256)}

  op = tree_.GetRange(4, 1);
  EXPECT_TRUE(op);
  EXPECT_THAT(*op, testing::Pair(56, 60));  // {[60, 64), [146, 160), [240, 256)}

  op = tree_.GetRange(32, 1);
  EXPECT_FALSE(op);
  tree_.Add(64, 146 - 64);
  op = tree_.GetRange(32, 4);
  EXPECT_TRUE(op);
  EXPECT_THAT(*op, testing::Pair(60, 92));
}

TEST_F(ExtentTreeTest, Union) {
  tree_.Add(0, 16);
  tree_.Add(16, 16);
  auto range = tree_.GetRange(32, 1);
  ASSERT_TRUE(range);
  EXPECT_THAT(*range, testing::Pair(0, 32));
}

}  // namespace dfly
