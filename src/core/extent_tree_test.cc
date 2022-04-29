// Copyright 2022, Roman Gershman.  All rights reserved.
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
  EXPECT_THAT(*op, testing::Pair(0, 64));

  tree_.Add(56, 8);
  op = tree_.GetRange(64, 16);
  EXPECT_TRUE(op);
  EXPECT_THAT(*op, testing::Pair(64, 128));

  op = tree_.GetRange(18, 2);
  EXPECT_TRUE(op);
  EXPECT_THAT(*op, testing::Pair(128, 146));

  op = tree_.GetRange(80, 16);
  EXPECT_TRUE(op);
  EXPECT_THAT(*op, testing::Pair(160, 240));

  op = tree_.GetRange(4, 1);
  EXPECT_TRUE(op);
  EXPECT_THAT(*op, testing::Pair(56, 60));

  op = tree_.GetRange(32, 1);
  EXPECT_FALSE(op);
  tree_.Add(64, 240 - 64);
  op = tree_.GetRange(32, 4);
  EXPECT_TRUE(op);
  EXPECT_THAT(*op, testing::Pair(60, 92));
}

}  // namespace dfly