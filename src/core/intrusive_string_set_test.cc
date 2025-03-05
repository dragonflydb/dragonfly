// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/intrusive_string_set.h"

#include "base/gtest.h"

namespace dfly {

using namespace std;

class IntrusiveStringSetTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
  }

  static void TearDownTestSuite() {
  }

  void SetUp() override {
  }

  void TearDown() override {
  }
};

TEST_F(IntrusiveStringSetTest, ISSEntryTest) {
  ISSEntry test("0123456789");

  EXPECT_EQ(test.Key(), "0123456789"sv);
  EXPECT_EQ(test.Next(), nullptr);

  test.SetNext(&test);

  EXPECT_EQ(test.Key(), "0123456789"sv);
  EXPECT_EQ(test.Next(), &test);
}

TEST_F(IntrusiveStringSetTest, ISMEntryTest) {
  ISMEntry test("0123456789", "qwertyuiopasdfghjklzxcvbnm");

  EXPECT_EQ(test.Key(), "0123456789"sv);
  EXPECT_EQ(test.Val(), "qwertyuiopasdfghjklzxcvbnm"sv);
  EXPECT_EQ(test.Next(), nullptr);

  test.SetVal("QWERTYUIOPASDFGHJKLZXCVBNM");
  test.SetNext(&test);

  EXPECT_EQ(test.Key(), "0123456789"sv);
  EXPECT_EQ(test.Val(), "QWERTYUIOPASDFGHJKLZXCVBNM"sv);
  EXPECT_EQ(test.Next(), &test);
}

}  // namespace dfly
