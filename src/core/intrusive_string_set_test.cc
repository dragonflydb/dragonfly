// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/intrusive_string_set.h"

#include <set>

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

TEST_F(IntrusiveStringSetTest, IntrusiveStringListTest) {
  IntrusiveStringList isl;
  ISLEntry test = isl.Emplace("0123456789");

  EXPECT_EQ(test.Key(), "0123456789"sv);

  test = isl.Emplace("123456789");

  EXPECT_EQ(test.Key(), "123456789"sv);

  test = isl.Emplace("23456789");

  EXPECT_EQ(isl.Find("0123456789").Key(), "0123456789"sv);
  EXPECT_EQ(isl.Find("23456789").Key(), "23456789"sv);
  EXPECT_EQ(isl.Find("123456789").Key(), "123456789"sv);
  EXPECT_EQ(isl.Find("test"), ISLEntry());

  EXPECT_TRUE(isl.Erase("23456789"));
  EXPECT_EQ(isl.Find("23456789"), ISLEntry());
  EXPECT_FALSE(isl.Erase("test"));
  EXPECT_EQ(isl.Find("test"), ISLEntry());
}

TEST_F(IntrusiveStringSetTest, IntrusiveStringSetAddFindTest) {
  IntrusiveStringSet ss;
  std::set<std::string> test_set;

  for (int i = 0; i < 10000; ++i) {
    test_set.insert(base::RandStr(20));
  }

  for (const auto& s : test_set) {
    auto e = ss.Add(s);
    EXPECT_EQ(e.Key(), s);
  }

  for (const auto& s : test_set) {
    auto e = ss.Find(s);
    EXPECT_EQ(e.Key(), s);
  }

  EXPECT_EQ(ss.Capacity(), 16384);
}

}  // namespace dfly
