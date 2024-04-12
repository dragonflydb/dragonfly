// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/hll_family.h"

#include "base/gtest.h"
#include "base/logging.h"
#include "facade/facade_test.h"
#include "server/command_registry.h"
#include "server/test_utils.h"

using namespace testing;
using namespace std;
using namespace util;

namespace dfly {

class HllFamilyTest : public BaseFamilyTest {
 protected:
  std::string GenerateUniqueValue(int index) {
    return "Value_{" + std::to_string(index) + "}";
  }
};

TEST_F(HllFamilyTest, Simple) {
  EXPECT_EQ(CheckedInt({"pfadd", "key", "1"}), 1);
  EXPECT_EQ(CheckedInt({"pfadd", "key", "1"}), 0);
  EXPECT_EQ(CheckedInt({"pfcount", "key"}), 1);
}

TEST_F(HllFamilyTest, Promote) {
  int unique_values = 20000;
  // Sparse hll is promoted to dense at the 1660th+- insertion
  // This value varies if any parameter in hyperloglog.c changes.
  int promote_i = 1660;
  // Keep consistent with hyperloglog.c
  int kHllSparseMaxBytes = 3000;
  int kHllDenseSize = 12304;
  for (int i = 0; i < unique_values; ++i) {
    std::string newkey = GenerateUniqueValue(i);
    Run({"pfadd", "key", newkey});
    if (i < promote_i) {
      EXPECT_LT(CheckedInt({"strlen", "key"}), kHllSparseMaxBytes + 1);
    } else {
      EXPECT_EQ(CheckedInt({"strlen", "key"}), kHllDenseSize);
    }
  }
  // HyperLogLog computations come with a
  // margin of error, with a standard error rate of 0.81%.
  // Set it to 5% so this test won't fail unless something went wrong badly.
  EXPECT_LT(std::abs(CheckedInt({"pfcount", "key"}) - unique_values * 1.0) / unique_values, 0.05);
}

TEST_F(HllFamilyTest, MultipleValues) {
  EXPECT_EQ(CheckedInt({"pfadd", "key", "1", "2", "3"}), 1);
  EXPECT_EQ(CheckedInt({"pfcount", "key"}), 3);
  EXPECT_EQ(CheckedInt({"pfadd", "key", "1", "2", "3"}), 0);
  EXPECT_EQ(CheckedInt({"pfcount", "key"}), 3);
  EXPECT_EQ(CheckedInt({"pfadd", "key", "1"}), 0);
  EXPECT_EQ(CheckedInt({"pfcount", "key"}), 3);
  EXPECT_EQ(CheckedInt({"pfadd", "key", "2"}), 0);
  EXPECT_EQ(CheckedInt({"pfcount", "key"}), 3);
  EXPECT_EQ(CheckedInt({"pfadd", "key", "3"}), 0);
  EXPECT_EQ(CheckedInt({"pfcount", "key"}), 3);
  EXPECT_EQ(CheckedInt({"pfadd", "key", "3", "4"}), 1);
  EXPECT_EQ(CheckedInt({"pfcount", "key"}), 4);
  EXPECT_EQ(CheckedInt({"pfadd", "key", "5"}), 1);
  EXPECT_EQ(CheckedInt({"pfcount", "key"}), 5);
  EXPECT_EQ(CheckedInt({"pfadd", "key", "1", "2", "3", "4", "5"}), 0);
  EXPECT_EQ(CheckedInt({"pfcount", "key"}), 5);
}

TEST_F(HllFamilyTest, MultipleValues_random) {
  int insertions = 20000;
  int unique_values = 0;
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(1, 20);
  // cumulated pfadd result
  for (int i = 0; i < insertions; ++i) {
    // Number of values to insert
    int num_values = dis(gen);
    unique_values += num_values;

    // Prepare the command
    std::vector<std::string> values;
    values.reserve(num_values + 2);
    values.push_back("pfadd");
    values.push_back("key");

    // Generate and add unique values to the command
    for (int j = 0; j < num_values; ++j) {
      values.push_back(GenerateUniqueValue(i * 20 + j));
    }

    std::vector<std::string_view> commandViews;
    for (const auto& val : values) {
      commandViews.push_back(val);
    }
    Run(commandViews);
  }
  // HyperLogLog computations come with a
  // margin of error, with a standard error rate of 0.81%.
  // Set it to 5% so this test won't fail unless something went wrong badly.
  EXPECT_LT(std::abs(CheckedInt({"pfcount", "key"}) - unique_values * 1.0) / unique_values, 0.05);
}

TEST_F(HllFamilyTest, AddInvalid) {
  EXPECT_EQ(Run({"set", "key", "..."}), "OK");
  EXPECT_THAT(Run({"pfadd", "key", "1"}), ErrArg(HllFamily::kInvalidHllErr));
  EXPECT_THAT(Run({"pfcount", "key"}), ErrArg(HllFamily::kInvalidHllErr));
}

TEST_F(HllFamilyTest, OtherType) {
  Run({"zadd", "key", "1", "a"});
  EXPECT_THAT(Run({"pfadd", "key", "1"}),
              ErrArg("Operation against a key holding the wrong kind of value"));
  EXPECT_THAT(Run({"pfcount", "key"}),
              ErrArg("Operation against a key holding the wrong kind of value"));
}

TEST_F(HllFamilyTest, CountEmpty) {
  EXPECT_EQ(CheckedInt({"pfcount", "nonexisting"}), 0);
}

TEST_F(HllFamilyTest, CountInvalid) {
  EXPECT_EQ(Run({"set", "key", "..."}), "OK");
  EXPECT_THAT(Run({"pfcount", "key"}), ErrArg(HllFamily::kInvalidHllErr));
}

TEST_F(HllFamilyTest, CountMultiple) {
  EXPECT_EQ(CheckedInt({"pfadd", "key1", "1", "2", "3"}), 1);
  EXPECT_EQ(CheckedInt({"pfcount", "key1"}), 3);

  EXPECT_EQ(CheckedInt({"pfadd", "key2", "1", "2", "3"}), 1);
  EXPECT_EQ(CheckedInt({"pfcount", "key2"}), 3);

  EXPECT_EQ(CheckedInt({"pfadd", "key3", "2", "3"}), 1);
  EXPECT_EQ(CheckedInt({"pfcount", "key3"}), 2);

  EXPECT_EQ(CheckedInt({"pfadd", "key4", "4", "5"}), 1);
  EXPECT_EQ(CheckedInt({"pfcount", "key4"}), 2);

  EXPECT_EQ(CheckedInt({"pfcount", "key1", "key4"}), 5);

  EXPECT_EQ(CheckedInt({"pfcount", "non-existing-key1", "non-existing-key2"}), 0);

  EXPECT_EQ(CheckedInt({"pfcount", "key1", "non-existing-key"}), 3);

  EXPECT_EQ(CheckedInt({"pfcount", "key1", "key2"}), 3);
  EXPECT_EQ(CheckedInt({"pfcount", "key1", "key3"}), 3);
  EXPECT_EQ(CheckedInt({"pfcount", "key1", "key2", "key3"}), 3);
  EXPECT_EQ(CheckedInt({"pfcount", "key1", "key2", "key3", "key4"}), 5);
  EXPECT_EQ(CheckedInt({"pfcount", "key1", "key2", "key3", "key4", "non-existing"}), 5);
  EXPECT_EQ(CheckedInt({"pfcount", "key1", "key4"}), 5);
}

TEST_F(HllFamilyTest, MergeToNew) {
  EXPECT_EQ(CheckedInt({"pfadd", "key1", "1", "2", "3"}), 1);
  EXPECT_EQ(CheckedInt({"pfadd", "key2", "4", "5"}), 1);
  EXPECT_EQ(Run({"pfmerge", "key3", "key1", "key2"}), "OK");
  EXPECT_EQ(CheckedInt({"pfcount", "key3"}), 5);
}

TEST_F(HllFamilyTest, MergeToExisting) {
  EXPECT_EQ(CheckedInt({"pfadd", "key1", "1", "2", "3"}), 1);
  EXPECT_EQ(CheckedInt({"pfadd", "key2", "4", "5"}), 1);
  EXPECT_EQ(Run({"pfmerge", "key2", "key1"}), "OK");
  EXPECT_EQ(CheckedInt({"pfcount", "key2"}), 5);
}

TEST_F(HllFamilyTest, MergeNonExisting) {
  EXPECT_EQ(CheckedInt({"pfadd", "key1", "1", "2", "3"}), 1);
  EXPECT_EQ(Run({"pfmerge", "key3", "key1", "key2"}), "OK");
  EXPECT_EQ(CheckedInt({"pfcount", "key3"}), 3);
}

TEST_F(HllFamilyTest, MergeOverlapping) {
  EXPECT_EQ(CheckedInt({"pfadd", "key1", "1", "2", "3"}), 1);
  EXPECT_EQ(CheckedInt({"pfadd", "key2", "2", "3"}), 1);
  EXPECT_EQ(CheckedInt({"pfadd", "key3", "1", "3"}), 1);
  EXPECT_EQ(CheckedInt({"pfadd", "key4", "2", "3"}), 1);
  EXPECT_EQ(CheckedInt({"pfadd", "key5", "3"}), 1);
  EXPECT_EQ(Run({"pfmerge", "key6", "key1", "key2", "key3", "key4", "key5"}), "OK");
  EXPECT_EQ(CheckedInt({"pfcount", "key6"}), 3);
}

TEST_F(HllFamilyTest, MergeInvalid) {
  EXPECT_EQ(CheckedInt({"pfadd", "key1", "1", "2", "3"}), 1);
  EXPECT_EQ(Run({"set", "key2", "..."}), "OK");
  EXPECT_THAT(Run({"pfmerge", "key1", "key2"}), ErrArg(HllFamily::kInvalidHllErr));
  EXPECT_EQ(CheckedInt({"pfcount", "key1"}), 3);
}

}  // namespace dfly
