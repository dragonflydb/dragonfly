// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/external_alloc.h"

#include "base/gtest.h"

namespace dfly {

using namespace std;

class ExternalAllocatorTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
  }

  static void TearDownTestSuite() {
  }

  ExternalAllocator ext_alloc_;
};

constexpr int64_t kSegSize = 1 << 28;

TEST_F(ExternalAllocatorTest, Basic) {
  int64_t res = ext_alloc_.Malloc(128);
  EXPECT_EQ(-kSegSize, res);

  ext_alloc_.AddStorage(0, kSegSize);
  EXPECT_EQ(0, ext_alloc_.Malloc(4000));
  EXPECT_EQ(4096, ext_alloc_.Malloc(4096));
  EXPECT_EQ(1048576, ext_alloc_.Malloc(8192));  // another page.

  ext_alloc_.Free(1048576, 8192);  // should return the page to the segment.
  EXPECT_EQ(1048576, ext_alloc_.Malloc(1 << 14));  // another page.

  ext_alloc_.Free(0, 4000);
  ext_alloc_.Free(4096, 4096);
  EXPECT_EQ(0, ext_alloc_.Malloc(4097));
}

TEST_F(ExternalAllocatorTest, Invariants) {
  ext_alloc_.AddStorage(0, kSegSize);

  std::map<int64_t, size_t> ranges;

  int64_t res = 0;
  while (res >= 0) {
    for (unsigned j = 1; j < 5; ++j) {
      size_t sz = 4000 * j;
      res = ext_alloc_.Malloc(sz);
      if (res < 0)
        break;
      auto [it, added] = ranges.emplace(res, sz);
      ASSERT_TRUE(added);
    }
  }

  EXPECT_GT(ext_alloc_.allocated_bytes(), ext_alloc_.capacity() * 0.75);

  off_t last = 0;
  for (const auto& k_v : ranges) {
    ASSERT_GE(k_v.first, last);
    last = k_v.first + k_v.second;
  }
}

}  // namespace dfly