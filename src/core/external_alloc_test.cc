// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/external_alloc.h"

#include "base/gtest.h"
#include "base/logging.h"

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

constexpr int64_t kSegSize = 256_MB;

std::map<int64_t, size_t> AllocateFully(ExternalAllocator* alloc) {
  std::map<int64_t, size_t> ranges;

  int64_t res = 0;
  while (res >= 0) {
    for (unsigned j = 1; j < 5; ++j) {
      size_t sz = 8000 * j;
      res = alloc->Malloc(sz);
      if (res < 0)
        break;
      auto [it, added] = ranges.emplace(res, sz);
      VLOG(1) << "res: " << res << " size: " << sz << " added: " << added;
      CHECK(added);
    }
  }

  return ranges;
}

constexpr size_t kMinBlockSize = ExternalAllocator::kMinBlockSize;

TEST_F(ExternalAllocatorTest, Basic) {
  int64_t res = ext_alloc_.Malloc(128);
  EXPECT_EQ(-kSegSize, res);

  ext_alloc_.AddStorage(0, kSegSize);
  EXPECT_EQ(0, ext_alloc_.Malloc(4000));   //  page0: 1
  EXPECT_EQ(kMinBlockSize, ext_alloc_.Malloc(4_KB));  // page0: 2
  size_t offset2 = ext_alloc_.Malloc(8193);   // page1: 1
  EXPECT_GT(offset2, 1_MB);  // another page.

  ext_alloc_.Free(offset2, 8193);                // should return the page to the segment.
  EXPECT_EQ(offset2, ext_alloc_.Malloc(16_KB));  // another page.  page1: 1

  ext_alloc_.Free(0, 4000);   // page0: 1
  ext_alloc_.Free(kMinBlockSize, 4_KB); // page0: 0
  EXPECT_EQ(0, ext_alloc_.Malloc(8_KB));  // page0
}

TEST_F(ExternalAllocatorTest, Invariants) {
  ext_alloc_.AddStorage(0, kSegSize);

  auto ranges = AllocateFully(&ext_alloc_);
  EXPECT_GT(ext_alloc_.allocated_bytes(), ext_alloc_.capacity() * 0.75);

  off_t last = 0;
  for (const auto& k_v : ranges) {
    ASSERT_GE(k_v.first, last);
    last = k_v.first + k_v.second;
  }

  for (const auto& k_v : ranges) {
    ext_alloc_.Free(k_v.first, k_v.second);
  }
  EXPECT_EQ(0, ext_alloc_.allocated_bytes());

  for (const auto& k_v : ranges) {
    int64_t res = ext_alloc_.Malloc(k_v.second);
    ASSERT_GE(res, 0);
  }
}

TEST_F(ExternalAllocatorTest, Classes) {
  ext_alloc_.AddStorage(0, kSegSize);
  off_t offs1 = ext_alloc_.Malloc(256_KB);
  EXPECT_EQ(detail::SMALL_P, ext_alloc_.PageClassFromOffset(offs1));
  off_t offs2 = ext_alloc_.Malloc(256_KB + 1);
  EXPECT_EQ(offs2, -kSegSize);

  ext_alloc_.AddStorage(kSegSize, kSegSize);
  offs2 = ext_alloc_.Malloc(256_KB + 1);
  EXPECT_EQ(detail::MEDIUM_P, ext_alloc_.PageClassFromOffset(offs2));
  off_t offs3 = ext_alloc_.Malloc(2_MB);
  EXPECT_EQ(detail::MEDIUM_P, ext_alloc_.PageClassFromOffset(offs3));
  EXPECT_EQ(2_MB, ExternalAllocator::GoodSize(2_MB));
}

}  // namespace dfly