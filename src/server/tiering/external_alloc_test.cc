// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/tiering/external_alloc.h"

#include <absl/types/span.h>

#include "base/gtest.h"
#include "base/logging.h"

namespace dfly::tiering {

using namespace std;

class ExternalAllocatorTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
  }

  static void TearDownTestSuite() {
  }

  // Allocates n blocks of size sz.
  vector<int64_t> AllocBlocks(size_t sz, unsigned n) {
    vector<int64_t> res;
    for (unsigned i = 0; i < n; ++i) {
      res.push_back(ext_alloc_.Malloc(sz));
      EXPECT_GE(res.back(), 0);
    }
    return res;
  }

  void FreeBlocks(absl::Span<const int64_t> blocks, size_t sz) {
    for (int64_t offs : blocks)
      ext_alloc_.Free(offs, sz);
  }

  size_t SegmentBytes() const {
    return ext_alloc_.GetStats().segment_bytes;
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
  EXPECT_EQ(0, ext_alloc_.Malloc(kMinBlockSize - 96));         //  page0: 1
  EXPECT_EQ(kMinBlockSize, ext_alloc_.Malloc(kMinBlockSize));  //  page0: 2

  constexpr auto kAnotherLen = kMinBlockSize * 2 - 10;
  size_t offset2 = ext_alloc_.Malloc(kAnotherLen);  // page1: 1
  EXPECT_EQ(offset2, 1_MB);                         // another page.

  ext_alloc_.Free(offset2, kAnotherLen);         // should return the page to the segment.
  EXPECT_EQ(offset2, ext_alloc_.Malloc(16_KB));  // another page.  page1: 1

  ext_alloc_.Free(0, kMinBlockSize - 96);         // page0: 1
  ext_alloc_.Free(kMinBlockSize, kMinBlockSize);  // page0: 0

  EXPECT_EQ(0, ext_alloc_.Malloc(kMinBlockSize * 2));  // page0
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
  using detail::ClassFromSize;

  ext_alloc_.AddStorage(0, kSegSize);
  constexpr size_t kMaxSmallPage = 64_KB;
  ASSERT_EQ(detail::SMALL_P, ClassFromSize(kMaxSmallPage));
  ASSERT_EQ(detail::MEDIUM_P, ClassFromSize(kMaxSmallPage + 1));
  ASSERT_EQ(detail::LARGE_P, ClassFromSize(1_MB + 1));

  off_t offs1 = ext_alloc_.Malloc(kMaxSmallPage);
  EXPECT_EQ(offs1, 0);

  off_t offs2 = ext_alloc_.Malloc(kMaxSmallPage + 1);
  EXPECT_EQ(offs2, -kSegSize);

  ext_alloc_.AddStorage(kSegSize, kSegSize);
  offs2 = ext_alloc_.Malloc(kMaxSmallPage * 2 + 1);
  ASSERT_GT(offs2, 0);
  offs2 = ext_alloc_.Malloc(1_MB);
  ASSERT_GT(offs2, 0);

  off_t offs3 = ext_alloc_.Malloc(1_MB + 1);
  ASSERT_LT(offs3, 0);
  ext_alloc_.AddStorage(kSegSize * 2, kSegSize);
  offs3 = ext_alloc_.Malloc(1_MB + 1);
  ASSERT_GT(offs3, 0);

  EXPECT_EQ(1_MB + 4_KB, ExternalAllocator::GoodSize(1_MB + 1));
}

// Fill up the allocator until it has to grow, remove 90% and make sure it has free space even with
// extreme fragmentation
TEST_F(ExternalAllocatorTest, EmptyFull) {
  const int kAllocSize = kMinBlockSize;
  ext_alloc_.AddStorage(0, 2 * kSegSize);

  // Fill up the allocator
  vector<int64_t> offsets;
  int64_t offset;
  do {
    offset = ext_alloc_.Malloc(kAllocSize);
    if (offset >= 0)
      offsets.push_back(offset);
  } while (offset >= 0);

  // Keep only 10%, free 90%
  for (size_t i = 0; i < offsets.size(); i++) {
    if (i % 10 == 0)
      continue;
    ext_alloc_.Free(offsets[i], kAllocSize);
  }

  // Expect to succeed adding 10% without growing
  for (size_t i = 0; i < offsets.size() / 10; i++)
    EXPECT_GT(ext_alloc_.Malloc(kAllocSize), 0u);
}

TEST_F(ExternalAllocatorTest, AllocLarge) {
  constexpr size_t kFirstSize = 1_MB + 1;
  constexpr size_t kSecondSize = 2_MB + 1;

  EXPECT_EQ(ext_alloc_.Malloc(kFirstSize), -kSegSize);
  EXPECT_EQ(ext_alloc_.allocated_bytes(), 0u);

  ext_alloc_.AddStorage(0, 2 * kSegSize);
  int64_t first = ext_alloc_.Malloc(kFirstSize);
  ASSERT_EQ(first, 0);
  EXPECT_EQ(ext_alloc_.allocated_bytes(), ExternalAllocator::GoodSize(kFirstSize));

  int64_t second = ext_alloc_.Malloc(kSecondSize);
  ASSERT_GE(second, 0);
  size_t large_bytes =
      ExternalAllocator::GoodSize(kFirstSize) + ExternalAllocator::GoodSize(kSecondSize);
  EXPECT_EQ(ext_alloc_.allocated_bytes(), large_bytes);

  int64_t small = ext_alloc_.Malloc(kMinBlockSize);
  ASSERT_GE(small, 0);
  EXPECT_EQ(ext_alloc_.allocated_bytes(), large_bytes + kMinBlockSize);

  ext_alloc_.Free(first, kFirstSize);
  EXPECT_EQ(ext_alloc_.allocated_bytes(), ExternalAllocator::GoodSize(kSecondSize) + kMinBlockSize);
  int64_t reused = ext_alloc_.Malloc(kFirstSize);
  ASSERT_EQ(reused, first);
  EXPECT_EQ(ext_alloc_.allocated_bytes(), large_bytes + kMinBlockSize);

  ext_alloc_.Free(second, kSecondSize);
  ext_alloc_.Free(reused, kFirstSize);
  ext_alloc_.Free(small, kMinBlockSize);
  EXPECT_EQ(ext_alloc_.allocated_bytes(), 0u);
}

// Pages that become fully free are unlinked from the middle and the tail of the free pages list
// and returned to the segment.
TEST_F(ExternalAllocatorTest, FreePageInsideFreeList) {
  constexpr size_t kBlock = 64_KB;  // 16 blocks per 1MB page.
  constexpr unsigned kBlocksInPage = 1_MB / kBlock;

  ext_alloc_.AddStorage(0, kSegSize);

  // Fill pages 0, 1 and 2.
  vector<int64_t> blocks;
  for (unsigned i = 0; i < 3 * kBlocksInPage; ++i) {
    blocks.push_back(ext_alloc_.Malloc(kBlock));
    ASSERT_EQ(blocks.back(), i * kBlock);
  }

  // Free the first block of each page, so the free list of the bin is: 2 -> 1 -> 0.
  for (unsigned page = 0; page < 3; ++page)
    ext_alloc_.Free(blocks[page * kBlocksInPage], kBlock);

  // Fully free page 1 (middle of the list), then page 0 (tail of the list).
  for (unsigned page : {1, 0}) {
    for (unsigned i = 1; i < kBlocksInPage; ++i)
      ext_alloc_.Free(blocks[page * kBlocksInPage + i], kBlock);
  }
  EXPECT_EQ(ext_alloc_.allocated_bytes(), (kBlocksInPage - 1) * kBlock);

  // Page 0 went back to the segment and can host another bin.
  EXPECT_EQ(ext_alloc_.Malloc(4_KB), 0);

  // Page 2 is the only page left in the list, then page 1 is taken from the segment.
  EXPECT_EQ(ext_alloc_.Malloc(kBlock), 2_MB);
  EXPECT_EQ(ext_alloc_.Malloc(kBlock), 1_MB);
}

// An empty segment is returned to the extent tree, so its range can be reused by another page
// class or by a large allocation.
TEST_F(ExternalAllocatorTest, ReleaseEmptySegment) {
  ext_alloc_.AddStorage(0, kSegSize);

  for (size_t sz : {4_KB, 128_KB, 2_MB}) {  // small, medium and large page classes.
    int64_t offs = ext_alloc_.Malloc(sz);
    ASSERT_EQ(offs, 0) << sz;
    ext_alloc_.Free(offs, sz);

    auto stats = ext_alloc_.GetStats();
    EXPECT_EQ(stats.segment_bytes, 0u) << sz;
    EXPECT_EQ(stats.free_extent_bytes, size_t(kSegSize)) << sz;
  }
}

// Releasing empty segments from the middle, the tail and the head of the segment queue keeps the
// queue consistent.
TEST_F(ExternalAllocatorTest, ReleaseSegmentFromQueue) {
  constexpr size_t kBlock = 64_KB;  // 16 blocks per 1MB small page.
  constexpr unsigned kBlocksInPage = 1_MB / kBlock;
  constexpr unsigned kBlocksInSeg = kSegSize / kBlock;

  ext_alloc_.AddStorage(0, 3 * kSegSize);

  // Fill segments A and B, then allocate once more to open C. A and B are detached when full.
  vector<int64_t> a_blocks = AllocBlocks(kBlock, kBlocksInSeg);
  vector<int64_t> b_blocks = AllocBlocks(kBlock, kBlocksInSeg);
  vector<int64_t> c_blocks = AllocBlocks(kBlock, 1);
  ASSERT_EQ(b_blocks[0], kSegSize);
  ASSERT_EQ(c_blocks[0], 2 * kSegSize);

  // Free the first page of A and then of B, so the segment queue is: C -> A -> B.
  auto a_span = absl::MakeConstSpan(a_blocks);
  auto b_span = absl::MakeConstSpan(b_blocks);
  FreeBlocks(a_span.first(kBlocksInPage), kBlock);
  FreeBlocks(b_span.first(kBlocksInPage), kBlock);

  // Release A from the middle of the queue. Its range can be reused by a large allocation.
  FreeBlocks(a_span.subspan(kBlocksInPage), kBlock);
  EXPECT_EQ(SegmentBytes(), 2 * kSegSize);
  int64_t large = ext_alloc_.Malloc(2_MB);
  EXPECT_EQ(large, 0);
  ext_alloc_.Free(large, 2_MB);

  // Release B from the tail of the queue and then C, the last queued segment.
  FreeBlocks(b_span.subspan(kBlocksInPage), kBlock);
  EXPECT_EQ(SegmentBytes(), size_t(kSegSize));
  FreeBlocks(c_blocks, kBlock);
  EXPECT_EQ(SegmentBytes(), 0u);

  // A new segment is opened once the queue is empty.
  EXPECT_EQ(ext_alloc_.Malloc(kBlock), 0);
}

// A full segment stays at the head of the segment queue until the next FindPage() detaches it.
// Freeing a page in such a segment must not relink it to itself and cut off the rest of the queue,
// and both segments must be released safely once they are empty.
TEST_F(ExternalAllocatorTest, FullHeadSegmentKeepsQueue) {
  constexpr size_t kBlock = 64_KB;  // 16 blocks per 1MB small page.
  constexpr unsigned kBlocksInPage = 1_MB / kBlock;
  constexpr unsigned kBlocksInSeg = kSegSize / kBlock;

  ext_alloc_.AddStorage(0, 2 * kSegSize);

  // Fill segment A, then allocate once more to detach A and open segment B.
  vector<int64_t> a_blocks = AllocBlocks(kBlock, kBlocksInSeg);
  vector<int64_t> b_blocks = AllocBlocks(kBlock, 1);
  ASSERT_EQ(b_blocks[0], kSegSize);

  // Free the first page of A, so A is queued after B.
  auto a_span = absl::MakeConstSpan(a_blocks);
  FreeBlocks(a_span.first(kBlocksInPage), kBlock);

  // Fill B, so it is full but still the queue head: B -> A. Then free it, so B is released.
  FreeBlocks(AllocBlocks(kBlock, kBlocksInSeg - 1), kBlock);
  FreeBlocks(b_blocks, kBlock);
  EXPECT_EQ(SegmentBytes(), size_t(kSegSize));

  // A is still queued, so its free page is reused instead of opening a new segment.
  int64_t offs = ext_alloc_.Malloc(kBlock);
  EXPECT_EQ(offs, a_blocks[0]);
  ext_alloc_.Free(offs, kBlock);

  // Release A as well.
  FreeBlocks(a_span.subspan(kBlocksInPage), kBlock);
  EXPECT_EQ(SegmentBytes(), 0u);
}

}  // namespace dfly::tiering
