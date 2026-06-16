// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "base/gtest.h"
#include "core/cuckoo.h"

namespace dfly {

using namespace std;

class CuckooFilterTest : public ::testing::Test {
 protected:
  CuckooFilterTest() : cf_(128, std::pmr::get_default_resource()) {
  }

  CuckooFilter cf_;
};

TEST_F(CuckooFilterTest, InsertAndExists) {
  const uint64_t h = CuckooFilter::Hash("foo");
  EXPECT_FALSE(cf_.Exists(h));
  EXPECT_TRUE(cf_.Insert(h));
  EXPECT_TRUE(cf_.Exists(h));
  EXPECT_EQ(cf_.NumItems(), 1u);
}

TEST_F(CuckooFilterTest, DeleteReducesCount) {
  const uint64_t h = CuckooFilter::Hash("bar");
  EXPECT_TRUE(cf_.Insert(h));
  EXPECT_TRUE(cf_.Delete(h));
  EXPECT_FALSE(cf_.Exists(h));
  EXPECT_EQ(cf_.NumItems(), 0u);
}

TEST_F(CuckooFilterTest, DeleteNonExistentReturnsFalse) {
  const uint64_t h = CuckooFilter::Hash("ghost");
  EXPECT_FALSE(cf_.Delete(h));
  EXPECT_EQ(cf_.NumItems(), 0u);
}

TEST_F(CuckooFilterTest, DuplicateInserts) {
  const uint64_t h = CuckooFilter::Hash("dup");
  EXPECT_TRUE(cf_.Insert(h));
  EXPECT_TRUE(cf_.Insert(h));
  EXPECT_EQ(cf_.NumItems(), 2u);
  // First delete: item still present
  EXPECT_TRUE(cf_.Delete(h));
  EXPECT_TRUE(cf_.Exists(h));
  EXPECT_EQ(cf_.NumItems(), 1u);
  // Second delete: now gone
  EXPECT_TRUE(cf_.Delete(h));
  EXPECT_FALSE(cf_.Exists(h));
  EXPECT_EQ(cf_.NumItems(), 0u);
}

TEST_F(CuckooFilterTest, FillBeyondCapacityExpands) {
  // Insert many items, filter should expand via AddNewSubFilter.
  const size_t n = 1000;
  size_t inserted = 0;
  for (size_t i = 0; i < n; ++i) {
    if (cf_.Insert(CuckooFilter::Hash(to_string(i))))
      ++inserted;
  }
  EXPECT_EQ(inserted, n);
  EXPECT_EQ(cf_.NumItems(), n);
  EXPECT_GT(cf_.NumKOInserts(), 0u) << "KO insert was never exercised";

  for (size_t i = 0; i < n; ++i) {
    EXPECT_TRUE(cf_.Exists(CuckooFilter::Hash(to_string(i)))) << "missing item " << i;
  }
}

TEST_F(CuckooFilterTest, NoExpansionRejectWhenFull) {
  // A small filter with expansion=0 must reject inserts once full.
  CuckooFilter small(4, std::pmr::get_default_resource(), CuckooFilter::kDefaultSlotsPerBucket,
                     CuckooFilter::kDefaultMaxIterations,
                     /*expansion=*/0);

  size_t inserted = 0;
  for (size_t i = 0; i < 1000; ++i) {
    if (small.Insert(CuckooFilter::Hash(to_string(i))))
      ++inserted;
    else
      break;
  }
  // Must have rejected at some point — can't insert 1000 into a tiny fixed filter.
  EXPECT_LT(inserted, 1000u);
  EXPECT_EQ(small.NumItems(), inserted);
}

TEST_F(CuckooFilterTest, UsedMemoryGrowsOnExpansion) {
  const size_t before = cf_.UsedMemory();
  // Force expansion by filling past capacity.
  for (size_t i = 0; i < 500; ++i) {
    cf_.Insert(CuckooFilter::Hash(to_string(i)));
  }
  EXPECT_GT(cf_.UsedMemory(), before);
}

TEST_F(CuckooFilterTest, InsertUniquePreventsduplicates) {
  const uint64_t h = CuckooFilter::Hash("unique");
  EXPECT_TRUE(cf_.InsertUnique(h));
  EXPECT_FALSE(cf_.InsertUnique(h));  // already exists
  EXPECT_EQ(cf_.NumItems(), 1u);
}

TEST_F(CuckooFilterTest, HashIsDeterministic) {
  EXPECT_EQ(CuckooFilter::Hash("hello"), CuckooFilter::Hash("hello"));
  EXPECT_NE(CuckooFilter::Hash("hello"), CuckooFilter::Hash("world"));
  EXPECT_NE(CuckooFilter::Hash("hello"), 0u);
}

}  // namespace dfly
