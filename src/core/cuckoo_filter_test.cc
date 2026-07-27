// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <absl/base/internal/endian.h>
#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>

#include <limits>

#include "base/gtest.h"
#include "core/compact_object.h"
#include "core/cuckoo.h"

namespace dfly {

using namespace std;

class CuckooFilterTest : public ::testing::Test {
 protected:
  CuckooFilterTest()
      : cf_(CuckooFilter::Options{.capacity = 128}, std::pmr::get_default_resource()) {
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
  CuckooFilter small({.capacity = 4, .expansion = 0}, std::pmr::get_default_resource());

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

TEST_F(CuckooFilterTest, MallocUsedGrowsOnExpansion) {
  const size_t before = cf_.MallocUsed();
  // Force expansion by filling past capacity.
  for (size_t i = 0; i < 500; ++i) {
    cf_.Insert(CuckooFilter::Hash(to_string(i)));
  }
  EXPECT_GT(cf_.MallocUsed(), before);
}

TEST_F(CuckooFilterTest, InsertUniquePreventsduplicates) {
  const uint64_t h = CuckooFilter::Hash("unique");
  EXPECT_TRUE(cf_.InsertUnique(h));
  EXPECT_FALSE(cf_.InsertUnique(h));  // already exists
  EXPECT_EQ(cf_.NumItems(), 1u);
}

TEST_F(CuckooFilterTest, CountTracksDuplicateInsertsAndDeletes) {
  const uint64_t h = CuckooFilter::Hash("counted");
  EXPECT_EQ(cf_.Count(h), 0u);

  EXPECT_TRUE(cf_.Insert(h));
  EXPECT_EQ(cf_.Count(h), 1u);

  // Insert (not InsertUnique) never dedups — a second insert of the same item occupies
  // its own slot, so Count reflects how many times it was added.
  EXPECT_TRUE(cf_.Insert(h));
  EXPECT_EQ(cf_.Count(h), 2u);

  EXPECT_TRUE(cf_.Delete(h));
  EXPECT_EQ(cf_.Count(h), 1u);
}

TEST_F(CuckooFilterTest, DumpAndLoadRoundTrip) {
  auto* mr = std::pmr::get_default_resource();
  CuckooFilter src(CuckooFilter::Options{.capacity = 8, .expansion = 2}, mr);

  constexpr unsigned kNumElems = 200;
  for (unsigned i = 0; i < kNumElems; ++i)
    src.Insert(CuckooFilter::Hash(absl::StrCat("item", i)));
  ASSERT_GT(src.NumFilters(), 1u) << "test needs growth to cover multiple sub-filters";

  CFDumpIterator it(src, 0);
  std::vector<CFChunk> chunks;
  for (auto c = it.Next(); c.cursor; c = it.Next())
    chunks.push_back(std::move(c));

  ASSERT_GE(chunks.size(), 2u);
  auto cit = chunks.cbegin();
  EXPECT_EQ(cit->cursor, 1);

  auto init_result = LoadCFHeader(cit->data, mr);
  ASSERT_TRUE(init_result.has_value());

  CuckooFilter* loaded = *init_result;
  absl::Cleanup cleanup = [&loaded] { CompactObj::DeleteMR<CuckooFilter>(loaded); };
  EXPECT_TRUE(loaded->IsLoading());

  ++cit;
  for (; cit != chunks.cend(); ++cit)
    ASSERT_EQ(LoadCFChunk(cit->cursor, cit->data, loaded), CFLoadResult::kOk);

  EXPECT_FALSE(loaded->IsLoading());
  EXPECT_EQ(loaded->SlotsPerBucket(), src.SlotsPerBucket());
  EXPECT_EQ(loaded->MaxIterations(), src.MaxIterations());
  EXPECT_EQ(loaded->Expansion(), src.Expansion());
  EXPECT_EQ(loaded->NumBuckets(), src.NumBuckets());
  EXPECT_EQ(loaded->NumItems(), src.NumItems());
  EXPECT_EQ(loaded->NumDeletes(), src.NumDeletes());
  EXPECT_EQ(loaded->NumFilters(), src.NumFilters());

  for (size_t i = 0; i < src.NumFilters(); ++i)
    EXPECT_EQ(loaded->FilterBytes(i), src.FilterBytes(i));

  for (unsigned i = 0; i < kNumElems; ++i)
    EXPECT_TRUE(loaded->Exists(CuckooFilter::Hash(absl::StrCat("item", i))))
        << "Missing item " << i;
}

TEST_F(CuckooFilterTest, DumpPastEndCursorReturnsEof) {
  cf_.Insert(CuckooFilter::Hash("x"));  // non-empty: exercise past-end resolution, not the
                                        // empty-filter shortcut covered by the test below.
  CFDumpIterator it(cf_, 999999);
  auto [cursor, data] = it.Next();
  EXPECT_EQ(cursor, 0);
  EXPECT_TRUE(data.empty());
}

TEST_F(CuckooFilterTest, DumpOfEmptyFilterShortCircuits) {
  // Matches upstream CF.SCANDUMP: a filter with zero items always reports done immediately,
  // regardless of the requested cursor — unlike BF.SCANDUMP, which has no such shortcut.
  EXPECT_EQ(cf_.NumItems(), 0u);

  for (int64_t cursor : {0, 1, 999999}) {
    CFDumpIterator it(cf_, cursor);
    auto [next_cursor, data] = it.Next();
    EXPECT_EQ(next_cursor, 0) << "cursor=" << cursor;
    EXPECT_TRUE(data.empty()) << "cursor=" << cursor;
  }
}

TEST_F(CuckooFilterTest, DumpAndLoadRoundTripAcrossMultipleChunksOfOneFilter) {
  // kMaxChunkSize is 16MiB; build a single sub-filter bigger than that so a dump/load
  // round trip must exercise the mid-filter continuation path (byte_offset_ > 0 without
  // advancing filter_index_), not just the across-filter boundary already covered above.
  auto* mr = std::pmr::get_default_resource();
  // capacity/slots_per_bucket rounds up to num_buckets; pick capacity so num_buckets *
  // slots_per_bucket clears 16MiB comfortably, with expansion=0 to keep it a single filter.
  CuckooFilter src(CuckooFilter::Options{.capacity = 20'000'000, .expansion = 0}, mr);
  ASSERT_EQ(src.NumFilters(), 1u);
  ASSERT_GT(src.FilterBytes(0).size(), CFDumpIterator::kMaxChunkSize);

  constexpr unsigned kNumElems = 500;
  for (unsigned i = 0; i < kNumElems; ++i)
    ASSERT_TRUE(src.Insert(CuckooFilter::Hash(absl::StrCat("item", i))));

  CFDumpIterator it(src, 0);
  std::vector<CFChunk> chunks;
  for (auto c = it.Next(); c.cursor; c = it.Next())
    chunks.push_back(std::move(c));

  // header + at least 2 chunks for the oversized single filter.
  ASSERT_GE(chunks.size(), 3u);

  auto cit = chunks.cbegin();
  auto init_result = LoadCFHeader(cit->data, mr);
  ASSERT_TRUE(init_result.has_value());
  CuckooFilter* loaded = *init_result;
  absl::Cleanup cleanup = [&loaded] { CompactObj::DeleteMR<CuckooFilter>(loaded); };

  ++cit;
  for (; cit != chunks.cend(); ++cit)
    ASSERT_EQ(LoadCFChunk(cit->cursor, cit->data, loaded), CFLoadResult::kOk);

  EXPECT_FALSE(loaded->IsLoading());
  ASSERT_EQ(loaded->NumFilters(), 1u);
  EXPECT_EQ(loaded->FilterBytes(0), src.FilterBytes(0));

  for (unsigned i = 0; i < kNumElems; ++i)
    EXPECT_TRUE(loaded->Exists(CuckooFilter::Hash(absl::StrCat("item", i))))
        << "Missing item " << i;
}

TEST_F(CuckooFilterTest, LoadHeaderRejectsBadVersionAndSize) {
  auto* mr = std::pmr::get_default_resource();

  auto too_short = LoadCFHeader("abc", mr);
  ASSERT_FALSE(too_short.has_value());
  EXPECT_EQ(too_short.error(), CFLoadResult::kBadInput);

  std::string bad_version(41, '\0');  // correct size, all-zero version field (!= kCfDumpVersion)
  auto bad = LoadCFHeader(bad_version, mr);
  ASSERT_FALSE(bad.has_value());
  EXPECT_EQ(bad.error(), CFLoadResult::kBadVersion);
}

namespace {
// Hand-builds a raw CF.LOADCHUNK header blob (see the wire format documented next to
// CFDumpIterator in core/cuckoo.h), so tests can feed LoadCFHeader adversarial field
// combinations without needing a real CuckooFilter to have produced them.
std::string BuildRawCfHeader(uint8_t slots_per_bucket, uint16_t max_iterations, uint16_t expansion,
                             uint64_t num_buckets, uint64_t num_items, uint64_t num_deletes,
                             uint64_t num_filters) {
  std::string out;
  char buf[8];

  absl::little_endian::Store32(buf, 1);  // kCfDumpVersion
  out.append(buf, 4);
  out.push_back(static_cast<char>(slots_per_bucket));
  absl::little_endian::Store16(buf, max_iterations);
  out.append(buf, 2);
  absl::little_endian::Store16(buf, expansion);
  out.append(buf, 2);
  absl::little_endian::Store64(buf, num_buckets);
  out.append(buf, 8);
  absl::little_endian::Store64(buf, num_items);
  out.append(buf, 8);
  absl::little_endian::Store64(buf, num_deletes);
  out.append(buf, 8);
  absl::little_endian::Store64(buf, num_filters);
  out.append(buf, 8);

  return out;
}
}  // namespace

TEST_F(CuckooFilterTest, LoadHeaderRejectsOverflowingFilterSize) {
  auto* mr = std::pmr::get_default_resource();

  // num_buckets alone (a legal power of two) already exceeds AddNewSubFilter's kMaxBuckets
  // invariant ((1<<56)-1) for organically-grown filters. A well-behaved header can never
  // claim this; LoadCFHeader must reject it instead of attempting a ~4-exabyte allocation.
  auto huge_buckets =
      LoadCFHeader(BuildRawCfHeader(/*slots_per_bucket=*/2, /*max_iterations=*/10, /*expansion=*/0,
                                    /*num_buckets=*/1ULL << 60, /*num_items=*/0, /*num_deletes=*/0,
                                    /*num_filters=*/1),
                   mr);
  ASSERT_FALSE(huge_buckets.has_value());
  EXPECT_EQ(huge_buckets.error(), CFLoadResult::kBadInput);

  // expansion == 0 means growth never advances, so num_filters is the only thing standing
  // between this header and an unbounded reserve() — must be rejected outright.
  auto huge_filter_count =
      LoadCFHeader(BuildRawCfHeader(/*slots_per_bucket=*/2, /*max_iterations=*/10, /*expansion=*/0,
                                    /*num_buckets=*/16, /*num_items=*/0, /*num_deletes=*/0,
                                    /*num_filters=*/std::numeric_limits<uint64_t>::max()),
                   mr);
  ASSERT_FALSE(huge_filter_count.has_value());
  EXPECT_EQ(huge_filter_count.error(), CFLoadResult::kBadInput);

  // Sanity check: the same shape with reasonable values must still succeed.
  auto ok = LoadCFHeader(BuildRawCfHeader(2, 10, 2, 16, 0, 0, 1), mr);
  ASSERT_TRUE(ok.has_value());
  CompactObj::DeleteMR<CuckooFilter>(*ok);
}

TEST_F(CuckooFilterTest, LoadChunkOutOfRangeRejected) {
  auto* mr = std::pmr::get_default_resource();
  CuckooFilter src(CuckooFilter::Options{.capacity = 8}, mr);
  src.Insert(CuckooFilter::Hash("x"));

  CFChunk header = CFDumpIterator(src, 0).Next();
  auto init_result = LoadCFHeader(header.data, mr);
  ASSERT_TRUE(init_result.has_value());
  CuckooFilter* loaded = *init_result;
  absl::Cleanup cleanup = [&loaded] { CompactObj::DeleteMR<CuckooFilter>(loaded); };

  // cursor smaller than data.size() cannot correspond to a valid write position.
  EXPECT_EQ(LoadCFChunk(1, "some bytes here", loaded), CFLoadResult::kOutOfRange);

  // cursor pointing past the end of the (single) sub-filter.
  EXPECT_EQ(LoadCFChunk(1000000, "x", loaded), CFLoadResult::kOutOfRange);
}

}  // namespace dfly
