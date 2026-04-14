// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/bloom.h"

#include <absl/base/internal/endian.h>
#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>

#include "base/gtest.h"
#include "core/compact_object.h"

namespace dfly {

using namespace std;

class BloomTest : public ::testing::Test {
 protected:
  BloomTest() {
    bloom_.Init(1000, 0.001, PMR_NS::get_default_resource());
  }

  ~BloomTest() {
    bloom_.Destroy(PMR_NS::get_default_resource());
  }

  Bloom bloom_;
};

TEST_F(BloomTest, Basic) {
  EXPECT_FALSE(bloom_.Exists(string_view{}));
  EXPECT_TRUE(bloom_.Add(string_view{}));
  EXPECT_TRUE(bloom_.Exists(string_view{}));
  EXPECT_FALSE(bloom_.Add(string_view{}));

  vector<string> values;
  for (unsigned i = 0; i < 100; ++i) {
    values.push_back(absl::StrCat("val", i));
  }

  for (const auto& val : values) {
    EXPECT_FALSE(bloom_.Exists(val));
    EXPECT_TRUE(bloom_.Add(val));
    EXPECT_TRUE(bloom_.Exists(val));
    EXPECT_FALSE(bloom_.Add(val));
  }
}

TEST_F(BloomTest, ErrorBound) {
  size_t max_capacity = bloom_.Capacity(0.001);
  for (unsigned i = 0; i < max_capacity; ++i) {
    ASSERT_FALSE(bloom_.Exists(absl::StrCat("item", i)));
  }

  unsigned collisions = 0;
  for (unsigned i = 0; i < max_capacity; ++i) {
    if (!bloom_.Add(absl::StrCat("item", i))) {
      ++collisions;
    }
  }

  EXPECT_EQ(collisions, 0) << max_capacity;
}

TEST_F(BloomTest, Extreme) {
  Bloom b2;

  // Init with unreasonable large error probability.
  b2.Init(10, 0.999, PMR_NS::get_default_resource());

  EXPECT_EQ(512, b2.bitlen());  // minimal bit length, even though requested smaller capacity.
  EXPECT_LT(b2.Capacity(0.999), 512);  // make sure our element capacity is smaller.
  b2.Destroy(PMR_NS::get_default_resource());
}

TEST_F(BloomTest, SBF) {
  SBF sbf(10, 0.001, 2, PMR_NS::get_default_resource());

  unsigned collisions = 0;
  constexpr unsigned kNumElems = 2000000;
  for (unsigned i = 0; i < kNumElems; ++i) {
    if (!sbf.Add(absl::StrCat("item", i))) {
      ++collisions;
    }
  }

  // TODO: to revisit the math for deriving number of hash functions for each filter
  // according the the SBF paper.
  EXPECT_LE(collisions, kNumElems * 0.008);
}

TEST_F(BloomTest, DumpAndLoadRoundTrip) {
  auto* mr = PMR_NS::get_default_resource();
  SBF src(10, 0.001, 2, mr);

  constexpr unsigned kNumElems = 200;
  for (unsigned i = 0; i < kNumElems; ++i)
    src.Add(absl::StrCat("item", i));

  SBFDumpIterator it(src, 0);
  std::vector<SBFChunk> chunks;
  for (auto c = it.Next(); c.cursor; c = it.Next())
    chunks.push_back(std::move(c));

  // at least header + 1 filter
  ASSERT_GE(chunks.size(), 2u);
  auto cit = chunks.cbegin();

  // first chunk cursor guaranteed to be 1
  EXPECT_EQ(cit->cursor, 1);

  auto init_result = LoadSBFHeader(cit->data, mr);
  ASSERT_TRUE(init_result.has_value());

  SBF* loaded = *init_result;
  absl::Cleanup cleanup = [&loaded] { CompactObj::DeleteMR<SBF>(loaded); };

  // load all the chunks
  ++cit;
  for (; cit != chunks.cend(); ++cit)
    ASSERT_EQ(LoadSBFChunk(loaded, cit->cursor, cit->data), SBFLoadResult::kOk);

  EXPECT_EQ(loaded->grow_factor(), src.grow_factor());
  EXPECT_DOUBLE_EQ(loaded->fp_probability(), src.fp_probability());
  EXPECT_EQ(loaded->max_capacity(), src.max_capacity());
  EXPECT_EQ(loaded->current_size(), src.current_size());
  EXPECT_EQ(loaded->prev_size(), src.prev_size());
  EXPECT_EQ(loaded->num_filters(), src.num_filters());

  for (uint32_t i = 0; i < src.num_filters(); ++i) {
    EXPECT_EQ(loaded->data(i), src.data(i));
    EXPECT_EQ(loaded->hashfunc_cnt(i), src.hashfunc_cnt(i));
  }

  for (unsigned i = 0; i < kNumElems; ++i)
    EXPECT_TRUE(loaded->Exists(absl::StrCat("item", i))) << "Missing item " << i;
}

TEST_F(BloomTest, DumpPastEndCursorReturnsEof) {
  SBF sbf(10, 0.001, 2, PMR_NS::get_default_resource());
  SBFDumpIterator it(sbf, 999999);
  auto [cursor, data] = it.Next();
  EXPECT_EQ(cursor, 0);
  EXPECT_TRUE(data.empty());
}

TEST_F(BloomTest, DumpWithGrowthDuringIteration) {
  auto* mr = PMR_NS::get_default_resource();
  SBF sbf(10, 0.001, 2, mr);

  constexpr unsigned initial = 100;
  for (unsigned i = 0; i < initial; ++i)
    sbf.Add(absl::StrCat("item", i));

  const uint32_t initial_filters = sbf.num_filters();

  // Dump header + first data chunk
  std::vector<SBFChunk> chunks;

  chunks.push_back(SBFDumpIterator(sbf, 0).Next());
  ASSERT_EQ(chunks[0].cursor, 1);

  chunks.push_back(SBFDumpIterator(sbf, 1).Next());
  int64_t cursor = chunks.back().cursor;

  // Trigger growth by adding many more items
  constexpr unsigned total = 5000;
  for (unsigned i = initial; i < total; ++i)
    sbf.Add(absl::StrCat("item", i));
  ASSERT_GT(sbf.num_filters(), initial_filters) << "SBF should have grown";

  // Continue dumping from old cursor, new filters should be picked up
  while (cursor != 0) {
    auto chunk = SBFDumpIterator(sbf, cursor).Next();
    cursor = chunk.cursor;
    if (cursor != 0)
      chunks.push_back(std::move(chunk));
  }

  auto init_result = LoadSBFHeader(chunks[0].data, mr);
  ASSERT_TRUE(init_result.has_value());

  SBF* loaded = *init_result;
  absl::Cleanup cleanup = [&loaded] { CompactObj::DeleteMR<SBF>(loaded); };

  for (size_t i = 1; i < chunks.size(); ++i)
    ASSERT_EQ(LoadSBFChunk(loaded, chunks[i].cursor, chunks[i].data), SBFLoadResult::kOk);

  // Loaded SBF should have all filters including the grown ones
  EXPECT_EQ(loaded->num_filters(), sbf.num_filters());

  for (unsigned i = 0; i < total; ++i)
    EXPECT_TRUE(loaded->Exists(absl::StrCat("item", i))) << "Missing item " << i;
}

static void BM_BloomExist(benchmark::State& state) {
  constexpr size_t kCapacity = 1U << 22;
  Bloom bloom;
  bloom.Init(kCapacity, 0.001, PMR_NS::get_default_resource());
  for (size_t i = 0; i < kCapacity * 0.8; ++i) {
    bloom.Add(absl::StrCat("val", i));
  }
  unsigned i = 0;
  char buf[32];
  memset(buf, 'x', sizeof(buf));
  string_view sv{buf, sizeof(buf)};
  while (state.KeepRunning()) {
    absl::numbers_internal::FastIntToBuffer(i, buf);
    bloom.Exists(sv);
  }
  bloom.Destroy(PMR_NS::get_default_resource());
}
BENCHMARK(BM_BloomExist);

}  // namespace dfly
