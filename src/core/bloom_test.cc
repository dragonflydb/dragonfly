// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/bloom.h"

#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>

#include "base/gtest.h"

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

TEST_F(BloomTest, DumpSBFInChunks) {
  using namespace absl::little_endian;

  SBF sbf(10, 0.001, 2, PMR_NS::get_default_resource());

  constexpr unsigned kNumElems = 200;
  for (unsigned i = 0; i < kNumElems; ++i) {
    sbf.Add(absl::StrCat("item", i));
  }

  SBFDumpIterator it(sbf, 0);
  std::vector<SBFChunk> chunks;
  while (true) {
    auto nxt = it.Next();
    chunks.push_back(nxt);
    if (nxt.cursor == 0)
      break;
  }

  // First chunk is the header with cursor=1
  ASSERT_GE(chunks.size(), 2u);
  EXPECT_EQ(chunks[0].cursor, 1);

  const auto& header_data = chunks[0].data;
  ASSERT_GE(header_data.size(), 48u);

  const uint8_t* p = reinterpret_cast<const uint8_t*>(header_data.data());

  // kSbfDumpVersion
  EXPECT_EQ(Load32(p + 0), 1u);

  EXPECT_DOUBLE_EQ(std::bit_cast<double>(Load64(p + 4)), sbf.grow_factor());
  EXPECT_DOUBLE_EQ(std::bit_cast<double>(Load64(p + 12)), sbf.fp_probability());
  EXPECT_EQ(Load64(p + 20), sbf.prev_size());
  EXPECT_EQ(Load64(p + 28), sbf.current_size());
  EXPECT_EQ(Load64(p + 36), sbf.max_capacity());

  const uint32_t num_filters = Load32(p + 44);
  EXPECT_EQ(num_filters, sbf.num_filters());

  ASSERT_EQ(header_data.size(), 48u + num_filters * 12u);

  std::vector<uint64_t> filter_sizes;
  size_t filter_start_offset = 48u;

  for (uint32_t i = 0; i < num_filters; ++i, filter_start_offset += 12) {
    auto hash_cnt = Load32(p + filter_start_offset);
    auto size = Load64(p + filter_start_offset + 4);

    EXPECT_EQ(hash_cnt, sbf.hashfunc_cnt(i));
    EXPECT_EQ(size, sbf.data(i).size());

    filter_sizes.emplace_back(size);
  }

  // header is finished, read data
  std::string all_data;
  for (size_t i = 1; i < chunks.size(); ++i) {
    all_data.append(chunks[i].data);
  }

  size_t offset = 0;
  for (uint32_t i = 0; i < num_filters; ++i) {
    const auto filter_size = filter_sizes[i];
    ASSERT_LE(offset + filter_size, all_data.size());
    string_view chunk_data(all_data.data() + offset, filter_size);
    EXPECT_EQ(chunk_data, sbf.data(i));
    offset += filter_size;
  }
  EXPECT_EQ(offset, all_data.size());
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
