// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/bloom.h"

#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>
#include <mimalloc.h>

#include "base/gtest.h"

namespace dfly {

using namespace std;

class BloomTest : public ::testing::Test {
 protected:
  BloomTest() : bloom_(1000, 0.001, mi_heap_get_default()) {
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

static void BM_BloomExist(benchmark::State& state) {
  constexpr size_t kCapacity = 1U << 22;
  Bloom bloom(kCapacity, 0.001, mi_heap_get_default());
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
}
BENCHMARK(BM_BloomExist);

}  // namespace dfly
