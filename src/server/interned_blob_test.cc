// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.

#include "core/json/detail/interned_blob.h"

#include "base/gtest.h"
#include "core/detail/stateless_allocator.h"
#include "core/mi_memory_resource.h"

using namespace std::literals;
using namespace dfly;

namespace {

MiMemoryResource* MemoryResource() {
  thread_local mi_heap_t* heap = mi_heap_new();
  thread_local MiMemoryResource memory_resource{heap};
  return &memory_resource;
}

}  // namespace

class InternedBlobTest : public testing::Test {
 protected:
  void SetUp() override {
    InitTLStatelessAllocMR(MemoryResource());
  }

  void TearDown() override {
    CleanupStatelessAllocMR();
  }
};

using detail::InternedBlob;

TEST_F(InternedBlobTest, MemoryUsage) {
  const auto* mr = MemoryResource();
  const auto usage_before = mr->used();
  {
    const auto blob = InternedBlob{"1234567"};
    const auto usage_after = mr->used();
    const auto expected_delta = blob.MemUsed();
    EXPECT_EQ(usage_before + expected_delta, usage_after);
  }
  const auto usage_after = mr->used();
  EXPECT_EQ(usage_before, usage_after);
}

void CheckBlob(const InternedBlob& blob, std::string_view expected, uint32_t ref_cnt = 1) {
  EXPECT_EQ(blob.View(), expected);
  EXPECT_EQ(blob.Size(), expected.size());
  EXPECT_EQ(blob.RefCount(), ref_cnt);
}

TEST_F(InternedBlobTest, Ctors) {
  {
    const InternedBlob blob{""};
    EXPECT_EQ(blob.Size(), 0);
  }

  {
    InternedBlob src{"foobar"};
    const InternedBlob dest{std::move(src)};
    CheckBlob(dest, "foobar");
    EXPECT_EQ(src.Data(), nullptr);  // NOLINT
  }

  {
    InternedBlob src{"foobar"};
    const InternedBlob dest = std::move(src);
    CheckBlob(dest, "foobar");
    EXPECT_EQ(src.Data(), nullptr);  // NOLINT
  }

  std::string data(100000, 'x');
  InternedBlob blob{data};
  EXPECT_EQ(blob.Size(), data.length());
}

TEST_F(InternedBlobTest, Comparison) {
  const InternedBlob blob{"foobar"};
  const detail::BlobEq blob_eq;

  EXPECT_TRUE(blob_eq(&blob, "foobar"));
  EXPECT_TRUE(blob_eq("foobar", &blob));

  InternedBlob second{"foobar"};
  second.SetRefCount(2000);

  EXPECT_TRUE(blob_eq(&blob, &second));
}

TEST_F(InternedBlobTest, Accessors) {
  const auto blob = InternedBlob{"1234567"};
  EXPECT_EQ(blob.Size(), 7);
  EXPECT_STREQ(blob.Data(), "1234567");
  EXPECT_EQ(blob.View(), "1234567"sv);
}

TEST_F(InternedBlobTest, RefCounts) {
  auto blob = InternedBlob{"1234567"};
  EXPECT_EQ(blob.RefCount(), 1);
  blob.IncrRefCount();
  blob.IncrRefCount();
  blob.IncrRefCount();
  EXPECT_EQ(blob.RefCount(), 4);
  blob.DecrRefCount();
  blob.DecrRefCount();
  blob.DecrRefCount();
  blob.DecrRefCount();
  EXPECT_EQ(blob.RefCount(), 0);
  EXPECT_DEBUG_DEATH(blob.DecrRefCount(), "Attempt to decrease zero refcount");
  blob.SetRefCount(std::numeric_limits<uint32_t>::max());
  EXPECT_DEBUG_DEATH(blob.IncrRefCount(), "Attempt to increase max refcount");
}

TEST_F(InternedBlobTest, Pool) {
  detail::InternedBlobPool pool{};
  const auto b1 = std::make_unique<InternedBlob>("foo");
  pool.emplace(b1.get());

  // search by string view
  EXPECT_TRUE(pool.contains("foo"));

  // increment the refcount. The blob is still found because the hasher only looks at the string
  b1->IncrRefCount();
  b1->IncrRefCount();
  b1->IncrRefCount();

  EXPECT_TRUE(pool.contains("foo"));
}

void BM_Getters(benchmark::State& state) {
  InitTLStatelessAllocMR(MemoryResource());
  InternedBlob b{"foobar"};
  for (auto _ : state) {
    benchmark::DoNotOptimize(b.Size());
    benchmark::DoNotOptimize(b.RefCount());
    benchmark::DoNotOptimize(b.View());
  }
}

BENCHMARK(BM_Getters);

void BM_Setters(benchmark::State& state) {
  InitTLStatelessAllocMR(MemoryResource());
  InternedBlob b{"foobar"};
  for (auto _ : state) {
    b.IncrRefCount();
    benchmark::ClobberMemory();
    b.DecrRefCount();
    benchmark::ClobberMemory();
  }
}

BENCHMARK(BM_Setters);

void BM_Hash(benchmark::State& state) {
  InitTLStatelessAllocMR(MemoryResource());
  InternedBlob b{"typical_keys"};
  detail::BlobHash hasher;
  for (auto _ : state) {
    benchmark::DoNotOptimize(hasher(&b));
  }
}
BENCHMARK(BM_Hash);

void BM_PoolLookup(benchmark::State& state) {
  InitTLStatelessAllocMR(MemoryResource());
  detail::InternedBlobPool pool;
  auto blob = std::make_unique<InternedBlob>("foobar!!!");
  pool.insert(blob.get());

  for (auto _ : state) {
    benchmark::DoNotOptimize(pool.find("foobar!!!"));
  }
}
BENCHMARK(BM_PoolLookup);

void BM_LookupMiss(benchmark::State& state) {
  InitTLStatelessAllocMR(MemoryResource());
  detail::InternedBlobPool pool;
  auto blob = std::make_unique<InternedBlob>("foobar!!!");
  pool.insert(blob.get());

  for (auto _ : state) {
    benchmark::DoNotOptimize(pool.find("!!!foobar"));
  }
}
BENCHMARK(BM_LookupMiss);
