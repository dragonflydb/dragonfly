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

using detail::BlobPtr;
using detail::InternedBlobHandle;

TEST_F(InternedBlobTest, MemoryUsage) {
  const auto* mr = MemoryResource();
  const auto usage_before = mr->used();
  {
    BlobPtr p = detail::MakeBlobPtr("1234567");
    InternedBlobHandle blob{p};
    const auto usage_after = mr->used();
    const auto expected_delta = blob.MemUsed();
    EXPECT_EQ(usage_before + expected_delta, usage_after);
    InternedBlobHandle::Destroy(blob);
  }
  const auto usage_after = mr->used();
  EXPECT_EQ(usage_before, usage_after);
}

void CheckBlob(InternedBlobHandle& blob, std::string_view expected, uint32_t ref_cnt = 1) {
  EXPECT_EQ(blob, expected);
  EXPECT_EQ(blob.Size(), expected.size());
  EXPECT_EQ(blob.RefCount(), ref_cnt);
}

TEST_F(InternedBlobTest, Ctors) {
  {
    BlobPtr p = detail::MakeBlobPtr("");
    InternedBlobHandle blob{p};
    EXPECT_EQ(blob.Size(), 0);
    InternedBlobHandle::Destroy(blob);
  }

  {
    BlobPtr p = detail::MakeBlobPtr("foobar");
    InternedBlobHandle src{p};
    InternedBlobHandle dest{src};
    CheckBlob(dest, "foobar");
    CheckBlob(src, "foobar");
    InternedBlobHandle::Destroy(dest);
  }

  std::string data(100000, 'x');
  BlobPtr p = detail::MakeBlobPtr(data);
  InternedBlobHandle blob{p};
  EXPECT_EQ(blob.Size(), data.length());
  InternedBlobHandle::Destroy(blob);
}

TEST_F(InternedBlobTest, Comparison) {
  BlobPtr p = detail::MakeBlobPtr("foobar");
  InternedBlobHandle blob{p};
  const detail::BlobEq blob_eq;

  EXPECT_TRUE(blob_eq(blob, "foobar"));
  EXPECT_TRUE(blob_eq("foobar", blob));

  InternedBlobHandle second{p};
  second.SetRefCount(2000);

  EXPECT_TRUE(blob_eq(blob, second));
  InternedBlobHandle::Destroy(blob);
}

TEST_F(InternedBlobTest, Accessors) {
  BlobPtr p = detail::MakeBlobPtr("1234567");
  InternedBlobHandle blob{p};
  EXPECT_EQ(blob.Size(), 7);
  EXPECT_STREQ(blob.Data(), "1234567");
  EXPECT_EQ(blob, "1234567"sv);
  InternedBlobHandle::Destroy(blob);
}

TEST_F(InternedBlobTest, RefCounts) {
  BlobPtr p = detail::MakeBlobPtr("1234567");
  InternedBlobHandle blob{p};
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
  InternedBlobHandle::Destroy(blob);
}

TEST_F(InternedBlobTest, Pool) {
  detail::InternedBlobPool pool{};
  BlobPtr p = detail::MakeBlobPtr("foo");
  InternedBlobHandle b1(p);
  pool.emplace(b1);

  // search by string view
  EXPECT_TRUE(pool.contains("foo"));

  // increment the refcount. The blob is still found because the hasher only looks at the string
  b1.IncrRefCount();
  b1.IncrRefCount();
  b1.IncrRefCount();

  EXPECT_TRUE(pool.contains("foo"));
  InternedBlobHandle::Destroy(b1);
}

void BM_Getters(benchmark::State& state) {
  InitTLStatelessAllocMR(MemoryResource());
  BlobPtr p = detail::MakeBlobPtr("foobar");
  InternedBlobHandle b{p};
  for (auto _ : state) {
    benchmark::DoNotOptimize(b.Size());
    benchmark::DoNotOptimize(b.RefCount());
    benchmark::DoNotOptimize(std::string_view{b});
  }
  InternedBlobHandle::Destroy(b);
}

BENCHMARK(BM_Getters);

void BM_Setters(benchmark::State& state) {
  InitTLStatelessAllocMR(MemoryResource());
  BlobPtr p = detail::MakeBlobPtr("foobar");
  InternedBlobHandle b{p};
  for (auto _ : state) {
    b.IncrRefCount();
    benchmark::ClobberMemory();
    b.DecrRefCount();
    benchmark::ClobberMemory();
  }
  InternedBlobHandle::Destroy(b);
}

BENCHMARK(BM_Setters);

void BM_Hash(benchmark::State& state) {
  InitTLStatelessAllocMR(MemoryResource());
  BlobPtr p = detail::MakeBlobPtr("typical_keys");
  InternedBlobHandle b{p};
  detail::BlobHash hasher;
  for (auto _ : state) {
    benchmark::DoNotOptimize(hasher(b));
  }
  InternedBlobHandle::Destroy(b);
}
BENCHMARK(BM_Hash);

void BM_PoolLookup(benchmark::State& state) {
  InitTLStatelessAllocMR(MemoryResource());
  detail::InternedBlobPool pool;
  BlobPtr ptr = detail::MakeBlobPtr("foobar!!!");
  InternedBlobHandle blob(ptr);
  pool.insert(blob);

  for (auto _ : state) {
    benchmark::DoNotOptimize(pool.find("foobar!!!"));
  }

  InternedBlobHandle::Destroy(blob);
}
BENCHMARK(BM_PoolLookup);

void BM_LookupMiss(benchmark::State& state) {
  InitTLStatelessAllocMR(MemoryResource());
  detail::InternedBlobPool pool;
  BlobPtr ptr = detail::MakeBlobPtr("foobar!!!");
  InternedBlobHandle blob(ptr);
  pool.insert(blob);

  for (auto _ : state) {
    benchmark::DoNotOptimize(pool.find("!!!foobar"));
  }

  InternedBlobHandle::Destroy(blob);
}
BENCHMARK(BM_LookupMiss);
