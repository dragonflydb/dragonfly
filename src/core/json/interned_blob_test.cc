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
  InternedBlobHandle blob = InternedBlobHandle::Create("1234567");
  const auto usage_after = mr->used();
  const auto expected_delta = blob.MemUsed();
  EXPECT_EQ(usage_before + expected_delta, usage_after);
  InternedBlobHandle::Destroy(blob);
  EXPECT_EQ(usage_before, mr->used());
}

void CheckBlob(InternedBlobHandle& blob, std::string_view expected, uint32_t ref_cnt = 1) {
  EXPECT_EQ(blob, expected);
  EXPECT_EQ(blob.Size(), expected.size());
  EXPECT_EQ(blob.RefCount(), ref_cnt);
}

TEST_F(InternedBlobTest, Ctors) {
  auto blob = InternedBlobHandle::Create("");
  EXPECT_EQ(blob.Size(), 0);
  EXPECT_FALSE(blob);
  InternedBlobHandle::Destroy(blob);

  InternedBlobHandle src = InternedBlobHandle::Create("foobar");
  InternedBlobHandle dest{src};
  CheckBlob(dest, "foobar");
  CheckBlob(src, "foobar");
  InternedBlobHandle::Destroy(dest);
}

TEST_F(InternedBlobTest, Comparison) {
  auto blob = InternedBlobHandle::Create("foobar");
  constexpr detail::BlobEq blob_eq;

  EXPECT_TRUE(blob_eq(blob, "foobar"));
  EXPECT_TRUE(blob_eq("foobar", blob));

  InternedBlobHandle second = blob;
  second.SetRefCount(2000);

  EXPECT_TRUE(blob_eq(blob, second));
  InternedBlobHandle::Destroy(blob);
}

TEST_F(InternedBlobTest, RefCounts) {
  auto blob = InternedBlobHandle::Create("1234567");
  EXPECT_EQ(blob.RefCount(), 1);
  blob.SetRefCount(0);
  EXPECT_DEBUG_DEATH(blob.DecrRefCount(), "Attempt to decrease zero refcount");
  blob.SetRefCount(std::numeric_limits<uint32_t>::max());
  EXPECT_DEBUG_DEATH(blob.IncrRefCount(), "Attempt to increase max refcount");
  InternedBlobHandle::Destroy(blob);
}

TEST_F(InternedBlobTest, Pool) {
  detail::InternedBlobPool pool{};
  InternedBlobHandle b1 = InternedBlobHandle::Create("foo");
  pool.emplace(b1);

  // search by string view
  EXPECT_TRUE(pool.contains("foo"));

  // increment the refcount. The blob is still found because the hasher only looks at the string
  b1.IncrRefCount();
  EXPECT_TRUE(pool.contains("foo"));
  InternedBlobHandle::Destroy(b1);
}
