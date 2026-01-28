// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.

#include "base/gtest.h"
#include "core/detail/stateless_allocator.h"
#include "core/json/detail/interned_string.h"
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
  EXPECT_DEATH(blob.DecrRefCount(), "Attempt to decrease zero refcount");
  blob.SetRefCount(std::numeric_limits<uint32_t>::max());
  EXPECT_DEATH(blob.IncrRefCount(), "Attempt to increase max refcount");
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

using detail::InternedString;

namespace {

void StringCheck(const InternedString& s, const char* ptr) {
  std::string_view sv{ptr};

  EXPECT_STREQ(s.data(), ptr);
  EXPECT_STREQ(s.c_str(), ptr);

  EXPECT_EQ(s.size(), sv.size());
  EXPECT_EQ(s.length(), sv.size());

  EXPECT_EQ(std::string_view(s), sv);
  EXPECT_EQ(std::string_view(s.data(), s.size()), sv);
  EXPECT_EQ(std::string_view(s.c_str(), s.size()), sv);
}

}  // namespace

TEST_F(InternedBlobTest, StringPool) {
  const auto& pool = InternedString::GetPoolRef();
  EXPECT_TRUE(pool.empty());
  {
    auto s1 = InternedString{"foobar"};
    StringCheck(s1, "foobar");
    EXPECT_EQ(pool.size(), 1);
    {
      // reuses the existing string
      auto s2 = InternedString{"foobar"};
      StringCheck(s2, "foobar");
      EXPECT_EQ(pool.size(), 1);
    }  // s2 destroyed here, pool size should still be 1
    EXPECT_EQ(pool.size(), 1);
  }  // foobar refcount=0, pool should become empty
  EXPECT_TRUE(pool.empty());

  // populate and drop a large number of strings
  std::vector<InternedString> strings;
  for (auto i = 0; i < 1000; ++i) {
    strings.emplace_back(std::to_string(i));
  }

  EXPECT_EQ(pool.size(), 1000);
  strings.clear();
  EXPECT_TRUE(pool.empty());

  for (auto i = 0; i < 1000; ++i) {
    strings.emplace_back("zyx");
  }
  EXPECT_EQ(pool.size(), 1);
  strings.clear();
  EXPECT_TRUE(pool.empty());
}

TEST_F(InternedBlobTest, StringApi) {
  auto s1 = InternedString{"foobar"};
  EXPECT_EQ(std::string_view{s1}, "foobar"sv);
  StringCheck(s1, "foobar");

  const auto& pool = InternedString::GetPoolRef();
  auto s2 = InternedString{"psi"};
  StringCheck(s2, "psi");

  EXPECT_EQ(pool.size(), 2);

  // swap pointers into the pool
  s1.swap(s2);

  EXPECT_EQ(pool.size(), 2);

  StringCheck(s1, "psi");
  StringCheck(s2, "foobar");

  EXPECT_NE(s1, s2);
  EXPECT_EQ(s1, s1);
  // foobar < psi lexicographically
  EXPECT_LT(s2, s1);
}

TEST_F(InternedBlobTest, StringCtors) {
  // These tests exercise self-move and self-copy behavior. This causes errors on newer GCC when
  // warnings are treated as errors (on CI). We need to version gate this because on older GCC this
  // check is not present.
#if defined(__GNUC__) && !defined(__clang__) && __GNUC__ >= 13
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wself-move"
#endif

  const auto& pool = InternedString::GetPoolRef();
  auto s1 = InternedString{"foobar"};
  EXPECT_EQ(pool.size(), 1);

  // move ctor
  auto to = std::move(s1);
  EXPECT_EQ(pool.size(), 1);

  StringCheck(to, "foobar");
  StringCheck(s1, "");

  // move assign to self
  to = std::move(to);
  StringCheck(to, "foobar");

  // copy ctor
  auto copied = to;
  EXPECT_EQ(pool.size(), 1);

  StringCheck(to, "foobar");
  StringCheck(copied, "foobar");

  // copy ctor to self
  copied = copied;
  StringCheck(copied, "foobar");
  EXPECT_EQ(pool.size(), 1);

  auto* mr = MemoryResource();
  auto before = mr->used();

  std::string_view sv{"......."};
  // ptr and size with some allocator, allocator will be ignored
  auto x = InternedString{sv.data(), sv.size(), std::allocator<char>{}};
  StringCheck(x, ".......");
  EXPECT_EQ(pool.size(), 2);

  EXPECT_GE(mr->used(), before + x.MemUsed());

  // iterator based ctor
  auto k = InternedString{sv.begin(), sv.end()};
  StringCheck(k, ".......");
  EXPECT_EQ(pool.size(), 2);
#if defined(__GNUC__) && !defined(__clang__) && __GNUC__ >= 13
#pragma GCC diagnostic pop
#endif
}
