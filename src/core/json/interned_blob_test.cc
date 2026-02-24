// Copyright 2026, DragonflyDB authors.  All rights reserved.
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
  second.IncrRefCount();

  EXPECT_TRUE(blob_eq(blob, second));
  InternedBlobHandle::Destroy(blob);
}

TEST_F(InternedBlobTest, RefCounts) {
  auto blob = InternedBlobHandle::Create("1234567");
  EXPECT_EQ(blob.RefCount(), 1);
  blob.DecrRefCount();
  EXPECT_DEBUG_DEATH(blob.DecrRefCount(), "Attempt to decrease zero refcount");
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
  size_t hits = GetInternedStringStats().hits;
  size_t misses = GetInternedStringStats().misses;
  const auto& pool = InternedString::GetPoolRef();
  EXPECT_TRUE(pool.empty());
  {
    const InternedString s1{"foobar"};
    StringCheck(s1, "foobar");
    EXPECT_EQ(pool.size(), 1);
    misses += 1;
    EXPECT_EQ(GetInternedStringStats().misses, misses);
    EXPECT_EQ(GetInternedStringStats().pool_entries, 1);
    {
      const InternedString s2{"foobar"};
      StringCheck(s2, "foobar");
      EXPECT_EQ(pool.size(), 1);
      EXPECT_EQ(GetInternedStringStats().misses, misses);
      EXPECT_EQ(GetInternedStringStats().pool_entries, 1);
      hits += 1;
      EXPECT_EQ(GetInternedStringStats().hits, hits);
    }
    EXPECT_EQ(pool.size(), 1);
  }
  EXPECT_TRUE(pool.empty());
  EXPECT_EQ(GetInternedStringStats().misses, misses);
  EXPECT_EQ(GetInternedStringStats().pool_entries, 0);
  EXPECT_EQ(GetInternedStringStats().pool_bytes, 0);
  EXPECT_EQ(GetInternedStringStats().hits, hits);

  std::vector<InternedString> strings;
  for (auto i = 0; i < 1000; ++i) {
    strings.emplace_back(std::to_string(i));
  }

  EXPECT_EQ(pool.size(), 1000);
  EXPECT_EQ(GetInternedStringStats().pool_entries, 1000);
  misses += 1000;
  EXPECT_EQ(GetInternedStringStats().misses, misses);
  strings.clear();
  EXPECT_TRUE(pool.empty());
  EXPECT_EQ(GetInternedStringStats().pool_entries, 0);
  EXPECT_EQ(GetInternedStringStats().pool_bytes, 0);

  for (auto i = 0; i < 1000; ++i) {
    strings.emplace_back("zyx");
  }
  EXPECT_EQ(pool.size(), 1);
  EXPECT_EQ(GetInternedStringStats().pool_entries, 1);
  hits += 999;
  EXPECT_EQ(GetInternedStringStats().hits, hits);
  strings.clear();
  EXPECT_TRUE(pool.empty());

  InternedString empty;
  EXPECT_TRUE(pool.empty());
}

TEST_F(InternedBlobTest, StringApi) {
  InternedString s1{"foobar"};
  EXPECT_EQ(std::string_view{s1}, "foobar"sv);
  StringCheck(s1, "foobar");

  const auto& pool = InternedString::GetPoolRef();
  InternedString s2{"psi"};
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
  const auto& pool = InternedString::GetPoolRef();
  InternedString s1{"foobar"};
  EXPECT_EQ(pool.size(), 1);

  // move ctor
  auto to = std::move(s1);
  EXPECT_EQ(pool.size(), 1);

  StringCheck(to, "foobar");
  StringCheck(s1, "");

  // These tests exercise self-move and self-copy behavior. This causes errors on newer GCC when
  // warnings are treated as errors (on CI). We need to version gate this because on older GCC this
  // check is not present.
#if defined(__GNUC__) && !defined(__clang__) && __GNUC__ >= 13
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wself-move"
#endif
  to = std::move(to);
  StringCheck(to, "foobar");

  auto copied = to;
  EXPECT_EQ(pool.size(), 1);

  StringCheck(to, "foobar");
  StringCheck(copied, "foobar");

  copied = copied;
#if defined(__GNUC__) && !defined(__clang__) && __GNUC__ >= 13
#pragma GCC diagnostic pop
#endif
  StringCheck(copied, "foobar");
  EXPECT_EQ(pool.size(), 1);

  const auto* mr = MemoryResource();
  const auto before = mr->used();

  std::string_view sv{"......."};
  // ptr and size with some allocator, allocator will be ignored
  InternedString x{sv.data(), sv.size(), std::allocator<char>{}};
  StringCheck(x, ".......");
  EXPECT_EQ(pool.size(), 2);

  EXPECT_GE(mr->used(), before + x.MemUsed());

  InternedString k{sv.begin(), sv.end()};
  StringCheck(k, ".......");
  EXPECT_EQ(pool.size(), 2);
}

TEST_F(InternedBlobTest, PoolShrink) {
  InternedString::ResetPool();
  std::vector<InternedString> v;
  const auto& ref = InternedString::GetPoolRef();
  for (const auto i : std::views::iota(0, 1000))
    v.emplace_back(std::to_string(i));

  std::vector<size_t> caps;

  constexpr auto jitter = std::views::iota(0, 6);

  while (!v.empty()) {
    constexpr auto step = 20;
    const auto from = v.end() - std::min<size_t>(step, v.size());
    v.erase(from, v.end());
    // Interleaving inserts right after a possible resize, to ensure we don't have to increase
    // capacity right after a shrink. The caps vector should remain monotonically decreasing.
    for (const auto j : jitter)
      v.emplace_back(std::to_string(10000 + j));
    caps.push_back(ref.capacity());
    for (size_t i = 0; i < jitter.size(); ++i)
      v.pop_back();
  }

  EXPECT_EQ(ref.load_factor(), 0);
  EXPECT_TRUE(std::ranges::is_sorted(caps, std::ranges::greater{}));

  // Check that capacity changes very infrequently
  size_t cap_trans = 0;
  for (size_t i = 1; i < caps.size(); ++i) {
    if (caps[i] != caps[i - 1])
      ++cap_trans;
  }
  EXPECT_LT(cap_trans, caps.size() / 2);
}
