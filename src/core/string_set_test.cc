// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/string_set.h"

#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>
#include <gtest/gtest.h>
#include <mimalloc.h>

#include <algorithm>
#include <cstddef>
#include <memory_resource>
#include <random>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

#include "core/compact_object.h"
#include "core/mi_memory_resource.h"
#include "glog/logging.h"
#include "redis/sds.h"

extern "C" {
#include "redis/zmalloc.h"
}

namespace dfly {

using namespace std;
using absl::StrCat;

class DenseSetAllocator : public pmr::memory_resource {
 public:
  bool all_freed() const {
    return alloced_ == 0;
  }

  void* do_allocate(size_t bytes, size_t alignment) override {
    alloced_ += bytes;
    void* p = pmr::new_delete_resource()->allocate(bytes, alignment);
    return p;
  }

  void do_deallocate(void* p, size_t bytes, size_t alignment) override {
    alloced_ -= bytes;
    return pmr::new_delete_resource()->deallocate(p, bytes, alignment);
  }

  bool do_is_equal(const pmr::memory_resource& other) const noexcept override {
    return pmr::new_delete_resource()->is_equal(other);
  }

 private:
  size_t alloced_ = 0;
};

class StringSetTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    auto* tlh = mi_heap_get_backing();
    init_zmalloc_threadlocal(tlh);
  }

  static void TearDownTestSuite() {
  }

  void SetUp() override {
    ss_ = new StringSet(&alloc_);
  }

  void TearDown() override {
    delete ss_;

    // ensure there are no memory leaks after every test
    EXPECT_TRUE(alloc_.all_freed());
    EXPECT_EQ(zmalloc_used_memory_tl, 0);
  }

  StringSet* ss_;
  DenseSetAllocator alloc_;
};

TEST_F(StringSetTest, Basic) {
  EXPECT_TRUE(ss_->Add("foo"sv));
  EXPECT_TRUE(ss_->Add("bar"sv));
  EXPECT_FALSE(ss_->Add("foo"sv));
  EXPECT_FALSE(ss_->Add("bar"sv));
  EXPECT_TRUE(ss_->Contains("foo"sv));
  EXPECT_TRUE(ss_->Contains("bar"sv));
  EXPECT_EQ(2, ss_->Size());
}

TEST_F(StringSetTest, StandardAddErase) {
  EXPECT_TRUE(ss_->Add("@@@@@@@@@@@@@@@@"));
  EXPECT_TRUE(ss_->Add("A@@@@@@@@@@@@@@@"));
  EXPECT_TRUE(ss_->Add("AA@@@@@@@@@@@@@@"));
  EXPECT_TRUE(ss_->Add("AAA@@@@@@@@@@@@@"));
  EXPECT_TRUE(ss_->Add("AAAAAAAAA@@@@@@@"));
  EXPECT_TRUE(ss_->Add("AAAAAAAAAA@@@@@@"));
  EXPECT_TRUE(ss_->Add("AAAAAAAAAAAAAAA@"));
  EXPECT_TRUE(ss_->Add("AAAAAAAAAAAAAAAA"));
  EXPECT_TRUE(ss_->Add("AAAAAAAAAAAAAAAD"));
  EXPECT_TRUE(ss_->Add("BBBBBAAAAAAAAAAA"));
  EXPECT_TRUE(ss_->Add("BBBBBBBBAAAAAAAA"));
  EXPECT_TRUE(ss_->Add("CCCCCBBBBBBBBBBB"));

  // Remove link in the middle of chain
  EXPECT_TRUE(ss_->Erase("BBBBBBBBAAAAAAAA"));
  // Remove start of a chain
  EXPECT_TRUE(ss_->Erase("CCCCCBBBBBBBBBBB"));
  // Remove end of link
  EXPECT_TRUE(ss_->Erase("AAA@@@@@@@@@@@@@"));
  // Remove only item in chain
  EXPECT_TRUE(ss_->Erase("AA@@@@@@@@@@@@@@"));
  EXPECT_TRUE(ss_->Erase("AAAAAAAAA@@@@@@@"));
  EXPECT_TRUE(ss_->Erase("AAAAAAAAAA@@@@@@"));
  EXPECT_TRUE(ss_->Erase("AAAAAAAAAAAAAAA@"));
}

static string random_string(mt19937& rand, unsigned len) {
  const string_view alpanum = "1234567890abcdefghijklmnopqrstuvwxyz";
  string ret;
  ret.reserve(len);

  for (size_t i = 0; i < len; ++i) {
    ret += alpanum[rand() % alpanum.size()];
  }

  return ret;
}

TEST_F(StringSetTest, Resizing) {
  constexpr size_t num_strs = 4096;
  // pseudo random deterministic sequence with known seed should produce
  // the same sequence on all systems
  mt19937 rand(0);

  vector<string> strs;
  while (strs.size() != num_strs) {
    auto str = random_string(rand, 10);
    if (find(strs.begin(), strs.end(), str) != strs.end()) {
      continue;
    }

    strs.push_back(random_string(rand, 10));
  }

  for (size_t i = 0; i < num_strs; ++i) {
    EXPECT_TRUE(ss_->Add(strs[i]));
    EXPECT_EQ(ss_->Size(), i + 1);

    // make sure we haven't lost any items after a grow
    // which happens every power of 2
    if (i != 0 && (ss_->Size() & (ss_->Size() - 1)) == 0) {
      for (size_t j = 0; j < i; ++j) {
        EXPECT_TRUE(ss_->Contains(strs[j]));
      }
    }
  }
}

TEST_F(StringSetTest, SimpleScan) {
  unordered_set<string_view> info = {"foo", "bar"};
  unordered_set<string_view> seen;

  for (auto str : info) {
    EXPECT_TRUE(ss_->Add(str));
  }

  uint32_t cursor = 0;
  do {
    cursor = ss_->Scan(cursor, [&](const sds ptr) {
      sds s = (sds)ptr;
      string_view str{s, sdslen(s)};
      EXPECT_TRUE(info.count(str));
      seen.insert(str);
    });
  } while (cursor != 0);

  EXPECT_TRUE(seen.size() == info.size() && equal(seen.begin(), seen.end(), info.begin()));
}

// Ensure REDIS scan guarantees are met
TEST_F(StringSetTest, ScanGuarantees) {
  unordered_set<string_view> to_be_seen = {"foo", "bar"};
  unordered_set<string_view> not_be_seen = {"AAA", "BBB"};
  unordered_set<string_view> maybe_seen = {"AA@@@@@@@@@@@@@@", "AAA@@@@@@@@@@@@@",
                                           "AAAAAAAAA@@@@@@@", "AAAAAAAAAA@@@@@@"};
  unordered_set<string_view> seen;

  auto scan_callback = [&](const sds ptr) {
    sds s = (sds)ptr;
    string_view str{s, sdslen(s)};
    EXPECT_TRUE(to_be_seen.count(str) || maybe_seen.count(str));
    EXPECT_FALSE(not_be_seen.count(str));
    if (to_be_seen.count(str)) {
      seen.insert(str);
    }
  };

  EXPECT_EQ(ss_->Scan(0, scan_callback), 0);

  for (auto str : not_be_seen) {
    EXPECT_TRUE(ss_->Add(str));
  }

  for (auto str : not_be_seen) {
    EXPECT_TRUE(ss_->Erase(str));
  }

  for (auto str : to_be_seen) {
    EXPECT_TRUE(ss_->Add(str));
  }

  // should reach at least the first item in the set
  uint32_t cursor = ss_->Scan(0, scan_callback);

  for (auto str : maybe_seen) {
    EXPECT_TRUE(ss_->Add(str));
  }

  while (cursor != 0) {
    cursor = ss_->Scan(cursor, scan_callback);
  }

  EXPECT_TRUE(seen.size() == to_be_seen.size());
}

TEST_F(StringSetTest, IntOnly) {
  constexpr size_t num_ints = 8192;
  unordered_set<unsigned int> numbers;
  for (size_t i = 0; i < num_ints; ++i) {
    numbers.insert(i);
    EXPECT_TRUE(ss_->Add(to_string(i)));
  }

  for (size_t i = 0; i < num_ints; ++i) {
    EXPECT_FALSE(ss_->Add(to_string(i)));
  }

  mt19937 generator(0);
  size_t num_remove = generator() % 4096;
  unordered_set<string> removed;

  for (size_t i = 0; i < num_remove; ++i) {
    auto remove_int = generator() % num_ints;
    auto remove = to_string(remove_int);
    if (numbers.count(remove_int)) {
      ASSERT_TRUE(ss_->Contains(remove)) << remove_int;
      EXPECT_TRUE(ss_->Erase(remove));
      numbers.erase(remove_int);
    } else {
      EXPECT_FALSE(ss_->Erase(remove));
    }

    EXPECT_FALSE(ss_->Contains(remove));
    removed.insert(remove);
  }

  size_t expected_seen = 0;
  auto scan_callback = [&](const sds ptr) {
    string str{ptr, sdslen(ptr)};
    EXPECT_FALSE(removed.count(str));

    if (numbers.count(atoi(str.data()))) {
      ++expected_seen;
    }
  };

  uint32_t cursor = 0;
  do {
    cursor = ss_->Scan(cursor, scan_callback);
    // randomly throw in some new numbers
    uint32_t val = generator();
    VLOG(1) << "Val " << val;
    ss_->Add(to_string(val));
  } while (cursor != 0);

  EXPECT_GE(expected_seen + removed.size(), num_ints);
}

TEST_F(StringSetTest, XtremeScanGrow) {
  unordered_set<string> to_see, force_grow, seen;

  mt19937 generator(0);
  while (to_see.size() != 8) {
    to_see.insert(random_string(generator, 10));
  }

  while (force_grow.size() != 8192) {
    string str = random_string(generator, 10);

    if (to_see.count(str)) {
      continue;
    }

    force_grow.insert(random_string(generator, 10));
  }

  for (auto& str : to_see) {
    EXPECT_TRUE(ss_->Add(str));
  }

  auto scan_callback = [&](const sds ptr) {
    sds s = (sds)ptr;
    string_view str{s, sdslen(s)};
    if (to_see.count(string(str))) {
      seen.insert(string(str));
    }
  };

  uint32_t cursor = ss_->Scan(0, scan_callback);

  // force approx 10 grows
  for (auto& s : force_grow) {
    EXPECT_TRUE(ss_->Add(s));
  }

  while (cursor != 0) {
    cursor = ss_->Scan(cursor, scan_callback);
  }

  EXPECT_EQ(seen.size(), to_see.size());
}

TEST_F(StringSetTest, Pop) {
  constexpr size_t num_items = 8;
  unordered_set<string> to_insert;

  mt19937 generator(0);

  while (to_insert.size() != num_items) {
    auto str = random_string(generator, 10);
    if (to_insert.count(str)) {
      continue;
    }

    to_insert.insert(str);
    EXPECT_TRUE(ss_->Add(str));
  }

  while (!ss_->Empty()) {
    size_t size = ss_->Size();
    auto str = ss_->Pop();
    DCHECK(ss_->Size() == to_insert.size() - 1);
    DCHECK(str.has_value());
    DCHECK(to_insert.count(str.value()));
    DCHECK_EQ(ss_->Size(), size - 1);
    to_insert.erase(str.value());
  }

  DCHECK(ss_->Empty());
  DCHECK(to_insert.empty());
}

TEST_F(StringSetTest, Iteration) {
  ss_->Add("foo");
  for (const sds ptr : *ss_) {
    LOG(INFO) << ptr;
  }
  ss_->Clear();
  constexpr size_t num_items = 8192;
  unordered_set<string> to_insert;

  mt19937 generator(0);

  while (to_insert.size() != num_items) {
    auto str = random_string(generator, 10);
    if (to_insert.count(str)) {
      continue;
    }

    to_insert.insert(str);
    EXPECT_TRUE(ss_->Add(str));
  }

  for (const sds ptr : *ss_) {
    string str{ptr, sdslen(ptr)};
    EXPECT_TRUE(to_insert.count(str));
    to_insert.erase(str);
  }

  EXPECT_EQ(to_insert.size(), 0);
}

TEST_F(StringSetTest, Ttl) {
  EXPECT_TRUE(ss_->Add("bla"sv, 1));
  EXPECT_FALSE(ss_->Add("bla"sv, 1));
  ss_->set_time(1);
  EXPECT_TRUE(ss_->Add("bla"sv, 1));
  EXPECT_EQ(1u, ss_->Size());

  for (unsigned i = 0; i < 100; ++i) {
    EXPECT_TRUE(ss_->Add(StrCat("foo", i), 1));
  }
  EXPECT_EQ(101u, ss_->Size());

  ss_->set_time(2);
  for (unsigned i = 0; i < 100; ++i) {
    EXPECT_TRUE(ss_->Add(StrCat("bar", i)));
  }

  for (auto it = ss_->begin(); it != ss_->end(); ++it) {
    ASSERT_TRUE(absl::StartsWith(*it, "bar")) << *it;
    string str = *it;
    VLOG(1) << *it;
  }
}

}  // namespace dfly
