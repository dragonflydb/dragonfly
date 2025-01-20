// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/rax_tree.h"

#include <absl/container/btree_set.h>
#include <absl/strings/str_cat.h>
#include <gtest/gtest.h>
#include <mimalloc.h>

#include <algorithm>
#include <memory_resource>

#include "base/gtest.h"
#include "base/iterator.h"
#include "base/logging.h"

extern "C" {
#include "redis/zmalloc.h"
}

namespace dfly::search {

using namespace std;

struct RaxTreeTest : public ::testing::Test {
  static void SetUpTestSuite() {
    auto* tlh = mi_heap_get_backing();
    init_zmalloc_threadlocal(tlh);
  }
};

TEST_F(RaxTreeTest, EmplaceAndIterate) {
  RaxTreeMap<std::string> map(pmr::get_default_resource());

  vector<pair<string, string>> elements(90);
  for (int i = 10; i < 100; i++)
    elements[i - 10] = make_pair(absl::StrCat("key-", i), absl::StrCat("value-", i));

  for (auto& [key, value] : elements) {
    auto [it, inserted] = map.try_emplace(key, value);
    EXPECT_TRUE(inserted);
    EXPECT_EQ(it->first, key);
    EXPECT_EQ(it->second, value);
  }

  size_t i = 0;
  for (auto [key, value] : map) {
    EXPECT_EQ(elements[i].first, key);
    EXPECT_EQ(elements[i].second, value);
    i++;
  }
}

TEST_F(RaxTreeTest, LowerBound) {
  RaxTreeMap<int> map(pmr::get_default_resource());
  vector<string> keys;

  for (unsigned i = 0; i < 5; i++) {
    for (unsigned j = 0; j < 5; j++) {
      keys.emplace_back(absl::StrCat("key-", string(1, 'a' + i), "-", j));
      map.try_emplace(keys.back(), 0);
    }
  }

  auto it1 = map.lower_bound("key-c-3");
  auto it2 = lower_bound(keys.begin(), keys.end(), "key-c-3");

  while (it1 != map.end()) {
    EXPECT_EQ((*it1).first, *it2);
    ++it1;
    ++it2;
  }

  EXPECT_TRUE(it1 == map.end());
  EXPECT_TRUE(it2 == keys.end());

  // Test lower bound empty string
  vector<string> keys2;
  for (auto it = map.lower_bound(string_view{}); it != map.end(); ++it)
    keys2.emplace_back((*it).first);
  EXPECT_EQ(keys, keys2);
}

TEST_F(RaxTreeTest, Find) {
  RaxTreeMap<int> map(pmr::get_default_resource());
  for (unsigned i = 100; i < 999; i += 2)
    map.try_emplace(absl::StrCat("value-", i), i);

  auto it = map.begin();
  for (unsigned i = 100; i < 999; i++) {
    auto fit = map.find(absl::StrCat("value-", i));
    if (i % 2 == 0) {
      EXPECT_TRUE(fit == it);
      EXPECT_EQ(fit->second, i);
      ++it;
    } else {
      EXPECT_TRUE(fit == map.end());
    }
  }

  // Test find with empty string
  EXPECT_TRUE(map.find(string_view{}) == map.end());
}

/* Run with mimalloc to make sure there is no double free */
TEST_F(RaxTreeTest, Iterate) {
  const char* kKeys[] = {
      "aaaaaaaaaaaaaaaaaaaa",
      "bbbbbbbbbbbbbbbbbbbbbb"
      "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
      "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
      "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
  };

  RaxTreeMap<int> map(pmr::get_default_resource());
  for (const char* key : kKeys) {
    map.try_emplace(key, 2);
  }

  for (auto it = map.begin(); it != map.end(); ++it) {
    EXPECT_EQ((*it).second, 2);
  }

  for (auto it = map.begin(); it != map.end(); ++it) {
    EXPECT_EQ((*it).second, 2);
  }
}

}  // namespace dfly::search
