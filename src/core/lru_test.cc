// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/lru.h"

#include <absl/strings/str_cat.h>

#include "base/gtest.h"
#include "base/logging.h"
#include "core/compact_object.h"
#include "core/mi_memory_resource.h"

using namespace std;

namespace dfly {

class StringLruTest : public ::testing::Test {
 protected:
  StringLruTest() : mr_(mi_heap_get_backing()), cache_(kSize, &mr_) {
  }

  const size_t kSize = 4;
  MiMemoryResource mr_;
  Lru<std::string> cache_;
};

TEST_F(StringLruTest, PutAndGet) {
  cache_.Put("a");
  ASSERT_EQ("a", cache_.GetHead());
  ASSERT_EQ("a", cache_.GetPrev("a"));
  ASSERT_EQ("a", cache_.GetTail());
  cache_.Put("a");
  ASSERT_EQ("a", cache_.GetHead());
  ASSERT_EQ("a", cache_.GetTail());
  cache_.Put("b");
  ASSERT_EQ("b", cache_.GetHead());
  ASSERT_EQ("a", cache_.GetTail());
  cache_.Put("c");
  ASSERT_EQ("c", cache_.GetHead());
  ASSERT_EQ("a", cache_.GetTail());
  cache_.Put("d");
  ASSERT_EQ("d", cache_.GetHead());
  ASSERT_EQ("a", cache_.GetTail());
  cache_.Put("a");
  ASSERT_EQ("a", cache_.GetHead());
  ASSERT_EQ("b", cache_.GetTail());
  cache_.Put("e");
  ASSERT_EQ("e", cache_.GetHead());
  ASSERT_EQ("b", cache_.GetTail());
  cache_.Put("f");
  ASSERT_EQ("f", cache_.GetHead());
  ASSERT_EQ("b", cache_.GetTail());
}

TEST_F(StringLruTest, PutAndPutTail) {
  cache_.Put("a");
  cache_.Put("a");  // a
  cache_.Put("b");  // b -> a
  cache_.Put("c");  // c -> b -> a
  cache_.Put("d");  // d-> c -> b -> a
  ASSERT_EQ("a", cache_.GetTail());
  cache_.Put("a");  // a -> d -> c -> b
  ASSERT_EQ("b", cache_.GetTail());
  ASSERT_EQ("c", cache_.GetPrev("b"));
  ASSERT_EQ("d", cache_.GetPrev("c"));
  ASSERT_EQ("b", cache_.GetPrev("a"));
  cache_.Put("d", Position::kTail);  // a -> c -> b -> d
  ASSERT_EQ("d", cache_.GetTail());
  ASSERT_EQ("b", cache_.GetPrev("d"));
  ASSERT_EQ("c", cache_.GetPrev("b"));
  ASSERT_EQ("a", cache_.GetPrev("c"));
  ASSERT_EQ("d", cache_.GetPrev("a"));
  cache_.Put("a");  // a -> c -> b -> d
  ASSERT_EQ("d", cache_.GetTail());
  cache_.Put("e", Position::kTail);  // a -> c -> b -> d -> e
  ASSERT_EQ("e", cache_.GetTail());
  ASSERT_EQ("d", cache_.GetPrev("e"));
  ASSERT_EQ("b", cache_.GetPrev("d"));
  ASSERT_EQ("a", cache_.GetPrev("c"));
  ASSERT_EQ("e", cache_.GetPrev("a"));
  cache_.Put("e", Position::kTail);  // a -> c -> b -> d -> e
  ASSERT_EQ("e", cache_.GetTail());
  ASSERT_EQ("d", cache_.GetPrev("e"));
  ASSERT_EQ("b", cache_.GetPrev("d"));
  ASSERT_EQ("a", cache_.GetPrev("c"));
  ASSERT_EQ("e", cache_.GetPrev("a"));
}

TEST_F(StringLruTest, BumpTest) {
  cache_.Put("a");
  cache_.Put("b");
  cache_.Put("c");
  cache_.Put("d");
  ASSERT_EQ("a", cache_.GetTail());
  cache_.Put("c");
  ASSERT_EQ("a", cache_.GetTail());
  ASSERT_EQ("d", cache_.GetPrev("b"));
  ASSERT_EQ("c", cache_.GetPrev("d"));
}

TEST_F(StringLruTest, DifferentOrder) {
  for (uint32_t i = 0; i < kSize * 2; ++i) {
    cache_.Put(absl::StrCat(i));
  }
  ASSERT_EQ("0", cache_.GetTail());

  for (uint32_t i = kSize; i > 0; --i) {
    cache_.Put(absl::StrCat(i));
  }
  ASSERT_EQ("0", cache_.GetTail());
  cache_.Put("0");
  ASSERT_EQ("5", cache_.GetTail());
}

TEST_F(StringLruTest, Delete) {
  cache_.Put("a");  // a
  cache_.Put("b");  // b -> a
  cache_.Put("c");  // c -> b -> a
  cache_.Put("d");  // d-> c -> b -> a
  cache_.Put("e");  // e -> d-> c -> b -> a
  ASSERT_EQ("e", cache_.GetHead());
  ASSERT_TRUE(cache_.Remove("e"));  // d-> c -> b -> a
  ASSERT_EQ("d", cache_.GetHead());
  ASSERT_EQ("a", cache_.GetTail());
  ASSERT_EQ("b", cache_.GetPrev("a"));
  ASSERT_EQ("c", cache_.GetPrev("b"));
  ASSERT_EQ("d", cache_.GetPrev("c"));
  ASSERT_EQ("a", cache_.GetPrev("d"));
  ASSERT_FALSE(cache_.Remove("e"));  // d-> c -> b -> a

  ASSERT_TRUE(cache_.Remove("c"));  // d -> b -> a
  ASSERT_EQ("d", cache_.GetHead());
  ASSERT_EQ("a", cache_.GetTail());
  ASSERT_EQ("b", cache_.GetPrev("a"));
  ASSERT_EQ("d", cache_.GetPrev("b"));
  ASSERT_EQ("a", cache_.GetPrev("d"));
  cache_.Put("c");  // c -> d -> b -> a
  ASSERT_EQ("c", cache_.GetHead());
  ASSERT_EQ("a", cache_.GetTail());
  ASSERT_EQ("b", cache_.GetPrev("a"));
  ASSERT_EQ("d", cache_.GetPrev("b"));
  ASSERT_EQ("c", cache_.GetPrev("d"));
  ASSERT_EQ("a", cache_.GetPrev("c"));
  ASSERT_TRUE(cache_.Remove("a"));  // c -> d -> b
  ASSERT_EQ("b", cache_.GetTail());
  ASSERT_EQ("d", cache_.GetPrev("b"));
  ASSERT_EQ("c", cache_.GetPrev("d"));
  ASSERT_EQ("b", cache_.GetPrev("c"));
}

class COVLruTest : public ::testing::Test {
 protected:
  COVLruTest() : mr_(mi_heap_get_backing()), cache_(kSize, &mr_) {
  }

  const size_t kSize = 100;
  MiMemoryResource mr_;
  Lru<CompactObjectView> cache_;
};

TEST_F(COVLruTest, MemoryUsagePrint) {
  size_t before = mr_.used();
  std::array<CompactObj, 100> obj_arr;
  for (int i = 0; i < 100; ++i) {
    obj_arr[i].SetString(absl::StrCat(i));
    cache_.Put(obj_arr[i]);
  }

  size_t after = mr_.used();
  LOG(INFO) << "CompactObjectView lru 100 items memory : " << absl::StrCat(after - before)
            << " bytes";
}

}  // namespace dfly
