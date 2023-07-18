// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <absl/container/btree_map.h>
#include <mimalloc.h>

#include "base/gtest.h"
#include "base/logging.h"
#include "core/mi_memory_resource.h"

extern "C" {
#include "redis/zmalloc.h"
#include "redis/zset.h"
}

namespace dfly {

// TODO: to actually add tests covering sorted_map.
class SortedMapTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    // configure redis lib zmalloc which requires mimalloc heap to work.
    auto* tlh = mi_heap_get_backing();
    init_zmalloc_threadlocal(tlh);
  }

  void AddMember(zskiplist* zsl, double score, const char* str) {
    zslInsert(zsl, score, sdsnew(str));
  }
};

// not a real test, just to see how much memory is used by zskiplist.
TEST_F(SortedMapTest, MemoryUsage) {
  zskiplist* zsl = zslCreate();
  LOG(INFO) << "zskiplist before: " << zmalloc_used_memory_tl << " bytes";

  for (int i = 0; i < 10'000; ++i) {
    AddMember(zsl, i, "fooba");
  }
  LOG(INFO) << zmalloc_used_memory_tl << " bytes";
  zslFree(zsl);

  LOG(INFO) << "zskiplist after: " << zmalloc_used_memory_tl << " bytes";
  MiMemoryResource mi_alloc(mi_heap_get_backing());
  using AllocType = PMR_NS::polymorphic_allocator<std::pair<const double, sds>>;
  AllocType alloc(&mi_alloc);
  absl::btree_map<double, sds, std::greater<double>, AllocType> btree(alloc);
  LOG(INFO) << "btree before: " << zmalloc_used_memory_tl + mi_alloc.used() << " bytes";
  for (int i = 0; i < 10000; ++i) {
    btree.emplace(i, sdsnew("fooba"));
  }
  LOG(INFO) << "btree after: " << zmalloc_used_memory_tl + mi_alloc.used() << " bytes";
}

}  // namespace dfly
