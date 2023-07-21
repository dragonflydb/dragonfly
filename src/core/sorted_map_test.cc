// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <absl/container/btree_map.h>
#include <absl/container/btree_set.h>
#include <mimalloc.h>

#include "base/gtest.h"
#include "base/logging.h"
#include "core/mi_memory_resource.h"

extern "C" {
#include "redis/zmalloc.h"
#include "redis/zset.h"
}

using namespace std;

namespace dfly {

// TODO: to actually add tests covering sorted_map.
class SortedMapTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    // configure redis lib zmalloc which requires mimalloc heap to work.
    auto* tlh = mi_heap_get_backing();
    init_zmalloc_threadlocal(tlh);
  }

  void AddMember(zskiplist* zsl, double score, sds ele) {
    zslInsert(zsl, score, ele);
  }
};

// not a real test, just to see how much memory is used by zskiplist.
TEST_F(SortedMapTest, MemoryUsage) {
  zskiplist* zsl = zslCreate();
  std::vector<sds> sds_vec;
  for (size_t i = 0; i < 10'000; ++i) {
    sds_vec.push_back(sdsnew("f"));
  }
  size_t sz_before = zmalloc_used_memory_tl;
  LOG(INFO) << "zskiplist before: " << sz_before << " bytes";

  for (size_t i = 0; i < sds_vec.size(); ++i) {
    zslInsert(zsl, i, sds_vec[i]);
  }
  LOG(INFO) << "zskiplist took: " << zmalloc_used_memory_tl - sz_before << " bytes";
  zslFree(zsl);

  sds_vec.clear();
  for (size_t i = 0; i < 10'000; ++i) {
    sds_vec.push_back(sdsnew("f"));
  }

  MiMemoryResource mi_alloc(mi_heap_get_backing());
  using AllocType = PMR_NS::polymorphic_allocator<std::pair<double, sds>>;
  AllocType alloc(&mi_alloc);
  absl::btree_set<pair<double, sds>, std::greater<pair<double, sds>>, AllocType> btree(alloc);

  LOG(INFO) << "btree before: " << mi_alloc.used() << " bytes";
  for (size_t i = 0; i < sds_vec.size(); ++i) {
    btree.emplace(i, sds_vec[i]);
  }
  LOG(INFO) << "btree after: " << mi_alloc.used() << " bytes";
}

}  // namespace dfly
