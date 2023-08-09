// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/sorted_map.h"

#include <mimalloc.h>

#include "base/gtest.h"
#include "base/logging.h"
#include "core/mi_memory_resource.h"

extern "C" {
#include "redis/zmalloc.h"
}

using namespace std;

namespace dfly {

using detail::SortedMap;

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

TEST_F(SortedMapTest, Add) {
  SortedMap sm;
  int out_flags;
  double new_score;

  sds ele = sdsnew("a");
  int res = sm.Add(1.0, ele, 0, &out_flags, &new_score);
  EXPECT_EQ(1, res);
  EXPECT_EQ(ZADD_OUT_ADDED, out_flags);
  EXPECT_EQ(1, new_score);

  res = sm.Add(2.0, ele, ZADD_IN_NX, &out_flags, &new_score);
  EXPECT_EQ(1, res);
  EXPECT_EQ(ZADD_OUT_NOP, out_flags);

  res = sm.Add(2.0, ele, ZADD_IN_INCR, &out_flags, &new_score);
  EXPECT_EQ(1, res);
  EXPECT_EQ(ZADD_OUT_UPDATED, out_flags);
  EXPECT_EQ(3, new_score);
}

}  // namespace dfly
