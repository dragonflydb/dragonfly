// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <mimalloc.h>
#include <mimalloc/types.h>

#include <thread>

#include "base/gtest.h"
#include "base/logging.h"

namespace dfly {

class MiHeapTest : public ::testing::Test {
 protected:
  MiHeapTest() {
  }
};

TEST_F(MiHeapTest, Basic) {
  mi_heap_t* heap = mi_heap_get_default();
  void* ptr = mi_heap_malloc_aligned(heap, 1024 /* size*/, 64 /* alignment*/);
  ASSERT_TRUE(ptr != nullptr);

  EXPECT_EQ(heap->tld->stats.malloc_normal.current, 1024);
  EXPECT_EQ(heap->tld->stats.malloc_huge.current, 0);

  void* ptr2 = mi_heap_malloc_aligned(heap, 1024 * 1024 /* size*/, 64 /* alignment*/);

  EXPECT_EQ(heap->tld->stats.malloc_normal.current, 1024);
  EXPECT_GE(heap->tld->stats.malloc_huge.current, 1024 * 1024);

  mi_free(ptr);

  EXPECT_EQ(heap->tld->stats.malloc_normal.current, 0);
  EXPECT_GE(heap->tld->stats.malloc_huge.current, 1024 * 1024);

  mi_free(ptr2);
  EXPECT_EQ(heap->tld->stats.malloc_huge.current, 0);
}

TEST_F(MiHeapTest, Threaded) {
  mi_heap_t* heap = mi_heap_get_default();

  void* ptr = mi_heap_malloc_aligned(heap, 1024 /* size*/, 64 /* alignment*/);
  ASSERT_TRUE(ptr != nullptr);

  // adding ptr to heap->thread_delayed_free
  std::thread t2([ptr]() {
    mi_free(ptr);
    // thread local stats are updated.
    EXPECT_EQ(mi_heap_get_default()->tld->stats.malloc_normal.current, -1024);
  });

  t2.join();
  EXPECT_EQ(heap->tld->stats.malloc_normal.current, 1024);
  EXPECT_EQ(heap->generic_collect_count, 0);

  // Force many mallocs to trigger delayed blocks collection.
  for (unsigned i = 0; i < 200; ++i) {
    ptr = mi_malloc(16 * i);
    mi_free(ptr);
  }

  // delayed collections was triggered
  EXPECT_GE(heap->generic_collect_count, 1);

  // mi_malloc does not track malloc back sizes back to the original heap threads.
  EXPECT_EQ(heap->tld->stats.malloc_normal.current, 1024);
}

}  // namespace dfly
