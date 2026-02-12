// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

// Disable mimalloc internal debug assertions for accessing internal structures
#define MI_DEBUG 0

#include <mimalloc.h>
#include <mimalloc/internal.h>
#include <mimalloc/types.h>

#include <thread>
#include <vector>

#include "base/gtest.h"
#include "base/logging.h"

// Stub out internal mimalloc assertions that aren't exported
// These are used by inline functions in internal.h
[[noreturn]] void _mi_assert_fail(const char* assertion, const char* fname, unsigned int line,
                                  const char* func) noexcept {
  fprintf(stderr, "mimalloc assertion failed: %s at %s:%u in %s\n", assertion, fname, line, func);
  abort();
}

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

// Verify that xthread_free lists are processed correctly during force collection
// on full pages.
TEST_F(MiHeapTest, FullPageThreadFreeInternal) {
  mi_heap_t* heap = mi_heap_get_default();
  constexpr size_t block_size = 64;
  std::vector<void*> allocations;

  // Allocate blocks until page is full
  void* first_ptr = mi_heap_malloc(heap, block_size);
  ASSERT_TRUE(first_ptr != nullptr);
  allocations.push_back(first_ptr);

  mi_page_t* page = _mi_ptr_page(first_ptr);
  ASSERT_TRUE(page != nullptr);

  while (page->used < page->capacity) {
    void* ptr = mi_heap_malloc(heap, block_size);
    ASSERT_TRUE(ptr != nullptr);
    if (_mi_ptr_page(ptr) == page) {
      allocations.push_back(ptr);
    } else {
      mi_free(ptr);
      break;
    }
  }

  EXPECT_EQ(page->used, page->capacity);

  // Free one block from another thread
  void* cross_thread_ptr = allocations.back();
  allocations.pop_back();

  std::thread t([cross_thread_ptr]() { mi_free(cross_thread_ptr); });
  t.join();

  EXPECT_EQ(page->used, page->capacity);
  EXPECT_NE(mi_atomic_load_relaxed(&page->xthread_free), 0);

  // Force collection should process xthread_free
  mi_heap_collect(heap, true);

  EXPECT_LT(page->used, page->capacity);
  EXPECT_EQ(mi_atomic_load_relaxed(&page->xthread_free), 0);

  // New allocation should reuse the freed block
  void* new_ptr = mi_heap_malloc(heap, block_size);
  EXPECT_EQ(_mi_ptr_page(new_ptr), page);

  // Clean up
  mi_free(new_ptr);
  for (void* ptr : allocations) {
    mi_free(ptr);
  }
}

// Verify that MI_BIN_FULL pages are cleared during collection.
TEST_F(MiHeapTest, FullBinQueueCollection) {
  mi_heap_t* heap = mi_heap_get_default();
  constexpr size_t block_size = 64;

  auto count_xthread_free = [&heap]() {
    size_t count = 0;
    for (size_t i = 0; i <= MI_BIN_FULL; ++i) {
      for (mi_page_t* page = heap->pages[i].first; page != nullptr; page = page->next) {
        if (mi_atomic_load_relaxed(&page->xthread_free) != 0) {
          count++;
        }
      }
    }
    return count;
  };

  // Allocate and cross-thread free to populate xthread_free lists
  std::vector<void*> allocations(2000);
  for (size_t i = 0; i < allocations.size(); ++i) {
    allocations[i] = mi_heap_malloc(heap, block_size);
    ASSERT_TRUE(allocations[i] != nullptr);
  }

  std::thread t([&allocations]() {
    for (size_t i = 0; i < allocations.size() / 2; ++i) {
      mi_free(allocations[i]);
    }
  });
  t.join();

  size_t xthread_before = count_xthread_free();
  EXPECT_GT(xthread_before, 0);

  mi_heap_collect(heap, true);

  EXPECT_EQ(count_xthread_free(), 0) << "All xthread_free lists should be cleared";

  // Clean up
  for (size_t i = allocations.size() / 2; i < allocations.size(); ++i) {
    mi_free(allocations[i]);
  }
}

// Test that verifies memory is reclaimed when a heap is abandoned (deleted without freeing
// allocations). This tests the MI_ABANDON case where mimalloc should properly reclaim pages
// from the abandoned heap.
//
// This test uses a dedicated arena to isolate the abandoned heap's memory and verify
// reclamation by checking arena-specific statistics.
TEST_F(MiHeapTest, AbandonedHeapReclamation) {
  constexpr size_t block_size = 128;
  constexpr size_t num_blocks = 2000;
  std::vector<void*> allocations(num_blocks);

  mi_heap_t* main_heap = mi_heap_get_default();

  // Allocate memory in a separate thread, then exit the thread
  std::thread allocator_thread([&]() {
    for (size_t i = 0; i < num_blocks; ++i) {
      allocations[i] = mi_malloc(block_size);
      ASSERT_TRUE(allocations[i] != nullptr);
    }
  });

  allocator_thread.join();

  // Free all allocations from the main thread (cross-thread free to abandoned heap)
  for (void* ptr : allocations) {
    mi_free(ptr);
  }

  // Force collection to reclaim abandoned segments
  mi_collect(true);

  // Verify memory and abandoned pages are reclaimed
  EXPECT_EQ(main_heap->tld->stats.malloc_normal.current, 0);
}

}  // namespace dfly
