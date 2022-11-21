// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <absl/strings/str_cat.h>
#include <mimalloc.h>

#include <list>

#include "base/gtest.h"
#include "base/logging.h"
#include "core/compact_object.h"
#include "core/mi_memory_resource.h"
extern "C" {
#include "redis/redis_aux.h"
#include "redis/zmalloc.h"  // for non-string objects.
}
#include <mimalloc-types.h>

// We must use this file - as using compact_object_test cannot be used
// due to issue with the standard memory_resource being called to release
// to make way to the thread local memory manager and this cause a crash
// due to invalid memory access.
namespace dfly {

// This code will setup local thread mi malloc allocator
// The same as we have for the shards
struct ThreadLocalResoure {
  mi_heap_t* tlh = nullptr;

  ThreadLocalResoure() {
    tlh = mi_heap_new();
    init_zmalloc_threadlocal(tlh);
  }

  static thread_local ThreadLocalResoure resource_;
};

thread_local ThreadLocalResoure ThreadLocalResoure::resource_;

struct DefragAllocator {
  MiMemoryResource mi_allocator;
  static thread_local DefragAllocator* self_;

  DefragAllocator(mi_heap_t* heap) : mi_allocator(heap) {
  }

  static void Init() {
    mi_heap_t* data_heap = ThreadLocalResoure::resource_.tlh;
    void* ptr =
        mi_heap_malloc_aligned(data_heap, sizeof(DefragAllocator), alignof(DefragAllocator));
    self_ = new (ptr) DefragAllocator(data_heap);
    CompactObj::InitThreadLocal(self_->memory_resource());
    SmallString::InitThreadLocal(data_heap);  // so we can run this with and without small string
  }

  std::pmr::memory_resource* memory_resource() {
    return &mi_allocator;
  }

  static void Destroy() {
    if (self_) {
      mi_heap_t* tlh = self_->mi_allocator.heap();
      mi_free(self_);
      self_ = nullptr;
      CompactObj::InitThreadLocal(nullptr);
      mi_heap_delete(tlh);
    }
  }
};

thread_local DefragAllocator* DefragAllocator::self_ = nullptr;

class CompactObjectTestDefrag : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    InitRedisTables();  // to initialize server struct.
    DefragAllocator::Init();
    ASSERT_NE(DefragAllocator::self_, nullptr);
  }

  static void TearDownTestSuite() {
    mi_heap_collect(mi_heap_get_backing(), true);

    auto cb_visit = [](const mi_heap_t* heap, const mi_heap_area_t* area, void* block,
                       size_t block_size, void* arg) {
      LOG(ERROR) << "Unfreed allocations: block_size " << block_size
                 << ", allocated: " << area->used * block_size;
      return true;
    };

    mi_heap_visit_blocks(mi_heap_get_backing(), false /* do not visit all blocks*/, cb_visit,
                         nullptr);
    DefragAllocator::Destroy();
  }
  CompactObj cobj_;
};

TEST_F(CompactObjectTestDefrag, DefragTest) {
  std::string tmp(11511, 'b');

  cobj_.SetString(tmp);
  EXPECT_EQ(tmp.size(), cobj_.Size());

  cobj_.SetString(tmp);
  EXPECT_EQ(tmp.size(), cobj_.Size());

  bool was_defrag = cobj_.DefragIfNeeded(1.0f);  // with 1 we should not defrag always
  ASSERT_FALSE(was_defrag);
  auto p = cobj_.RObjPtr();
  std::list<CompactObj> objects;

  // unfortunately this does not work
  // it would never set was_defrag to true
  // this is doe to the fact that the test at
  // zmalloc_mi.c at line 175 always returning page->prev == NULL
  bool match_prev_ptr = false;
  while (objects.size() < 1000000 && !was_defrag) {
    CompactObj new_entry;
    new_entry.SetString(tmp);
    void* p2 = new_entry.RObjPtr();
    match_prev_ptr =
        (p == p2);  // we should assert here, but then we will never see the other issues
    p = p2;
    objects.push_back(std::move(new_entry));
    was_defrag = cobj_.DefragIfNeeded(0.0f);  // with 0 we should defrag
  }

  ASSERT_TRUE(was_defrag);  // this will trigger an error in the test
  ASSERT_FALSE(match_prev_ptr);
}

}  // namespace dfly
