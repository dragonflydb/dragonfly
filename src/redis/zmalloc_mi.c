// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <assert.h>
#include <mimalloc-types.h>
#include <mimalloc.h>
#include <string.h>

#include "atomicvar.h"
#include "zmalloc.h"

__thread ssize_t zmalloc_used_memory_tl = 0;
__thread mi_heap_t* zmalloc_heap = NULL;

/* Allocate memory or panic */
void* zmalloc(size_t size) {
  assert(zmalloc_heap);
  void* res = mi_heap_malloc(zmalloc_heap, size);
  size_t usable = mi_usable_size(res);

  // assertion does not hold. Basically mi_good_size is not a good function for
  // doing accounting.
  // assert(usable == mi_good_size(size));
  zmalloc_used_memory_tl += usable;

  return res;
}

void* ztrymalloc_usable(size_t size, size_t* usable) {
  return zmalloc_usable(size, usable);
}

size_t zmalloc_usable_size(const void* p) {
  return mi_usable_size(p);
}

void zfree(void* ptr) {
  size_t usable = mi_usable_size(ptr);

  // assert(zmalloc_used_memory_tl >= (ssize_t)usable);
  zmalloc_used_memory_tl -= usable;

  mi_free_size(ptr, usable);
}

void* zrealloc(void* ptr, size_t size) {
  size_t usable;
  return zrealloc_usable(ptr, size, &usable);
}

void* zcalloc(size_t size) {
  // mi_good_size(size) is not working. try for example, size=690557.

  void* res = mi_heap_calloc(zmalloc_heap, 1, size);
  size_t usable = mi_usable_size(res);
  zmalloc_used_memory_tl += usable;

  return res;
}

void* zmalloc_usable(size_t size, size_t* usable) {
  assert(zmalloc_heap);
  void* res = mi_heap_malloc(zmalloc_heap, size);
  size_t uss = mi_usable_size(res);
  *usable = uss;

  zmalloc_used_memory_tl += uss;

  return res;
}

void* zrealloc_usable(void* ptr, size_t size, size_t* usable) {
  ssize_t prev = mi_usable_size(ptr);

  void* res = mi_heap_realloc(zmalloc_heap, ptr, size);
  ssize_t uss = mi_usable_size(res);
  *usable = uss;
  zmalloc_used_memory_tl += (uss - prev);

  return res;
}

size_t znallocx(size_t size) {
  return mi_good_size(size);
}

void zfree_size(void* ptr, size_t size) {
  ssize_t uss = mi_usable_size(ptr);
  zmalloc_used_memory_tl -= uss;
  mi_free_size(ptr, uss);
}

void* ztrymalloc(size_t size) {
  size_t usable;
  return zmalloc_usable(size, &usable);
}

void* ztrycalloc(size_t size) {
  size_t g = mi_good_size(size);
  zmalloc_used_memory_tl += g;
  void* ptr = mi_heap_calloc(zmalloc_heap, 1, size);
  assert(mi_usable_size(ptr) == g);
  return ptr;
}

typedef struct Sum_s {
  size_t allocated;
  size_t comitted;
} Sum_t;

bool heap_visit_cb(const mi_heap_t* heap, const mi_heap_area_t* area, void* block,
                   size_t block_size, void* arg) {
  assert(area->used < (1u << 31));

  Sum_t* sum = (Sum_t*)arg;

  // mimalloc mistakenly exports used in blocks instead of bytes.
  sum->allocated += block_size * area->used;
  sum->comitted += area->committed;

  return true;  // continue iteration
};

int zmalloc_get_allocator_info(size_t* allocated, size_t* active, size_t* resident) {
  Sum_t sum = {0};

  mi_heap_visit_blocks(zmalloc_heap, false /* visit all blocks*/, heap_visit_cb, &sum);
  *allocated = sum.allocated;
  *resident = sum.comitted;
  *active = 0;

  return 1;
}

void init_zmalloc_threadlocal(void* heap) {
  if (zmalloc_heap)
    return;
  zmalloc_heap = heap;
}

// taken from mimalloc private code.
static inline mi_slice_t* mi_slice_first(const mi_slice_t* slice) {
  mi_slice_t* start = (mi_slice_t*)((uint8_t*)slice - slice->slice_offset);
  return start;
}

// taken from mimalloc private code.
static inline mi_segment_t* _mi_ptr_segment(const void* p) {
  return (mi_segment_t*)((uintptr_t)p & ~MI_SEGMENT_MASK);
}

#define mi_likely(x) __builtin_expect(!!(x), true)

// returns true if page is not active, and its used blocks <= capacity * ratio
int zmalloc_page_is_underutilized(void* ptr, float ratio) {
  mi_segment_t* const segment = _mi_ptr_segment(ptr);

  // from _mi_segment_page_of
  ptrdiff_t diff = (uint8_t*)ptr - (uint8_t*)segment;
  size_t idx = (size_t)diff >> MI_SEGMENT_SLICE_SHIFT;
  mi_slice_t* slice0 = (mi_slice_t*)&segment->slices[idx];
  mi_slice_t* slice = mi_slice_first(slice0);  // adjust to the block that holds the page data
  mi_page_t* page = (mi_page_t*)slice;
  // end from _mi_segment_page_of //

  // from mi_page_heap
  mi_heap_t* page_heap = (mi_heap_t*)(mi_atomic_load_relaxed(&(page)->xheap));

  // the heap id matches and it is not a full page
  if (mi_likely(page_heap == zmalloc_heap && page->flags.x.in_full == 0)) {
    // mi_page_queue_t* pq = mi_heap_page_queue_of(heap, page);

    // first in the list, meaning it's the head of page queue, thus being used for malloc
    if (page->prev == NULL)
      return false;

    // this page belong to this heap and is not first in the page queue. Lets check its
    // utilization.
    return page->used <= (unsigned)(page->capacity * ratio);
  }

  return false;
}
