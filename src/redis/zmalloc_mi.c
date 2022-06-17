// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <assert.h>
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

  // I wish we can keep this assert but rdb_load creates objects in one thread and
  // uses them in another.
  // assert(zmalloc_used_memory_tl >= (ssize_t)usable);
  zmalloc_used_memory_tl -= usable;

  mi_free_size(ptr, usable);
}

void* zrealloc(void* ptr, size_t size) {
  size_t usable;
  return zrealloc_usable(ptr, size, &usable);
}

void* zcalloc(size_t size) {
  size_t usable = mi_good_size(size);

  zmalloc_used_memory_tl += usable;

  return mi_heap_calloc(zmalloc_heap, 1, size);
}

void* zmalloc_usable(size_t size, size_t* usable) {
  size_t g = mi_good_size(size);
  *usable = g;

  zmalloc_used_memory_tl += g;
  assert(zmalloc_heap);
  void* ptr = mi_heap_malloc(zmalloc_heap, g);
  assert(mi_usable_size(ptr) == g);

  return ptr;
}

void* zrealloc_usable(void* ptr, size_t size, size_t* usable) {
  size_t g = mi_good_size(size);
  size_t prev = mi_usable_size(ptr);
  *usable = g;

  zmalloc_used_memory_tl += (g - prev);
  void* res = mi_heap_realloc(zmalloc_heap, ptr, g);
  // does not hold, say when prev = 16 and size = 6. mi_malloc does not shrink in this case.
  // assert(mi_usable_size(res) == g);
  return res;
}

size_t znallocx(size_t size) {
  return mi_good_size(size);
}

void zfree_size(void* ptr, size_t size) {
  zmalloc_used_memory_tl -= size;
  mi_free_size(ptr, size);
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
