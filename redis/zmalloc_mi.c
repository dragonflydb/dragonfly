// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <assert.h>
#include <mimalloc.h>
#include <string.h>

#include "atomicvar.h"
#include "zmalloc.h"

__thread ssize_t used_memory_tl = 0;
__thread mi_heap_t* zmalloc_heap = NULL;

/* Allocate memory or panic */
void* zmalloc(size_t size) {
  size_t usable;
  return zmalloc_usable(size, &usable);
}

void* ztrymalloc_usable(size_t size, size_t* usable) {
  return zmalloc_usable(size, usable);
}

size_t zmalloc_usable_size(const void* p) {
  return mi_usable_size(p);
}

void zfree(void* ptr) {
  size_t usable = mi_usable_size(ptr);
  used_memory_tl -= usable;

  return mi_free_size(ptr, usable);
}

void* zrealloc(void* ptr, size_t size) {
  size_t usable;
  return zrealloc_usable(ptr, size, &usable);
}

void* zcalloc(size_t size) {
  size_t usable = mi_good_size(size);

  used_memory_tl += usable;

  return mi_heap_calloc(zmalloc_heap, 1, usable);
}

void* zmalloc_usable(size_t size, size_t* usable) {
  size_t g = mi_good_size(size);
  *usable = g;

  used_memory_tl += g;
  assert(zmalloc_heap);
  return mi_heap_malloc(zmalloc_heap, g);
}

void* zrealloc_usable(void* ptr, size_t size, size_t* usable) {
  size_t g = mi_good_size(size);
  size_t prev = mi_usable_size(ptr);
  *usable = g;

  used_memory_tl += (g - prev);
  return mi_heap_realloc(zmalloc_heap, ptr, g);
}

size_t znallocx(size_t size) {
  return mi_good_size(size);
}

void zfree_size(void* ptr, size_t size) {
  mi_free_size(ptr, size);
}

void* ztrymalloc(size_t size) {
  size_t usable;
  return zmalloc_usable(size, &usable);
}

void* ztrycalloc(size_t size) {
  size_t g = mi_good_size(size);
  used_memory_tl += g;
  return mi_heap_calloc(zmalloc_heap, 1, size);
}

void init_zmalloc_threadlocal() {
  if (zmalloc_heap)
    return;
  zmalloc_heap = mi_heap_get_backing();
}
