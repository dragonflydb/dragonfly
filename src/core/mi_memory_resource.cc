// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "core/mi_memory_resource.h"

#include <new>
#include <ostream>

#include "glog/logging.h"

namespace dfly {

void* MiMemoryResource::do_allocate(std::size_t size, std::size_t align) {
  DCHECK(align);

  void* res = mi_heap_malloc_aligned(heap_, size, align);

  if (!res)
    throw std::bad_alloc{};

  // It seems that mimalloc has a bug with larger allocations that causes
  // mi_heap_contains_block to lie. See https://github.com/microsoft/mimalloc/issues/587
  // For now I avoid the check by checking the size. mi_usable_size works though.
  DCHECK(size > 33554400 || mi_heap_contains_block(heap_, res));
  size_t delta = mi_usable_size(res);

  used_ += delta;
  DVLOG(1) << "do_allocate: " << heap_ << " " << delta;

  return res;
}

void MiMemoryResource::do_deallocate(void* ptr, std::size_t size, std::size_t align) {
  DCHECK(size > 33554400 || mi_heap_contains_block(heap_, ptr));

  size_t usable = mi_usable_size(ptr);

  DVLOG(1) << "do_deallocate: " << heap_ << " " << usable;

  DCHECK_GE(used_, size);
  used_ -= usable;
  mi_free_size_aligned(ptr, size, align);
}

}  // namespace dfly
