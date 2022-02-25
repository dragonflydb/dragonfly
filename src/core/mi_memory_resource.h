// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <mimalloc.h>

#include <memory_resource>

namespace dfly {

class MiMemoryResource final : public std::pmr::memory_resource {
 public:
  explicit MiMemoryResource(mi_heap_t* heap) : heap_(heap) {
  }

 private:
  void* do_allocate(std::size_t size, std::size_t align) {
    return mi_heap_malloc_aligned(heap_, size, align);
  }

  void do_deallocate(void* ptr, std::size_t size, std::size_t align) {
    mi_free_size_aligned(ptr, size, align);
  }

  bool do_is_equal(const std::pmr::memory_resource& o) const noexcept {
    return this == &o;
  }

  mi_heap_t* heap_;
};

}  // namespace dfly