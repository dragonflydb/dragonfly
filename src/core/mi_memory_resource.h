// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <mimalloc.h>

#include <cstddef>

#ifdef __clang__
#include <experimental/memory_resource>

namespace PMR_NS = std::experimental::pmr;
#else
#include <memory_resource>

namespace PMR_NS = std::pmr;
#endif

namespace dfly {

class MiMemoryResource : public PMR_NS::memory_resource {
 public:
  explicit MiMemoryResource(mi_heap_t* heap) : heap_(heap) {
  }

  mi_heap_t* heap() {
    return heap_;
  }

  size_t used() const {
    return used_;
  }

 private:
  void* do_allocate(std::size_t size, std::size_t align) final;

  void do_deallocate(void* ptr, std::size_t size, std::size_t align) final;

  bool do_is_equal(const PMR_NS::memory_resource& o) const noexcept {
    return this == &o;
  }

  mi_heap_t* heap_;
  size_t used_ = 0;
};

}  // namespace dfly
