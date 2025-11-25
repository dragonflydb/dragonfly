// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.

#pragma once

#include <cassert>

#include "base/pmr/memory_resource.h"

namespace dfly {

namespace detail {
inline thread_local PMR_NS::memory_resource* tl_mr = nullptr;
}

template <typename T> class StatelessAllocator {
 public:
  using value_type = T;
  using size_type = std::size_t;
  using difference_type = std::ptrdiff_t;
  using is_always_equal = std::true_type;

  template <typename U> StatelessAllocator(const StatelessAllocator<U>&) noexcept {
  }

  StatelessAllocator() noexcept {
    assert(detail::tl_mr != nullptr);
  };

  static value_type* allocate(size_type n) {
    void* ptr = detail::tl_mr->allocate(n * sizeof(value_type), alignof(value_type));
    return static_cast<value_type*>(ptr);
  }

  static void deallocate(value_type* ptr, size_type n) noexcept {
    detail::tl_mr->deallocate(ptr, n * sizeof(value_type), alignof(value_type));
  }

  static PMR_NS::memory_resource* resource() {
    return detail::tl_mr;
  }
};

template <typename T, typename U>
bool operator==(const StatelessAllocator<T>&, const StatelessAllocator<U>&) noexcept {
  return true;
}

template <typename T, typename U>
bool operator!=(const StatelessAllocator<T>&, const StatelessAllocator<U>&) noexcept {
  return false;
}

inline void InitTLStatelessAllocMR(PMR_NS::memory_resource* mr) {
  detail::tl_mr = mr;
}

}  // namespace dfly
