// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.

#pragma once

#include <cassert>

#include "base/pmr/memory_resource.h"

namespace dfly {

namespace detail {
inline thread_local PMR_NS::memory_resource* search_tl_mr = nullptr;
}

// Modeled after StatelessAllocator from core/detail/stateless_allocator.h,
// However, this allocator wraps a different memory resource, this is done so that the used bytes
// accounting for the memory resource remains separate from the other allocator.
template <typename T> class StatelessSearchAllocator {
 public:
  using value_type = T;
  using size_type = std::size_t;
  using difference_type = std::ptrdiff_t;
  using is_always_equal = std::true_type;

  template <typename U>
  StatelessSearchAllocator(const StatelessSearchAllocator<U>&) noexcept {  // NOLINT
  }

  StatelessSearchAllocator() noexcept {
    assert(detail::search_tl_mr != nullptr);
  };

  template <typename U, typename... _Args> void construct(U* __p, _Args&&... __args) {
    ::new (static_cast<void*>(__p)) U(std::forward<_Args>(__args)...);
  }

  static value_type* allocate(size_type n) {
    static_assert(
        std::is_empty_v<StatelessSearchAllocator>,
        "StatelessSearchAllocator must not contain state, so it can use empty base optimization");

    void* ptr = detail::search_tl_mr->allocate(n * sizeof(value_type), alignof(value_type));
    return static_cast<value_type*>(ptr);
  }

  static void deallocate(value_type* ptr, size_type n) noexcept {
    detail::search_tl_mr->deallocate(ptr, n * sizeof(value_type), alignof(value_type));
  }

  static PMR_NS::memory_resource* resource() {
    return detail::search_tl_mr;
  }
};

template <typename T, typename U>
bool operator==(const StatelessSearchAllocator<T>&, const StatelessSearchAllocator<U>&) noexcept {
  return true;
}

template <typename T, typename U>
bool operator!=(const StatelessSearchAllocator<T>&, const StatelessSearchAllocator<U>&) noexcept {
  return false;
}

inline void InitTLSearchMR(PMR_NS::memory_resource* mr) {
  detail::search_tl_mr = mr;
}

}  // namespace dfly
