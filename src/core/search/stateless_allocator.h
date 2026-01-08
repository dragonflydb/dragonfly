// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.

#pragma once

#include <cassert>

#include "base/pmr/memory_resource.h"
#include "core/detail/stateless_allocator.h"

namespace dfly {

namespace detail {
inline thread_local PMR_NS::memory_resource* search_tl_mr = nullptr;
}

template <typename T>
class StatelessSearchAllocator : public StatelessAllocatorBase<T, StatelessSearchAllocator<T>> {
 public:
  StatelessSearchAllocator() noexcept {
    assert(detail::search_tl_mr != nullptr);
  }

  template <typename U>
  StatelessSearchAllocator(const StatelessSearchAllocator<U>&) noexcept {  // NOLINT
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
