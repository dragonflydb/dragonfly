// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <algorithm>

namespace rng {

namespace detail {

#define WRAPPER(name, algo)                                                \
  template <typename C, typename... Args> auto name(C&& c, Args... args) { \
    return algo(c.begin(), c.end(), std::forward<Args>(args)...);          \
  }

WRAPPER(all_of, std::all_of);
WRAPPER(sort, std::sort);

}  // namespace detail

using detail::all_of;
using detail::sort;

}  // namespace rng
