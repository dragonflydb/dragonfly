// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <memory>
#include <optional>

#include "util/fibers/synchronization.h"

namespace dfly::tiering {

inline namespace literals {

constexpr inline unsigned long long operator""_MB(unsigned long long x) {
  return x << 20U;
}

constexpr inline unsigned long long operator""_KB(unsigned long long x) {
  return x << 10U;
}

}  // namespace literals

// Location on the offloaded blob, measured in bytes
struct DiskSegment {
  DiskSegment FillPages() const {
    return {offset / 4096 * 4096, (length + 4096 - 1) / 4096 * 4096};
  }

  DiskSegment() = default;
  DiskSegment(size_t offset, size_t length) : offset{offset}, length{length} {
  }
  DiskSegment(std::pair<size_t, size_t> p) : offset{p.first}, length(p.second) {
  }

  size_t offset = 0, length = 0;
};

};  // namespace dfly::tiering
