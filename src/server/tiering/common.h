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

constexpr size_t kPageSize = 4_KB;

// Location on the offloaded blob, measured in bytes
struct DiskSegment {
  DiskSegment ContainingPages() const {
    return {offset / kPageSize * kPageSize, (length + kPageSize - 1) / kPageSize * kPageSize};
  }

  DiskSegment() = default;
  DiskSegment(size_t offset, size_t length) : offset{offset}, length{length} {
  }
  DiskSegment(std::pair<size_t, size_t> p) : offset{p.first}, length(p.second) {
  }

  bool operator==(const DiskSegment& other) const {
    return offset == other.offset && length == other.length;
  }

  size_t offset = 0, length = 0;
};

};  // namespace dfly::tiering
