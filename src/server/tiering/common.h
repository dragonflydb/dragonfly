// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <memory>
#include <optional>
#include <variant>

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
  DiskSegment() = default;
  DiskSegment(size_t offset, size_t length) : offset{offset}, length{length} {
  }
  DiskSegment(std::pair<size_t, size_t> p) : offset{p.first}, length(p.second) {
  }

  bool operator==(const DiskSegment& other) const {
    return offset == other.offset && length == other.length;
  }

  DiskSegment ContainingPages() const {
    return {offset / kPageSize * kPageSize, (length + kPageSize - 1) / kPageSize * kPageSize};
  }

  size_t offset = 0, length = 0;

  friend std::ostream& operator<<(std::ostream& os, const DiskSegment& ds) {
    return os << "[" << ds.offset << ", " << ds.length << "]";
  }
};

using KeyRef = std::pair<uint16_t /* DbIndex */, std::string_view>;

// Two separate keyspaces are provided - one for strings, one for numeric identifiers.
// Ids can be used to track auxiliary values that don't map to real keys (like a page index).
// Specifically, we track page indexes when serializing small-bin pages with multiple items.
using PendingId = std::variant<uintptr_t, KeyRef>;

};  // namespace dfly::tiering
