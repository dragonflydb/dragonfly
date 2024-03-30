// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <memory>
#include <optional>

#include "util/fibers/synchronization.h"

namespace dfly::tiering {

// Location on the offloaded blob, measured in bytes
struct DiskSegment {
  DiskSegment() = default;
  DiskSegment(size_t offset, size_t length) : offset{offset}, length{length} {
  }
  DiskSegment(std::pair<size_t, size_t> p) : offset{p.first}, length(p.second) {
  }

  size_t offset = 0, length = 0;
};

};  // namespace dfly::tiering
