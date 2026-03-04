// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstddef>
#include <source_location>

namespace dfly::search {

// Running time quota that can be reset by suspending the fiber
struct RenewableQuota {
  // Create unlimited quota
  static RenewableQuota Unlimited();

  // Check if quota is remaining and suspend the fiber if it ran out
  void Check(std::source_location location = std::source_location::current()) const;

  const size_t max_usec;
};

}  // namespace dfly::search
