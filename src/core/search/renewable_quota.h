// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstddef>

namespace dfly::search {

struct RenewableQuota {
  void Check() const;

  static RenewableQuota Unlimited();

  const size_t max_usec;
};

}  // namespace dfly::search
