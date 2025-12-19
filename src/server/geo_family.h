// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "server/common.h"

namespace dfly {
class CommandRegistry;

class GeoFamily {
 public:
  static void Register(CommandRegistry* registry);
};

}  // namespace dfly
