// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "facade/facade_types.h"

namespace dfly {

class CommandRegistry;

class StringFamily {
 public:
  static void Register(CommandRegistry* registry);
};

}  // namespace dfly
