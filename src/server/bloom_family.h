// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "server/common.h"

namespace dfly {

class CommandRegistry;
struct CommandContext;

class BloomFamily {
 public:
  static void Register(CommandRegistry* registry);

 private:
};

}  // namespace dfly
