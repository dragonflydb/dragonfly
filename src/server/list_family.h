// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "facade/op_status.h"
#include "server/common.h"

namespace dfly {

using facade::OpResult;

class CommandRegistry;
struct CommandContext;

class ListFamily {
 public:
  static void Register(CommandRegistry* registry);
};

}  // namespace dfly
