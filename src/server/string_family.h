// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "facade/facade_types.h"

namespace facade {
class SinkReplyBuilder;
}  // namespace facade

namespace dfly {

struct CommandContext;
class CommandRegistry;

class StringFamily {
 public:
  static void Register(CommandRegistry* registry);
};

}  // namespace dfly
