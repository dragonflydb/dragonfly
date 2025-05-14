// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "server/common.h"

namespace dfly {

class CommandRegistry;
struct CommandContext;

class TopKeysFamily {
 public:
  static void Register(CommandRegistry* registry);

 private:
  static void Reserve(CmdArgList args, const CommandContext& cmd_cntx);
  static void Add(CmdArgList args, const CommandContext& cmd_cntx);
  static void List(CmdArgList args, const CommandContext& cmd_cntx);
};

}  // namespace dfly
