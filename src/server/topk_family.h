// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "server/command_registry.h"
#include "server/common.h"

namespace dfly {

class CommandContext;

struct TopkFamily {
  static void Reserve(CmdArgList args, CommandContext* cmd_cntx);
  static void Add(CmdArgList args, CommandContext* cmd_cntx);
  static void IncrBy(CmdArgList args, CommandContext* cmd_cntx);
  static void Query(CmdArgList args, CommandContext* cmd_cntx);
  static void Count(CmdArgList args, CommandContext* cmd_cntx);
  static void List(CmdArgList args, CommandContext* cmd_cntx);
  static void Info(CmdArgList args, CommandContext* cmd_cntx);
};

}  // namespace dfly
