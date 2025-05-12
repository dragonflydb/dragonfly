// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "server/common.h"

namespace dfly {

class CommandRegistry;
struct CommandContext;

class TDigestFamily {
 public:
  static void Register(CommandRegistry* registry);

 private:
  static void Create(CmdArgList args, const CommandContext& cmd_cntx);
  static void Del(CmdArgList args, const CommandContext& cmd_cntx);
  static void CreateRule(CmdArgList args, const CommandContext& cmd_cntx);
  static void DeleteRule(CmdArgList args, const CommandContext& cmd_cntx);
  static void Alter(CmdArgList args, const CommandContext& cmd_cntx);
  static void MCreate(CmdArgList args, const CommandContext& cmd_cntx);
  static void Add(CmdArgList args, const CommandContext& cmd_cntx);
  static void Get(CmdArgList args, const CommandContext& cmd_cntx);
  static void Range(CmdArgList args, const CommandContext& cmd_cntx);
  static void MAdd(CmdArgList args, const CommandContext& cmd_cntx);
  static void MGet(CmdArgList args, const CommandContext& cmd_cntx);
  static void MRange(CmdArgList args, const CommandContext& cmd_cntx);
  static void MQueryIndex(CmdArgList args, const CommandContext& cmd_cntx);
  static void Info(CmdArgList args, const CommandContext& cmd_cntx);
};

}  // namespace dfly
