// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "facade/op_status.h"
#include "server/common.h"

namespace facade {
class SinkReplyBuilder;
}  // namespace facade

namespace dfly {

using facade::OpResult;

class CommandRegistry;
struct CommandContext;

class ListFamily {
 public:
  static void Register(CommandRegistry* registry);

 private:
  using SinkReplyBuilder = facade::SinkReplyBuilder;

  static void LPush(CmdArgList args, CommandContext* cmd_cntx);
  static void LPushX(CmdArgList args, CommandContext* cmd_cntx);
  static void RPush(CmdArgList args, CommandContext* cmd_cntx);
  static void RPushX(CmdArgList args, CommandContext* cmd_cntx);
  static void LPop(CmdArgList args, CommandContext* cmd_cntx);
  static void RPop(CmdArgList args, CommandContext* cmd_cntx);
  static void BLPop(CmdArgList args, CommandContext* cmd_cntx);
  static void BRPop(CmdArgList args, CommandContext* cmd_cntx);
  static void LMPop(CmdArgList args, CommandContext* cmd_cntx);
  static void BLMPop(CmdArgList args, CommandContext* cmd_cntx);
  static void LLen(CmdArgList args, CommandContext* cmd_cntx);
  static void LPos(CmdArgList args, CommandContext* cmd_cntx);
  static void LIndex(CmdArgList args, CommandContext* cmd_cntx);
  static void LInsert(CmdArgList args, CommandContext* cmd_cntx);
  static void LTrim(CmdArgList args, CommandContext* cmd_cntx);
  static void LRange(CmdArgList args, CommandContext* cmd_cntx);
  static void LRem(CmdArgList args, CommandContext* cmd_cntx);
  static void LSet(CmdArgList args, CommandContext* cmd_cntx);
  static void LMove(CmdArgList args, CommandContext* cmd_cntx);
};

}  // namespace dfly
