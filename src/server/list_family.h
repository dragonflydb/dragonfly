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

  static void LPush(CmdArgList args, const CommandContext& cmd_cntx);
  static void LPushX(CmdArgList args, const CommandContext& cmd_cntx);
  static void RPush(CmdArgList args, const CommandContext& cmd_cntx);
  static void RPushX(CmdArgList args, const CommandContext& cmd_cntx);
  static void LPop(CmdArgList args, const CommandContext& cmd_cntx);
  static void RPop(CmdArgList args, const CommandContext& cmd_cntx);
  static void BLPop(CmdArgList args, const CommandContext& cmd_cntx);
  static void BRPop(CmdArgList args, const CommandContext& cmd_cntx);
  static void LLen(CmdArgList args, const CommandContext& cmd_cntx);
  static void LPos(CmdArgList args, const CommandContext& cmd_cntx);
  static void LIndex(CmdArgList args, const CommandContext& cmd_cntx);
  static void LInsert(CmdArgList args, const CommandContext& cmd_cntx);
  static void LTrim(CmdArgList args, const CommandContext& cmd_cntx);
  static void LRange(CmdArgList args, const CommandContext& cmd_cntx);
  static void LRem(CmdArgList args, const CommandContext& cmd_cntx);
  static void LSet(CmdArgList args, const CommandContext& cmd_cntx);
  static void LMove(CmdArgList args, const CommandContext& cmd_cntx);
};

}  // namespace dfly
