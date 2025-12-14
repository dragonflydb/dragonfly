// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <string>

#include "base/mutex.h"
#include "server/common.h"

namespace facade {
class SinkReplyBuilder;
}  // namespace facade

namespace dfly {
class CommandRegistry;
class CommandContext;

class SearchFamily {
  using SinkReplyBuilder = facade::SinkReplyBuilder;

  static void FtCreate(CmdArgList args, CommandContext* cmd_cntx);
  static void FtAlter(CmdArgList args, CommandContext* cmd_cntx);
  static void FtDropIndex(CmdArgList args, CommandContext* cmd_cntx);
  static void FtInfo(CmdArgList args, CommandContext* cmd_cntx);
  static void FtList(CmdArgList args, CommandContext* cmd_cntx);
  static void FtSearch(CmdArgList args, CommandContext* cmd_cntx);
  static void FtProfile(CmdArgList args, CommandContext* cmd_cntx);
  static void FtAggregate(CmdArgList args, CommandContext* cmd_cntx);
  static void FtTagVals(CmdArgList args, CommandContext* cmd_cntx);
  static void FtSynDump(CmdArgList args, CommandContext* cmd_cntx);
  static void FtSynUpdate(CmdArgList args, CommandContext* cmd_cntx);
  static void FtConfig(CmdArgList args, CommandContext* cmd_cntx);
  static void FtDebug(CmdArgList args, CommandContext* cmd_cntx);

 public:
  static void Register(CommandRegistry* registry);
  static void Shutdown();
};

}  // namespace dfly
