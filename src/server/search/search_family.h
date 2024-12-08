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
struct CommandContext;

class SearchFamily {
  using SinkReplyBuilder = facade::SinkReplyBuilder;

  static void FtCreate(CmdArgList args, const CommandContext& cmd_cntx);
  static void FtAlter(CmdArgList args, const CommandContext& cmd_cntx);
  static void FtDropIndex(CmdArgList args, const CommandContext& cmd_cntx);
  static void FtInfo(CmdArgList args, const CommandContext& cmd_cntx);
  static void FtList(CmdArgList args, const CommandContext& cmd_cntx);
  static void FtSearch(CmdArgList args, const CommandContext& cmd_cntx);
  static void FtProfile(CmdArgList args, const CommandContext& cmd_cntx);
  static void FtAggregate(CmdArgList args, const CommandContext& cmd_cntx);
  static void FtTagVals(CmdArgList args, const CommandContext& cmd_cntx);

 public:
  static void Register(CommandRegistry* registry);
};

}  // namespace dfly
