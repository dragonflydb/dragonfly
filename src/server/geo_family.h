// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "server/common.h"

namespace dfly {

class CommandRegistry;
struct CommandContext;

class GeoFamily {
 public:
  static void Register(CommandRegistry* registry);

 private:
  static void GeoAdd(CmdArgList args, const CommandContext& cmd_cntx);
  static void GeoHash(CmdArgList args, const CommandContext& cmd_cntx);
  static void GeoPos(CmdArgList args, const CommandContext& cmd_cntx);
  static void GeoDist(CmdArgList args, const CommandContext& cmd_cntx);
  static void GeoSearch(CmdArgList args, const CommandContext& cmd_cntx);
  static void GeoRadiusByMember(CmdArgList args, const CommandContext& cmd_cntx);
};

}  // namespace dfly
