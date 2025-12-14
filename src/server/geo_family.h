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
  static void GeoAdd(CmdArgList args, CommandContext* cmd_cntx);
  static void GeoHash(CmdArgList args, CommandContext* cmd_cntx);
  static void GeoPos(CmdArgList args, CommandContext* cmd_cntx);
  static void GeoDist(CmdArgList args, CommandContext* cmd_cntx);
  static void GeoSearch(CmdArgList args, CommandContext* cmd_cntx);
  static void GeoRadiusByMemberGeneric(CmdArgList args, CommandContext* cmd_cntx, bool read_only);
  static void GeoRadiusByMember(CmdArgList args, CommandContext* cmd_cntx);
  static void GeoRadiusByMemberRO(CmdArgList args, CommandContext* cmd_cntx);
  static void GeoRadiusGeneric(CmdArgList args, CommandContext* cmd_cntx, bool read_only);
  static void GeoRadius(CmdArgList args, CommandContext* cmd_cntx);
  static void GeoRadiusRO(CmdArgList args, CommandContext* cmd_cntx);
};

}  // namespace dfly
