// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "server/common.h"

namespace facade {
class SinkReplyBuilder;
}  // namespace facade

namespace dfly {

class CommandRegistry;
struct CommandContext;

class BloomFamily {
 public:
  static void Register(CommandRegistry* registry);

 private:
  using SinkReplyBuilder = facade::SinkReplyBuilder;

  static void Reserve(CmdArgList args, const CommandContext& cmd_cntx);
  static void Add(CmdArgList args, const CommandContext& cmd_cntx);
  static void MAdd(CmdArgList args, const CommandContext& cmd_cntx);
  static void Exists(CmdArgList args, const CommandContext& cmd_cntx);
  static void MExists(CmdArgList args, const CommandContext& cmd_cntx);
};

}  // namespace dfly
