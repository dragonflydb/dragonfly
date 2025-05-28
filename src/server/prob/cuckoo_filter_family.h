// Copyright 2025, DragonflyDB authors.  All rights reserved.
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

class CuckooFilterFamily {
 public:
  static void Register(CommandRegistry* registry);

 private:
  using SinkReplyBuilder = facade::SinkReplyBuilder;

  static void Reserve(CmdArgList args, const CommandContext& cmd_cntx);
  static void Add(CmdArgList args, const CommandContext& cmd_cntx);
  static void AddNx(CmdArgList args, const CommandContext& cmd_cntx);
  static void Insert(CmdArgList args, const CommandContext& cmd_cntx);
  static void InsertNx(CmdArgList args, const CommandContext& cmd_cntx);
  static void Exists(CmdArgList args, const CommandContext& cmd_cntx);
  static void MExists(CmdArgList args, const CommandContext& cmd_cntx);
  static void Del(CmdArgList args, const CommandContext& cmd_cntx);
  static void Count(CmdArgList args, const CommandContext& cmd_cntx);
};

}  // namespace dfly
