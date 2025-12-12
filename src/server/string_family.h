// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "facade/facade_types.h"

namespace facade {
class SinkReplyBuilder;
}  // namespace facade

namespace dfly {

struct CommandContext;
class CommandRegistry;

class StringFamily {
 public:
  static void Register(CommandRegistry* registry);

 private:
  using SinkReplyBuilder = facade::SinkReplyBuilder;
  using CmdArgList = facade::CmdArgList;

  static void Append(CmdArgList args, CommandContext* cmnd_cntx);
  static void Decr(CmdArgList args, CommandContext* cmnd_cntx);
  static void DecrBy(CmdArgList args, CommandContext* cmnd_cntx);
  static void Get(CmdArgList args, CommandContext* cmnd_cntx);
  static void GetDel(CmdArgList args, CommandContext* cmnd_cntx);
  static void GetRange(CmdArgList args, CommandContext* cmnd_cntx);
  static void GetSet(CmdArgList args, CommandContext* cmnd_cntx);
  static void GetEx(CmdArgList args, CommandContext* cmnd_cntx);
  static void Incr(CmdArgList args, CommandContext* cmnd_cntx);
  static void IncrBy(CmdArgList args, CommandContext* cmnd_cntx);
  static void IncrByFloat(CmdArgList args, CommandContext* cmnd_cntx);
  static void MGet(CmdArgList args, CommandContext* cmnd_cntx);
  static void MSet(CmdArgList args, CommandContext* cmnd_cntx);
  static void MSetNx(CmdArgList args, CommandContext* cmnd_cntx);

  static void Set(CmdArgList args, CommandContext* cmnd_cntx);
  static void SetNx(CmdArgList args, CommandContext* cmnd_cntx);
  static void SetRange(CmdArgList args, CommandContext* cmnd_cntx);
  static void SetExGeneric(CmdArgList args, CommandContext* cmnd_cntx);
  static void StrLen(CmdArgList args, CommandContext* cmnd_cntx);
  static void Prepend(CmdArgList args, CommandContext* cmnd_cntx);

  static void ClThrottle(CmdArgList args, CommandContext* cmnd_cntx);
  static void GAT(CmdArgList args, CommandContext* cmnd_cntx);
};

}  // namespace dfly
