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

  static void Append(CmdArgList args, const CommandContext& cmnd_cntx);
  static void Decr(CmdArgList args, const CommandContext& cmnd_cntx);
  static void DecrBy(CmdArgList args, const CommandContext& cmnd_cntx);
  static void Get(CmdArgList args, const CommandContext& cmnd_cntx);
  static void GetDel(CmdArgList args, const CommandContext& cmnd_cntx);
  static void GetRange(CmdArgList args, const CommandContext& cmnd_cntx);
  static void GetSet(CmdArgList args, const CommandContext& cmnd_cntx);
  static void GetEx(CmdArgList args, const CommandContext& cmnd_cntx);
  static void Incr(CmdArgList args, const CommandContext& cmnd_cntx);
  static void IncrBy(CmdArgList args, const CommandContext& cmnd_cntx);
  static void IncrByFloat(CmdArgList args, const CommandContext& cmnd_cntx);
  static void MGet(CmdArgList args, const CommandContext& cmnd_cntx);
  static void MSet(CmdArgList args, const CommandContext& cmnd_cntx);
  static void MSetNx(CmdArgList args, const CommandContext& cmnd_cntx);

  static void Set(CmdArgList args, const CommandContext& cmnd_cntx);
  static void SetEx(CmdArgList args, const CommandContext& cmnd_cntx);
  static void SetNx(CmdArgList args, const CommandContext& cmnd_cntx);
  static void SetRange(CmdArgList args, const CommandContext& cmnd_cntx);
  static void StrLen(CmdArgList args, const CommandContext& cmnd_cntx);
  static void Prepend(CmdArgList args, const CommandContext& cmnd_cntx);
  static void PSetEx(CmdArgList args, const CommandContext& cmnd_cntx);

  static void ClThrottle(CmdArgList args, const CommandContext& cmnd_cntx);
};

}  // namespace dfly
