// Copyright 2022, DragonflyDB authors.  All rights reserved.
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

class JsonFamily {
 public:
  static void Register(CommandRegistry* registry);

 private:
  using SinkReplyBuilder = facade::SinkReplyBuilder;

  static void Get(CmdArgList args, const CommandContext& cmd_cntx);
  static void MGet(CmdArgList args, const CommandContext& cmd_cntx);
  static void Type(CmdArgList args, const CommandContext& cmd_cntx);
  static void StrLen(CmdArgList args, const CommandContext& cmd_cntx);
  static void ObjLen(CmdArgList args, const CommandContext& cmd_cntx);
  static void ArrLen(CmdArgList args, const CommandContext& cmd_cntx);
  static void Toggle(CmdArgList args, const CommandContext& cmd_cntx);
  static void NumIncrBy(CmdArgList args, const CommandContext& cmd_cntx);
  static void NumMultBy(CmdArgList args, const CommandContext& cmd_cntx);
  static void Del(CmdArgList args, const CommandContext& cmd_cntx);
  static void ObjKeys(CmdArgList args, const CommandContext& cmd_cntx);
  static void StrAppend(CmdArgList args, const CommandContext& cmd_cntx);
  static void Clear(CmdArgList args, const CommandContext& cmd_cntx);
  static void ArrPop(CmdArgList args, const CommandContext& cmd_cntx);
  static void ArrTrim(CmdArgList args, const CommandContext& cmd_cntx);
  static void ArrInsert(CmdArgList args, const CommandContext& cmd_cntx);
  static void ArrAppend(CmdArgList args, const CommandContext& cmd_cntx);
  static void ArrIndex(CmdArgList args, const CommandContext& cmd_cntx);
  static void Debug(CmdArgList args, const CommandContext& cmd_cntx);
  static void Resp(CmdArgList args, const CommandContext& cmd_cntx);
  static void Set(CmdArgList args, const CommandContext& cmd_cntx);
  static void MSet(CmdArgList args, const CommandContext& cmd_cntx);
  static void Merge(CmdArgList args, const CommandContext& cmd_cntx);
};

}  // namespace dfly
