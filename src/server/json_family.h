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

  static void Get(CmdArgList args, CommandContext* cmd_cntx);
  static void MGet(CmdArgList args, CommandContext* cmd_cntx);
  static void Type(CmdArgList args, CommandContext* cmd_cntx);
  static void StrLen(CmdArgList args, CommandContext* cmd_cntx);
  static void ObjLen(CmdArgList args, CommandContext* cmd_cntx);
  static void ArrLen(CmdArgList args, CommandContext* cmd_cntx);
  static void Toggle(CmdArgList args, CommandContext* cmd_cntx);
  static void NumIncrBy(CmdArgList args, CommandContext* cmd_cntx);
  static void NumMultBy(CmdArgList args, CommandContext* cmd_cntx);
  static void Del(CmdArgList args, CommandContext* cmd_cntx);
  static void ObjKeys(CmdArgList args, CommandContext* cmd_cntx);
  static void StrAppend(CmdArgList args, CommandContext* cmd_cntx);
  static void Clear(CmdArgList args, CommandContext* cmd_cntx);
  static void ArrPop(CmdArgList args, CommandContext* cmd_cntx);
  static void ArrTrim(CmdArgList args, CommandContext* cmd_cntx);
  static void ArrInsert(CmdArgList args, CommandContext* cmd_cntx);
  static void ArrAppend(CmdArgList args, CommandContext* cmd_cntx);
  static void ArrIndex(CmdArgList args, CommandContext* cmd_cntx);
  static void Debug(CmdArgList args, CommandContext* cmd_cntx);
  static void Resp(CmdArgList args, CommandContext* cmd_cntx);
  static void Set(CmdArgList args, CommandContext* cmd_cntx);
  static void MSet(CmdArgList args, CommandContext* cmd_cntx);
  static void Merge(CmdArgList args, CommandContext* cmd_cntx);
};

}  // namespace dfly
