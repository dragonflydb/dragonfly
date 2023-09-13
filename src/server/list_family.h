// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "facade/op_status.h"
#include "server/acl/acl_commands_def.h"
#include "server/common.h"

namespace dfly {

using facade::OpResult;

class ConnectionContext;
class CommandRegistry;
class EngineShard;

class ListFamily {
 public:
  static void Register(CommandRegistry* registry);

 private:
  static void LPush(CmdArgList args, ConnectionContext* cntx);
  static void LPushX(CmdArgList args, ConnectionContext* cntx);
  static void RPush(CmdArgList args, ConnectionContext* cntx);
  static void RPushX(CmdArgList args, ConnectionContext* cntx);
  static void LPop(CmdArgList args, ConnectionContext* cntx);
  static void RPop(CmdArgList args, ConnectionContext* cntx);
  static void BLPop(CmdArgList args, ConnectionContext* cntx);
  static void BRPop(CmdArgList args, ConnectionContext* cntx);
  static void LLen(CmdArgList args, ConnectionContext* cntx);
  static void LPos(CmdArgList args, ConnectionContext* cntx);
  static void LIndex(CmdArgList args, ConnectionContext* cntx);
  static void LInsert(CmdArgList args, ConnectionContext* cntx);
  static void LTrim(CmdArgList args, ConnectionContext* cntx);
  static void LRange(CmdArgList args, ConnectionContext* cntx);
  static void LRem(CmdArgList args, ConnectionContext* cntx);
  static void LSet(CmdArgList args, ConnectionContext* cntx);
  static void LMove(CmdArgList args, ConnectionContext* cntx);

  static void PopGeneric(ListDir dir, CmdArgList args, ConnectionContext* cntx);
  static void PushGeneric(ListDir dir, bool skip_notexist, CmdArgList args,
                          ConnectionContext* cntx);

  static void BPopGeneric(ListDir dir, CmdArgList args, ConnectionContext* cntx);
};

}  // namespace dfly
