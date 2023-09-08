// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "server/acl/acl_commands_def.h"
#include "server/common.h"
#include "server/engine_shard_set.h"

namespace dfly {

class ConnectionContext;
class CommandRegistry;
using facade::OpResult;
using facade::OpStatus;
using facade::RedisReplyBuilder;

class JsonFamily {
 public:
  static void Register(CommandRegistry* registry, acl::CommandTableBuilder builder);

 private:
  static void Get(CmdArgList args, ConnectionContext* cntx);
  static void MGet(CmdArgList args, ConnectionContext* cntx);
  static void Type(CmdArgList args, ConnectionContext* cntx);
  static void StrLen(CmdArgList args, ConnectionContext* cntx);
  static void ObjLen(CmdArgList args, ConnectionContext* cntx);
  static void ArrLen(CmdArgList args, ConnectionContext* cntx);
  static void Toggle(CmdArgList args, ConnectionContext* cntx);
  static void NumIncrBy(CmdArgList args, ConnectionContext* cntx);
  static void NumMultBy(CmdArgList args, ConnectionContext* cntx);
  static void Del(CmdArgList args, ConnectionContext* cntx);
  static void ObjKeys(CmdArgList args, ConnectionContext* cntx);
  static void StrAppend(CmdArgList args, ConnectionContext* cntx);
  static void Clear(CmdArgList args, ConnectionContext* cntx);
  static void ArrPop(CmdArgList args, ConnectionContext* cntx);
  static void ArrTrim(CmdArgList args, ConnectionContext* cntx);
  static void ArrInsert(CmdArgList args, ConnectionContext* cntx);
  static void ArrAppend(CmdArgList args, ConnectionContext* cntx);
  static void ArrIndex(CmdArgList args, ConnectionContext* cntx);
  static void Debug(CmdArgList args, ConnectionContext* cntx);
  static void Resp(CmdArgList args, ConnectionContext* cntx);
  static void Set(CmdArgList args, ConnectionContext* cntx);
};

}  // namespace dfly
