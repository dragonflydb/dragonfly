// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "facade/facade_types.h"

namespace dfly {

class ConnectionContext;
class CommandRegistry;

using facade::CmdArgList;

class StringFamily {
 public:
  static void Register(CommandRegistry* registry);

 private:
  static void Append(CmdArgList args, ConnectionContext* cntx);
  static void Decr(CmdArgList args, ConnectionContext* cntx);
  static void DecrBy(CmdArgList args, ConnectionContext* cntx);
  static void Get(CmdArgList args, ConnectionContext* cntx);
  static void GetDel(CmdArgList args, ConnectionContext* cntx);
  static void GetRange(CmdArgList args, ConnectionContext* cntx);
  static void GetSet(CmdArgList args, ConnectionContext* cntx);
  static void GetEx(CmdArgList args, ConnectionContext* cntx);
  static void Incr(CmdArgList args, ConnectionContext* cntx);
  static void IncrBy(CmdArgList args, ConnectionContext* cntx);
  static void IncrByFloat(CmdArgList args, ConnectionContext* cntx);
  static void MGet(CmdArgList args, ConnectionContext* cntx);
  static void MSet(CmdArgList args, ConnectionContext* cntx);
  static void MSetNx(CmdArgList args, ConnectionContext* cntx);

  static void Set(CmdArgList args, ConnectionContext* cntx);
  static void SetEx(CmdArgList args, ConnectionContext* cntx);
  static void SetNx(CmdArgList args, ConnectionContext* cntx);
  static void SetRange(CmdArgList args, ConnectionContext* cntx);
  static void StrLen(CmdArgList args, ConnectionContext* cntx);
  static void Prepend(CmdArgList args, ConnectionContext* cntx);
  static void PSetEx(CmdArgList args, ConnectionContext* cntx);

  static void ClThrottle(CmdArgList args, ConnectionContext* cntx);
};

}  // namespace dfly
